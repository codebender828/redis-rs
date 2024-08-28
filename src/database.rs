/**
 * This file is responsible for interacting with the raw RDB file
 *
 * At a high level, the RDB file format has the following structure
 * ----------------------------#
 *  52 45 44 49 53              # Magic String "REDIS"
 *  30 30 30 33                 # RDB Version Number as ASCII string. "0003" = 3
 *  ----------------------------
 *  FA                          # Auxiliary field
 *  $string-encoded-key         # May contain arbitrary metadata
 *  $string-encoded-value       # such as Redis version, creation time, used memory, ...
 *  ----------------------------
 *  FE 00                       # Indicates database selector. db number = 00
 *  FB                          # Indicates a resizedb field
 *  $length-encoded-int         # Size of the corresponding hash table
 *  $length-encoded-int         # Size of the corresponding expire hash table
 *  ----------------------------# Key-Value pair starts
 *  FD $unsigned-int            # "expiry time in seconds", followed by 4 byte unsigned int
 *  $value-type                 # 1 byte flag indicating the type of value
 *  $string-encoded-key         # The key, encoded as a redis string
 *  $encoded-value              # The value, encoding depends on $value-type
 *  ----------------------------
 *  FC $unsigned long           # "expiry time in ms", followed by 8 byte unsigned long
 *  $value-type                 # 1 byte flag indicating the type of value
 *  $string-encoded-key         # The key, encoded as a redis string
 *  $encoded-value              # The value, encoding depends on $value-type
 *  ----------------------------
 *  $value-type                 # key-value pair without expiry
 *  $string-encoded-key
 *  $encoded-value
 *  ----------------------------
 *  FE $length-encoding         # Previous db ends, next db starts.
 *  ----------------------------
 *  ...                         # Additional key-value pairs, databases, ... *
 *  FF                          ## End of RDB file indicator
 *  8-byte-checksum             ## CRC64 checksum of the entire file.
 * ----------------------------#
 *
 * An example file looks like this:
 * 524544495330303130fa0972656469732d76657206372e302e3130fa0a72656469732d62697473c040fa056374696d65c2d5bbcc66fa08757365642d6d656dc2d0171100fa08616f662d62617365c000fe00fb0201fc86de7dad91010000000362617a037a61670003666f6f03626172ff20b3abf967cff893
 *
 */
use crate::{config::Config, parser, storage::Storage};
use log::{debug, error, warn};
use std::io::{Error, ErrorKind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{panic, str, sync::Arc, time::Instant};
use tokio::sync::Mutex;

pub async fn populate_hot_storage(storage: &Arc<Mutex<Storage>>, config: &Arc<Mutex<Config>>) {
  // Extract the directory and dbfilename from the configuration
  // and populate the storage with the data

  let storage = storage.lock().await;
  let config = config.lock().await;

  let directory = config.get("dir").unwrap();
  let dbfilename = config.get("dbfilename").unwrap();
  let rdb_file_path = format!("{}/{}", directory, dbfilename);

  // Read the file and populate the storage
  let rdb_contents = std::fs::read_to_string(rdb_file_path).unwrap();
  let rdb_contents = rdb_contents.to_string().to_uppercase();
  println!("Contents: {}", rdb_contents);

  let rdb_data = hex::decode(rdb_contents.trim()).expect("Failed to decode RDB file");
  let mut parser = RDBParser::new(rdb_data);

  if let Err(e) = parser.parse() {
    eprintln!("Error parsing RDB file: {}", e);
    dbg!(e);
    // Handle the error appropriately
  } else {
    // Use the parsed data as needed
    println!(
      "Parsed {} non-expiring entries and {} expiring entries",
      parser.entries.len(),
      parser.expiry_entries.len()
    );
  }
}

/// Parse the RDB version from the RDB file
pub fn parse_rdb_version(data: &[u8]) -> Result<u32, &'static str> {
  if data.len() < 9 {
    return Err("Input data too short to parse RDB version");
  }

  // The first 5 bytes are the magic string "REDIS"
  let magic = str::from_utf8(&data[0..5]).map_err(|_| "Invalid magic string")?;
  if magic != "REDIS" {
    return Err("Invalid RDB file. Magic String is missing");
  }

  let version = str::from_utf8(&data[5..9]).map_err(|_| "Invalid RDB version")?;
  version.parse::<u32>().map_err(|_| "Invalid RDB version")
}

/// Parser struct for the RDBParser
#[derive(Debug)]
pub struct RDBParser {
  /// Raw file data for the RDB file
  data: Vec<u8>,
  keys: Vec<Vec<u8>>,
  rdb_version: u32,
  redis_version: Vec<u8>,
  creation_time: Vec<u8>,
  aux_fields: Vec<(Vec<u8>, Vec<u8>)>,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  expiry_entries: Vec<(Vec<u8>, Vec<u8>, SystemTime)>,
}

impl RDBParser {
  /// Create a new RDBParser instance
  pub fn new(data: Vec<u8>) -> Self {
    RDBParser {
      data,
      keys: Vec::new(),
      entries: Vec::new(),
      rdb_version: 0,
      redis_version: Vec::new(),
      creation_time: Vec::new(),
      aux_fields: Vec::new(),
      expiry_entries: Vec::new(),
    }
  }

  /// Parse the RDB file
  pub fn parse(&mut self) -> Result<(), Error> {
    debug!(
      "Starting to parse RDB file. Total data length: {}",
      self.data.len()
    );

    self.rdb_version = self
      .parse_rdb_version(&self.data)
      .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
    debug!("RDB version: {}", self.rdb_version);

    let (aux_fields, index) = self.parse_auxiliary_fields(&self.data)?;
    self.aux_fields = aux_fields;
    debug!(
      "Parsed {} auxiliary fields. Index after aux fields: {}",
      self.aux_fields.len(),
      index
    );

    // Process auxiliary fields...

    // Parse the database entries
    let (entries, expiry_entries) = self.process_entries(&self.data[index..]).map_err(|e| {
      error!("Failed to process entries: {}", e);
      e
    })?;

    // Add the processed entries to self
    self.entries.extend(entries);
    self.expiry_entries.extend(expiry_entries);

    debug!(
      "Finished parsing RDB file. Regular entries: {}, Expiry entries: {}",
      self.entries.len(),
      self.expiry_entries.len()
    );
    Ok(())
  }

  /// Extract the RDB version from RDB.
  pub fn parse_rdb_version(&self, data: &[u8]) -> Result<u32, &'static str> {
    if data.len() < 9 {
      return Err("Input data too short to parse RDB version");
    }

    // The first 5 bytes are the magic string "REDIS"
    let magic = str::from_utf8(&data[0..5]).map_err(|_| "Invalid magic string")?;
    if magic != "REDIS" {
      return Err("Invalid RDB file. Magic String is missing");
    }

    let version = str::from_utf8(&data[5..9]).map_err(|_| "Invalid RDB version")?;
    version.parse::<u32>().map_err(|_| "Invalid RDB version")
  }

  /// Parse the auxiliary fields from the RDB file
  pub fn parse_aux_fields(&mut self) {}

  /// decode the length of a length encoded string
  fn decode_length(&self, data: &[u8]) -> Result<(usize, usize), Error> {
    if data.is_empty() {
      return Err(Error::new(
        ErrorKind::UnexpectedEof,
        "Empty data when decoding length",
      ));
    }

    let first_byte = data[0];
    debug!("Decoding length, first byte: {}", first_byte);

    match first_byte {
      0..=63 => Ok((1, first_byte as usize)),
      64..=127 => {
        if data.len() < 2 {
          return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Insufficient data for medium length",
          ));
        }
        let length = ((first_byte as usize & 0x3f) << 8) | data[1] as usize;
        Ok((2, length))
      }
      128..=191 => {
        if data.len() < 4 {
          return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Insufficient data for long length",
          ));
        }
        let length = ((first_byte as usize & 0x3f) << 24)
          | ((data[1] as usize) << 16)
          | ((data[2] as usize) << 8)
          | data[3] as usize;
        Ok((4, length))
      }
      192..=253 => Ok((1, (first_byte as usize - 192))),
      254 => {
        if data.len() < 5 {
          return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Insufficient data for 32-bit length",
          ));
        }
        let length = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        Ok((5, length))
      }
      255 => Err(Error::new(
        ErrorKind::InvalidData,
        "Invalid length encoding (255)",
      )),
    }
  }

  /// Decode an integer from the RDB file
  pub fn decode_integer(&self, data: &[u8]) -> Result<(usize, i64), Error> {
    if data.is_empty() {
      return Err(Error::new(ErrorKind::InvalidData, "Empty data"));
    }

    let first_byte = data[0];
    match first_byte {
      0xC0 => Ok((2, data[1] as i64)),
      0xC1 => {
        if data.len() < 3 {
          return Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data for 16-bit integer",
          ));
        }
        Ok((3, i16::from_le_bytes([data[1], data[2]]) as i64))
      }
      0xC2 => {
        if data.len() < 5 {
          return Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data for 32-bit integer",
          ));
        }
        Ok((
          5,
          i32::from_le_bytes([data[1], data[2], data[3], data[4]]) as i64,
        ))
      }
      0xC3 => {
        if data.len() < 9 {
          return Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data for 64-bit integer",
          ));
        }
        let bytes = [
          data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ];
        Ok((9, i64::from_le_bytes(bytes)))
      }
      192..=223 => Ok((1, (first_byte & 0x3f) as i64)),
      _ => Err(Error::new(
        ErrorKind::InvalidData,
        format!("Invalid integer encoding: {}", first_byte),
      )),
    }
  }

  /// Decode a length encoded data
  fn decode_length_encoded_data(&self, data: &[u8]) -> Result<(usize, Vec<u8>), Error> {
    debug!("Decoding length-encoded data. Data length: {}", data.len());

    if data.is_empty() {
      return Err(Error::new(
        ErrorKind::UnexpectedEof,
        "Empty data when decoding length-encoded data",
      ));
    }

    let (length_bytes, length) = self.decode_length(data)?;
    debug!(
      "Decoded length: {} bytes, length encoding used {} bytes",
      length, length_bytes
    );

    let total_bytes = length_bytes + length;

    if data.len() < total_bytes {
      error!(
        "Insufficient data for encoded string. Need {} bytes, have {}",
        total_bytes,
        data.len()
      );
      return Err(Error::new(
        ErrorKind::UnexpectedEof,
        format!(
          "Insufficient data for encoded string. Need {} bytes, have {}",
          total_bytes,
          data.len()
        ),
      ));
    }

    let result = data[length_bytes..total_bytes].to_vec();
    Ok((total_bytes, result))
  }

  /// Parse the auxiliary fields from the RDB file
  pub fn parse_auxiliary_fields(
    &self,
    data: &[u8],
  ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, usize), Error> {
    let mut fields: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut index = 9; // Start after the RDB version

    while index < data.len() && data[index] == 0xFA {
      index += 1; // Skip the 0xFA marker

      // Decode key
      let (key_bytes, key) = self.decode_length_encoded_data(&data[index..])?;
      index += key_bytes;

      // Decode value
      let value = if data[index] >= 0xC0 && data[index] <= 0xC3 {
        // Integer encoding
        let (int_bytes, int_value) = self.decode_integer(&data[index..])?;
        index += int_bytes;
        int_value.to_le_bytes().to_vec()
      } else {
        // String encoding
        let (str_bytes, str_value) = self.decode_length_encoded_data(&data[index..])?;
        index += str_bytes;
        str_value
      };

      fields.push((key, value));
    }

    Ok((fields, index))
  }

  /// Decode the value from the RDB file
  pub fn decode_value(
    &self,
    data: &[u8],
    value_type: u8,
    index: &mut usize,
  ) -> Result<Vec<u8>, Error> {
    match value_type {
      0 => {
        // String encoding
        let (value_bytes, value) = self.decode_length_encoded_data(&data[*index..])?;
        *index += value_bytes;
        Ok(value)
      }
      1 => {
        // List encoding
        let (length_bytes, length) = self.decode_length(&data[*index..]).unwrap();
        *index += length_bytes;
        let mut list = Vec::new();
        for _ in 0..length {
          let (value_bytes, value) = self.decode_length_encoded_data(&data[*index..])?;
          *index += value_bytes;
          list.extend_from_slice(&value);
          list.push(b',');
        }
        if !list.is_empty() {
          list.pop();
        }

        Ok(list)
      }
      2 => {
        // Set encoding
        let (length_bytes, length) = self.decode_length(&data[*index..]).unwrap();
        *index += length_bytes;
        let mut set = Vec::new();
        for _ in 0..length {
          let (value_bytes, value) = self.decode_length_encoded_data(&data[*index..])?;
          *index += value_bytes;
          set.extend_from_slice(&value);
          set.push(b',');
        }
        if !set.is_empty() {
          set.pop();
        }
        Ok(set)
      }
      3 => {
        // Sorted set encoding
        let (length_bytes, length) = self.decode_length(&data[*index..]).unwrap();
        *index += length_bytes;
        let mut sorted_set = Vec::new();
        for _ in 0..length {
          let (member_bytes, member) = self.decode_length_encoded_data(&data[*index..])?;
          *index += member_bytes;
          let (score_bytes, score) = self.decode_length(&data[*index..]).unwrap();
          *index += score_bytes;

          sorted_set.extend_from_slice(&member);
          sorted_set.push(b':');
          sorted_set.extend_from_slice(&score.to_le_bytes());
          sorted_set.push(b',');
        }
        if !sorted_set.is_empty() {
          sorted_set.pop();
        }
        Ok(sorted_set)
      }
      4 => {
        // Hash encoding
        let (length_bytes, length) = self.decode_length(&data[*index..]).unwrap();
        *index += length_bytes;
        let mut hash = Vec::new();

        for _ in 0..length {
          let (field_bytes, field) = self.decode_length_encoded_data(&data[*index..])?;
          *index += field_bytes;
          let (value_bytes, value) = self.decode_length_encoded_data(&data[*index..])?;
          *index += value_bytes;

          hash.extend_from_slice(&field);
          hash.push(b':');
          hash.extend_from_slice(&value);
          hash.push(b',');
        }
        if !hash.is_empty() {
          hash.pop();
        }
        Ok(hash)
      }
      9 | 10 | 11 | 12 => {
        // Integer encodings
        let (int_bytes, int_value) = self.decode_integer(&data[*index..])?;
        *index += int_bytes;
        Ok(int_value.to_le_bytes().to_vec())
      }
      55 => {
        // This might be a specific Redis encoding. For now, we'll treat it as a raw byte.
        warn!("Encountered encoding type 55, treating as raw byte");
        if *index < data.len() {
          let value = vec![data[*index]];
          *index += 1;
          Ok(value)
        } else {
          Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Unexpected end of data",
          ))
        }
      }
      // Add handling for the problematic encoding (250)
      250 => {
        // This might be a special encoding. For now, we'll treat it as a raw byte.
        if *index < data.len() {
          let value = vec![data[*index]];
          *index += 1;
          Ok(value)
        } else {
          Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Unexpected end of data",
          ))
        }
      }
      _ => Err(Error::new(
        ErrorKind::InvalidData,
        format!("Unknown or unsupported encoding: {}", value_type),
      )),
    }
  }

  /// Process expire keys
  /// This function is responsible for processing keys that have an expiry time
  fn process_expiry_entry(
    &mut self,
    data: &[u8],
    expiry: SystemTime,
    index: &mut usize,
  ) -> Result<(), Error> {
    let value_type = data[*index];
    *index += 1;

    let (key_bytes, key) = self.decode_length_encoded_data(&data[*index..])?;
    *index += key_bytes;

    let value = self.decode_value(data, value_type, index)?;
    self.expiry_entries.push((key, value, expiry));
    Ok(())
  }

  /// Process non-expiry keys
  /// This function is responsible for processing keys that do not have an expiry time
  pub fn process_non_expiry_entry(&mut self, data: &[u8], index: &mut usize) -> Result<(), Error> {
    let value_type = data[*index];
    *index += 1;

    let (key_bytes, key) = self.decode_length_encoded_data(&data[*index..])?;
    *index += key_bytes;

    let value = self.decode_value(data, value_type, index)?;
    self.entries.push((key, value));
    Ok(())
  }

  /// Process all database entries
  /// This function is responsible for processing all database entries

  pub fn process_entries(
    &self,
    data: &[u8],
  ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>, SystemTime)>), Error> {
    let mut index = 0;
    let mut entries = Vec::new();
    let mut expiry_entries = Vec::new();

    while index < data.len() {
      match data[index] {
        0xFE => {
          // Database selector
          index += 2; // Skip selector and DB number
          if index < data.len() && data[index] == 0xFB {
            // Resizedb field
            index += 1;
            let (size_bytes, _) = self.decode_length(&data[index..])?;
            index += size_bytes;
            let (expire_size_bytes, _) = self.decode_length(&data[index..])?;
            index += expire_size_bytes;
          }
        }
        0xFD | 0xFC => {
          // Expiry time
          let (expiry_bytes, expiry_time) = if data[index] == 0xFD {
            (
              5,
              SystemTime::UNIX_EPOCH
                + Duration::from_secs(u32::from_le_bytes([
                  data[index + 1],
                  data[index + 2],
                  data[index + 3],
                  data[index + 4],
                ]) as u64),
            )
          } else {
            (
              9,
              SystemTime::UNIX_EPOCH
                + Duration::from_millis(u64::from_le_bytes([
                  data[index + 1],
                  data[index + 2],
                  data[index + 3],
                  data[index + 4],
                  data[index + 5],
                  data[index + 6],
                  data[index + 7],
                  data[index + 8],
                ])),
            )
          };
          index += expiry_bytes;
          let (key, value) = self.process_key_value_pair(data, &mut index)?;
          expiry_entries.push((key, value, expiry_time));
        }
        0xFF => {
          // End of RDB file
          break;
        }
        _ => {
          // Key-value pair without expiry
          let (key, value) = self.process_key_value_pair(data, &mut index)?;
          entries.push((key, value));
        }
      }
    }

    Ok((entries, expiry_entries))
  }

  fn process_key_value_pair(
    &self,
    data: &[u8],
    index: &mut usize,
  ) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let value_type = data[*index];
    *index += 1;

    let (key_bytes, key) = self.decode_length_encoded_data(&data[*index..])?;
    *index += key_bytes;

    let value = self.decode_value(data, value_type, index)?;

    Ok((key, value))
  }
}
