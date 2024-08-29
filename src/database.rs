/**
 * This file is responsible for interacting with the raw RDB file
 * At a high level, the RDB file format has the following structure
 *
 * @source https://rdb.fnordig.de/file_format.html
 * Example file:
 * ```rdb
 * 524544495330303130fa0972656469732d76657206372e302e3130fa0a72656469732d62697473c040fa056374696d65c2d5bbcc66fa08757365642d6d656dc2d0171100fa08616f662d62617365c000fe00fb0201fc86de7dad91010000000362617a037a61670003666f6f03626172ff20b3abf967cff893
 * ```
 *
 */
use crate::{config::Config, storage::Storage};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use std::io::{Error, ErrorKind};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::vec;
use std::{str, sync::Arc};
use tokio::sync::Mutex;

/// Auxiliary value type
#[derive(Debug, Clone)]
pub enum AuxValue {
  String(String),
  Integer(i64),
}

pub async fn populate_hot_storage(storage: &Arc<Mutex<Storage>>, config: &Arc<Mutex<Config>>) {
  // Extract the directory and dbfilename from the configuration
  // and populate the storage with the data

  let storage = storage.lock().await;
  let config = config.lock().await;

  // Extract the directory and dbfilename from the configuration
  // and populate the storage with the data
  // If the dir or dbfilename is not present, return early
  if !config.has("dir") || !config.has("dbfilename") {
    info!("Configuration does not contain dir or dbfilename. Skipping read.");
    return;
  }

  let directory = config.get("dir").unwrap();
  let dbfilename = config.get("dbfilename").unwrap();
  let rdb_file_path = format!("{}/{}", directory, dbfilename);

  println!("Reading RDB file: {}", rdb_file_path);

  let rdb_data = match std::fs::read(&rdb_file_path) {
    Ok(data) => data,
    Err(e) => {
      error!("Failed to read RDB file: {}", e);
      return;
    }
  };

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

  parser.entries.iter().for_each(|(key, value)| {
    let key = RDBParser::stringify(key);
    let value = RDBParser::stringify(value);
    storage.set(key, value, vec![]);
  });

  parser
    .expiry_entries
    .iter()
    .for_each(|(key, value, expiry_time)| {
      let key = RDBParser::stringify(key);
      let value = RDBParser::stringify(value);

      let now = SystemTime::now();
      let time_since_expiry = expiry_time.duration_since(now).unwrap_or_default();

      storage.set(
        key,
        value,
        vec![("EX".to_string(), time_since_expiry.as_secs().to_string())],
      );
    });

  drop(parser)
}

/// Parser struct for the RDBParser
#[derive(Debug)]
pub struct RDBParser {
  /// Raw file data for the RDB file
  data: Vec<u8>,
  keys: Vec<Vec<u8>>,
  rdb_version: u32,
  aux_fields: DashMap<String, AuxValue>,
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
      aux_fields: DashMap::new(),
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

    let auxiliary_fields = aux_fields.clone();
    self.print_rdb_info(self.rdb_version, auxiliary_fields);

    self.aux_fields = aux_fields;
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

  pub fn stringify(value: &[u8]) -> String {
    String::from_utf8_lossy(value).into_owned()
  }

  /// Print the RDB file information
  fn print_rdb_info(&self, version: u32, aux_fields: DashMap<String, AuxValue>) {
    println!("RDB file version: {}", version);
    println!("Auxiliary Fields:");
    for entry in aux_fields.iter() {
      match entry.value() {
        AuxValue::String(s) => println!("  {}: {}", entry.key(), s),
        AuxValue::Integer(i) => println!("  {}: {}", entry.key(), i),
      }
    }

    // Explicitly print redis-bits if it exists
    if let Some(entry) = aux_fields.get("redis-bits") {
      if let AuxValue::Integer(redis_bits) = entry.value() {
        println!("\nRedis Bits: {}", redis_bits);
      }
    } else {
      println!("\nRedis Bits: Not found in auxiliary fields");
    }
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
  ) -> Result<(DashMap<String, AuxValue>, usize), Error> {
    let fields = DashMap::new();
    let mut index = 9; // Start after the RDB version

    while index < data.len() && data[index] == 0xFA {
      index += 1; // Skip the 0xFA marker

      // Decode key
      let (key_bytes, key) = self.decode_length_encoded_data(&data[index..])?;
      index += key_bytes;

      let key_string = str::from_utf8(&key)
        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?
        .to_string();

      // check if value is integrer
      if (data[index] >= 0xC0 && data[index] <= 0xC3) || (data[index] >= 192 && data[index] <= 223)
      {
        let (int_bytes, int_value) = self.decode_integer(&data[index..])?;
        fields.insert(key_string, AuxValue::Integer(int_value));
        index += int_bytes;
      } else {
        let (value_bytes, value) = self.decode_length_encoded_data(&data[index..])?;
        fields.insert(
          key_string,
          AuxValue::String(value.iter().map(|&x| x as char).collect()),
        );
        index += value_bytes;
      }
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
