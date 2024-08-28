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
use std::io::{Error, ErrorKind};
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

  // Extract the keys that do not have an expiry time
  let keys = extract_non_expire_keys_from_rdb(&rdb_contents);
}

pub fn extract_non_expire_keys_from_rdb(rdb_contents: &String) {
  // Extract the keys that do not have an expiry time
  // from the RDB file

  if rdb_contents.is_empty() {
    // Early return if it comes across an empty file
    return;
  }

  let rdb_data = hex::decode(rdb_contents.trim()).expect("Failed to decode RDB file");

  let mut parser = RDBParser::new(rdb_data);
  let db = parser.parse();
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
pub struct RDBParser {
  /// Raw file data for the RDB file
  data: Vec<u8>,
  keys: Vec<String>,
  entries: Vec<(String, String)>,
  rdb_version: u32,
  redis_version: String,
  creation_time: String,
  aux_fields: Vec<(String, String)>,
  expiry_keys: Vec<(String, String, Instant)>,
}

impl RDBParser {
  /// Create a new RDBParser instance
  pub fn new(data: Vec<u8>) -> Self {
    RDBParser {
      data,
      keys: Vec::new(),
      entries: Vec::new(),
      rdb_version: 0,
      redis_version: String::new(),
      creation_time: String::new(),
      aux_fields: Vec::new(),
      expiry_keys: Vec::new(),
    }
  }

  /// Parse the RDB file
  pub fn parse(&mut self) {
    let rdb_version = match self.parse_rdb_version(&self.data) {
      Ok(version) => version,
      Err(e) => {
        eprintln!("Error parsing RDB version: {}", e);
        return;
      }
    };

    let aux_fields = self
      .parse_auxiliary_fields(&self.data)
      .expect("Unable to parse auxiliary fields");

    self.rdb_version = rdb_version;
    self.aux_fields = aux_fields.clone();

    dbg!(rdb_version);
    dbg!(aux_fields);
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
    let first_byte = data[0];

    dbg!(first_byte);

    match first_byte {
      0..=63 => Ok((1, first_byte as usize)),
      64..=127 => {
        let second_byte = data[1];

        if data.len() < 2 {
          return Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data for medium length string",
          ));
        }
        let length = ((first_byte as usize & 0x3f) << 8) | second_byte as usize;
        Ok((2, length))
      }
      128 => {
        if data.len() < 5 {
          return Err(Error::new(
            ErrorKind::InvalidData,
            "Insufficient data for long length string",
          ));
        }
        let length = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        Ok((5, length))
      }
      192..=255 => {
        // Special integer encodign
        Ok((1, (first_byte & 0x3f) as usize))
      }
      _ => Err(Error::new(
        ErrorKind::InvalidData,
        format!("Invalid length encoding: {}", first_byte),
      )),
    }
  }

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

  /// Decode a length encoded string
  fn decode_length_encoded_string(&self, data: &[u8]) -> (usize, String) {
    let (length_bytes, length) = self
      .decode_length(data)
      .expect("Could not decode length encoded string Invalid string encoding");
    let string = str::from_utf8(&data[length_bytes..length_bytes + length])
      .expect("Invalid UTF-8 string")
      .to_string();
    (length_bytes + length, string)
  }

  /// Parse the auxiliary fields from the RDB file
  pub fn parse_auxiliary_fields(&self, data: &[u8]) -> Result<Vec<(String, String)>, Error> {
    let mut fields: Vec<(String, String)> = Vec::new();
    let mut index = 9;

    while index < data.len() && data[index] == 0xFA {
      index += 1; // Skip the 0xFA marker
      let (key_bytes, key) = self.decode_length_encoded_string(&data[index..]);
      index += key_bytes;

      // Check if the value is an integer
      if (data[index] >= 0xC0 && data[index] <= 0xC3)
        || (data[index] >= 0xC0 && data[index] <= 0xDF)
      {
        let (value_bytes, value) = self.decode_integer(&data[index..])?;
        fields.push((key, value.to_string()));
        index += value_bytes;
      } else {
        let (value_bytes, value) = self.decode_length_encoded_string(&data[index..]);
        index += value_bytes;
        fields.push((key, value));
      }
    }

    Ok(fields)
  }
}
