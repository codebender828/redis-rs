use crate::config::Config;
use log::info;
use nanoid::nanoid;
use std::fs::create_dir_all;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

const ALPHABET: [char; 62] = [
  '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
  'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
  'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
  'V', 'W', 'X', 'Y', 'Z',
];

pub type CLIArguments = Vec<(String, String)>;

/// Parses CLI arguments into tuple
pub fn parse_cli_arguments(options: Vec<String>) -> CLIArguments {
  options
    .into_iter()
    .filter(|s| !s.is_empty())
    .collect::<Vec<String>>()
    .chunks(2)
    .filter_map(|chunk| {
      if chunk.len() == 2 {
        Some((chunk[0].clone().to_lowercase(), chunk[1].clone()))
      } else {
        None
      }
    })
    .collect()
}

pub async fn process_configuration_arguments(
  arguments: CLIArguments,
  config: Arc<AsyncMutex<Config>>,
) {
  let config = config.lock().await;
  for (argument, argument_value) in arguments {
    match argument.as_str() {
      "--dir" => {
        println!("Dir: {}", argument_value);
        let directory = argument_value.clone();
        config.set("dir".to_string(), argument_value);
        // Create the directory if it doesn't exist
        create_dir_all(directory.clone()).unwrap();
      }
      "--dbfilename" => {
        println!("DBFilename: {}", argument_value);
        config.set("dbfilename".to_string(), argument_value);

        let file_path = format!(
          "{}/{}",
          config.get("dir").unwrap(),
          config.get("dbfilename").unwrap()
        );
        // Create the file if it doesn't exist
        let file_path = Path::new(&file_path);
        // check if the file exists
        if !file_path.exists() {
          File::create(file_path).unwrap();
        }
      }
      "--replicaof" => {
        info!(
          "Role: Slave. This redis instance is a replica of {}",
          argument_value
        );
        let directory = argument_value.clone();
        config.set("replicaof".to_string(), argument_value);
        // Create the directory if it doesn't exist
        create_dir_all(directory.clone()).unwrap();
      }
      _ => {
        // If there is no replicaof argument, then this instance is a master.
        // generate random id
        let replication_id = nanoid!(40, &ALPHABET);
        config.set("replication_id".to_string(), replication_id.to_string());
        config.set("replication_offset".to_string(), "0".to_string());
      }
    }
  }
}
