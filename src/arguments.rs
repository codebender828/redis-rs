use crate::config::Config;
use std::fs::create_dir_all;
use std::fs::File;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

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

        // Create the file if it doesn't exist
        let file_path = format!(
          "{}/{}",
          config.get("dir").unwrap(),
          config.get("dbfilename").unwrap()
        );
        File::create(file_path).unwrap();
      }
      _ => {
        eprintln!("Unknown option: {}", argument);
      }
    }
  }
}
