use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use crate::config::Config;

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
                config.set("dir".to_string(), argument_value);
            }
            "--dbfilename" => {
                println!("DBFilename: {}", argument_value);
                config.set("dbfilename".to_string(), argument_value);
            }
            _ => {
                eprintln!("Unknown option: {}", argument);
            }
        }
    }
}
