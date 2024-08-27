use std::sync::Arc;

use parser::{parse_command, serialize_response, Command, RedisValue};
use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

pub mod parser;
// import the storage module
pub mod storage;
use storage::Storage;

pub mod config;
use config::Config;

pub mod arguments;
use arguments::{parse_cli_arguments, process_configuration_arguments};

pub mod database;
use database::populate_hot_storage;

#[tokio::main]
async fn main() {
  println!("Starting Redis Server!");

  let mut args: Vec<String> = env::args().collect();
  // Remove the first argument which is the binary name
  args.remove(0);

  let arguments = parse_cli_arguments(args);

  let _config = Arc::new(AsyncMutex::new(Config::new()));
  let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

  let _storage = Arc::new(AsyncMutex::new(Storage::new()));
  process_configuration_arguments(arguments, _config.clone()).await;
  populate_hot_storage(&_storage, &_config).await;

  loop {
    let stream = listener.accept().await;
    let storage = _storage.clone();
    let config = _config.clone();

    match stream {
      Ok((stream, _)) => handle_connection(stream, storage, config),
      Err(e) => {
        println!("error: {}", e);
      }
    };
  }
}

/** Handles TCP connections to Redis Server */
fn handle_connection(
  mut stream: TcpStream,
  storage: Arc<AsyncMutex<Storage>>,
  config: Arc<AsyncMutex<Config>>,
) {
  println!("Accepted new connection");
  tokio::spawn(async move {
    loop {
      let mut buf = [0; 512];
      match stream.read(&mut buf).await {
        Ok(0) => break,
        Ok(n) => {
          println!("Received {} bytes", n);
          match parse_command(&buf[..n]) {
            Ok(Command::PING(message)) => {
              let response = match message {
                Some(msg) => serialize_response(RedisValue::SimpleString(msg.to_string())),
                None => serialize_response(RedisValue::SimpleString("PONG".to_string())),
              };
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Ok(Command::ECHO(message)) => {
              let response = serialize_response(RedisValue::SimpleString(message.to_string()));
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Ok(Command::UNKNOWN(cmd)) => {
              eprintln!("Unknown command: {}", cmd);
              let response = serialize_response(RedisValue::BulkString(Some(format!(
                "ERR Unknown command: {}",
                cmd
              ))));
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Ok(Command::SET(key, value, optional_ags)) => {
              // Handle all optional parameters
              let storage = storage.lock().await;
              storage.set(key, value, optional_ags.unwrap_or_default());

              let response = serialize_response(RedisValue::SimpleString("OK".to_string()));
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Ok(Command::GET(key)) => {
              eprintln!("GET command: key = {}", key);
              let storage = storage.lock().await;
              let response = match storage.get(&key) {
                Some(value) => serialize_response(RedisValue::BulkString(Some(value))),
                None => serialize_response(RedisValue::BulkString(None)),
              };
              println!("Response: {:?}", response);
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Ok(Command::CONFIGGET(entry)) => {
              let config = config.lock().await;
              let value = config.get(&entry);
              let mut result = Vec::new();
              result.push(entry);
              result.push(value.unwrap_or_default());
              let response = serialize_response(RedisValue::Array(result));
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
            Err(e) => {
              eprintln!("Failed to parse command: {}", e);
              let response = serialize_response(RedisValue::BulkString(Some(format!(
                "ERR Failed to parse command: {}",
                e
              ))));
              if let Err(e) = stream.write_all(response.as_bytes()).await {
                println!("Failed to write to stream; err = {:?}", e);
                break;
              }
            }
          }
        }
        Err(e) => {
          println!("Failed to read from stream; err = {:?}", e);
          break;
        }
      }
    }
  });
}
