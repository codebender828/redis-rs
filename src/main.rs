use std::sync::Arc;

use parser::{parse_command, serialize_response, Command, RedisValue};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex as AsyncMutex;

pub mod parser;
// import the storage module
pub mod storage;
use storage::Storage;

#[tokio::main]
async fn main() {
    println!("Starting Redis Server!");

    let _storage = Arc::new(AsyncMutex::new(Storage::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        let _storage = _storage.clone();

        match stream {
            Ok((stream, _)) => handle_connection(stream, _storage),
            Err(e) => {
                println!("error: {}", e);
            }
        };
    }
}

/** Handles TCP connections to Redis Server */
fn handle_connection(mut stream: TcpStream, storage: Arc<AsyncMutex<Storage>>) {
    println!("Accepted new connection");
    tokio::spawn(async move {
        loop {
            let mut buf = [0; 512];
            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    println!("Received {} bytes", n);
                    match parse_command(&buf[..n]) {
                        Ok(Command::Ping(message)) => {
                            let response = match message {
                                Some(msg) => {
                                    serialize_response(RedisValue::SimpleString(msg.to_string()))
                                }
                                None => {
                                    serialize_response(RedisValue::SimpleString("PONG".to_string()))
                                }
                            };
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Echo(message)) => {
                            let response =
                                serialize_response(RedisValue::SimpleString(message.to_string()));
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Unknown(cmd)) => {
                            eprintln!("Unknown command: {}", cmd);
                            let response = serialize_response(RedisValue::BulkString(Some(
                                format!("ERR Unknown command: {}", cmd),
                            )));
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Set(key, value, optional_ags)) => {
                            // Handle all optional parameters
                            let storage = storage.lock().await;
                            storage.set(key, value, optional_ags.unwrap_or_default());

                            let response =
                                serialize_response(RedisValue::SimpleString("OK".to_string()));
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Get(key)) => {
                            eprintln!("GET command: key = {}", key);
                            let storage = storage.lock().await;
                            let response = match storage.get(&key) {
                                Some(value) => {
                                    serialize_response(RedisValue::BulkString(Some(value)))
                                }
                                None => serialize_response(RedisValue::BulkString(None)),
                            };
                            println!("Response: {:?}", response);
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to parse command: {}", e);
                            let response = serialize_response(RedisValue::BulkString(Some(
                                format!("ERR Failed to parse command: {}", e),
                            )));
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
