use dashmap::DashMap;
use parser::{parse_command, serialize_response, Command};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub mod parser;

#[tokio::main]
async fn main() {
    println!("Starting Redis Server!");

    let storage: &'static DashMap<String, String> = Box::leak(Box::new(DashMap::new()));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => handle_connection(stream, &storage),
            Err(e) => {
                println!("error: {}", e);
            }
        };
    }
}

/** Handles TCP connections to Redis Server */
fn handle_connection(mut stream: TcpStream, storage: &'static DashMap<String, String>) {
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
                                Some(msg) => serialize_response(&msg),
                                None => serialize_response("PONG"),
                            };
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Echo(message)) => {
                            let response = serialize_response(&message);
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Unknown(cmd)) => {
                            eprintln!("Unknown command: {}", cmd);
                            let response =
                                serialize_response(&format!("ERR Unknown command: {}", cmd));
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Set(key, value)) => {
                            eprintln!("SET command: key = {}, value = {}", key, value);
                            storage.insert(key, value);
                            let response = serialize_response("OK");
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Ok(Command::Get(key)) => {
                            eprintln!("GET command: key = {}", key);
                            let response = match storage.get(&key) {
                                Some(value) => serialize_response(&value),
                                None => format!("$-1\r\n"),
                            };
                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                println!("Failed to write to stream; err = {:?}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to parse command: {}", e);
                            let response =
                                serialize_response(&format!("ERR Failed to parse command: {}", e));
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
