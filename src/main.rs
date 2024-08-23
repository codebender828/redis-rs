use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() {
    println!("Starting Redis Server!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, _)) => handle_connection(stream),
            Err(e) => {
                println!("error: {}", e);
            }
        };
    }
}

/** Handles TCP connections to Redis Server */
fn handle_connection(mut stream: TcpStream) {
    println!("Accepted new connection");
    tokio::spawn(async move {
        loop {
            let mut buf = [0; 512];

            match stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    println!("Received {} bytes", n);
                    if let Err(e) = stream.write_all(b"+PONG\r\n").await {
                        println!("Failed to write to stream; err = {:?}", e);
                        break;
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
