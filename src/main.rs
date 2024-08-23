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
    println!("accepted new connection");
    tokio::spawn(async move {
        loop {
            let mut buf = [0; 512];
            let read_count = stream.read(&mut buf).await.unwrap();
            if read_count == 0 {
                break;
            }
            stream.write(b"+PONG\r\n").await.unwrap();
        }
    });
}
