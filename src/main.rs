// Uncomment this block to pass the first stage
use std::{
    error::Error,
    io::{Read, Write},
};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                println!("Established connection with client.");

                tokio::spawn(handle_connection(socket));
            }
            Err(e) => println!("Failed to establish connection with client: {:?}", e),
        }
    }
}

async fn handle_connection(mut socket: TcpStream) {
    let mut buf = [0u8; 512 * 1024 * 1024];

    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("Connection closed by client");
                    break;
                }

                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                println!("Received request: {}", request);

                let response = "+PONG\r\n";

                if let Err(e) = socket.write_all(response.as_bytes()).await {
                    eprintln!("Failed to write to client: {}", e);
                    break;
                }
            }
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }
    }
}

fn decode_resp_bulk_string() {}
