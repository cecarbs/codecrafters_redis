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
            Ok((socket, addr)) => {
                println!("accepted new connection");

                tokio::spawn(handle_connection(socket));
            }
            Err(e) => println!("couldn't get client: {:?}", e),
        }
    }
}

async fn handle_connection(mut socket: TcpStream) {
    let mut buf = [0; 1024];

    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    // Connection closed by the client
                    println!("Connection closed by client");
                    break;
                }

                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                println!("Received request: {}", request);

                // Process the request and prepare the response
                let response = "+PONG\r\n";

                // Write the response to the client
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
