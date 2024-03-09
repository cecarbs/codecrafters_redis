// Uncomment this block to pass the first stage
use std::error::Error;

use bytes::{BufMut, Bytes, BytesMut};
use redis_starter_rust::handle_connection;
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
