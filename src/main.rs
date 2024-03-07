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
    //
    // for stream in listener.try_into() {
    //     match stream {
    //         Ok(stream) => {
    //             println!("accepted new connection");
    //             handle_client(stream);
    //         }
    //         Err(e) => {
    //             println!("error: {}", e);
    //         }
    //     }
    // }
}

async fn handle_connection(mut socket: TcpStream) {
    let mut buf = [0; 1024];
    // let mut stream = BufStream::new(socket);

    let bytes_read = socket.read(&mut buf).await;

    println!("bytes read {:?}", bytes_read);

    if bytes_read.unwrap() == 0 {
        return;
    }

    let response = "+PONG\r\n";
    let _ = socket.write_all(response.as_bytes()).await;

    println!("Failed to write to client");
}

// fn handle_client(mut stream: TcpStream) {
//     let mut buf = [0; 1024];
//     loop {
//         let bytes_read = stream.read(&mut buf).expect("Failed to read from client");
//
//         if bytes_read == 0 {
//             return;
//         }
//
//         let response = "+PONG\r\n";
//         stream
//             .write_all(response.as_bytes())
//             .expect("Failed to write to client");
//
//         println!("Responded with PONG");
//     }
// }
