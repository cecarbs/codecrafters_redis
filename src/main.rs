// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    //
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_client(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut buf = [0; 1024];
    loop {
        // let bytes_read = stream.read(&mut buf).expect("Failed to read from client");
        //
        // if bytes_read == 0 {
        //     return;
        // }

        let response = "+PONG\r\n";
        stream
            .write_all(response.as_bytes())
            .expect("Failed to write to client");
    }
}
