use std::collections::HashMap;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufStream},
    net::{TcpListener, TcpStream},
};

pub async fn handle_connection(mut socket: TcpStream) {
    // let mut buf = BytesMut::with_capacity(1024);
    let mut buf = [0; 1024];

    let mut hash_map: HashMap<String, String> = HashMap::new();
    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("Connection closed by client");
                    break;
                }

                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                println!("Received request: {}", request);

                let decoded_str: Vec<String> =
                    decode_resp_bulk_string(request.to_string()).unwrap();
                let command: &String = &decoded_str[0];
                println!("Decoded string: {:?}", decoded_str);

                match command.as_str() {
                    "echo" => {
                        let response = encode_resp_bulk_string(&decoded_str[1]);
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("ECHO: Failed to write to client: {}", e);
                            break;
                        }
                    }
                    "ping" => {
                        if let Err(e) = socket.write_all("+PONG\r\n".as_bytes()).await {
                            eprintln!("PING: Failed to write to client: {}", e);
                            break;
                        }
                    }
                    "set" => {
                        hash_map.insert(decoded_str[1].to_owned(), decoded_str[2].to_owned());
                        println!("Inserted into hashmap: {:?}", hash_map);
                        if let Err(e) = socket.write_all("+OK\r\n".as_bytes()).await {
                            eprintln!("SET: Failed to write to client: {}", e);
                        }
                        break;
                    }
                    "get" => {
                        if let Some(key) = hash_map.get(&decoded_str[1]) {
                            let response = encode_resp_bulk_string(key);
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                eprintln!("GET: Failed to write to client: {}", e);
                            }
                        }
                        if (socket.write_all("$-1\r\n".as_bytes()).await).is_err() {
                            eprintln!("GET: Null bulk string");
                        }
                        break;
                    }
                    _ => {
                        eprintln!("Failed to write to client.");
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }
    }
}
// TODO: Fix
fn decode_resp_bulk_string(input: String) -> Option<Vec<String>> {
    if let Some(dollar_byte_idx) = input.find('$') {
        let bulk_string: Vec<String> =
            get_command_and_arguments(input[dollar_byte_idx..].to_string());
        Some(bulk_string)
    } else {
        None
    }
}

fn get_command_and_arguments(input: String) -> Vec<String> {
    let mut ptr = 0;
    let mut commands: Vec<String> = Vec::new();
    let mut result: Vec<String> = Vec::new();

    for (idx, char) in input.char_indices() {
        if char == '\n' {
            ptr = idx;
        }
        if char == '\r' {
            let command = &input[ptr + 1..idx];
            commands.push(command.to_string());
        }
    }

    for (idx, command) in commands.iter().enumerate() {
        if idx % 2 == 1 {
            result.push(command.to_string());
        }
    }

    result
}

fn encode_resp_bulk_string(input: &str) -> String {
    let length = input.len().to_string();
    let response = format!(
        "{}{}{}{}{}",
        String::from("$"),
        length,
        String::from("\r\n"),
        input,
        String::from("\r\n")
    );
    response
}
