mod helper_cli;
mod timed_hashmap;
use std::time::Duration;

use helper_cli::{HelperCLI, Role};
use timed_hashmap::TimedHashMap;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

pub async fn handle_connection(mut socket: TcpStream, role: String) {
    // let mut buf = BytesMut::with_capacity(1024);
    let mut buf = [0; 1024];
    let mut timed_hashmap: TimedHashMap<String, String> = TimedHashMap::new();

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
                        if role == "master" {
                            if let Err(e) =
                                socket.write_all(encode_resp_array("ping").as_bytes()).await
                            {
                                eprintln!("PING: Failed to write to client: {}", e);
                                break;
                            }
                        }
                        if role == "slave" {
                            if let Err(e) =
                                socket.write_all(encode_resp_array("ping").as_bytes()).await
                            {
                                eprintln!("PING: Failed to write to client: {}", e);
                                break;
                            }
                        }
                        // if let Err(e) = socket.write_all("+PONG\r\n".as_bytes()).await {
                        //     eprintln!("PING: Failed to write to client: {}", e);
                        //     break;
                        // }
                    }
                    "set" => match decoded_str.len() {
                        3 => {
                            timed_hashmap.insert(
                                decoded_str[1].to_owned(),
                                decoded_str[2].to_owned(),
                                None,
                            );
                            println!("Inserted into hashmap: {:?}", timed_hashmap);
                            if let Err(e) = socket.write_all("+OK\r\n".as_bytes()).await {
                                eprintln!("SET: Failed to write to client: {}", e);
                                break;
                            }
                        }
                        5 => {
                            // TODO: only works for 'px' implement other variation(s)
                            let milliseconds: u64 = decoded_str[4].to_owned().parse().unwrap();
                            let ttl: Duration = Duration::from_millis(milliseconds);

                            timed_hashmap.insert(
                                decoded_str[1].to_owned(),
                                decoded_str[2].to_owned(),
                                Some(ttl),
                            );

                            if let Err(e) = socket.write_all("+OK\r\n".as_bytes()).await {
                                eprintln!("SET: Failed to write to client: {}", e);
                                break;
                            }
                        }
                        _ => {
                            println!("Unable to insert into either hashmaps.");
                        }
                    },
                    "get" => {
                        println!("Entering get command.");
                        timed_hashmap.remove_expired_entries();

                        if let Some(key) = timed_hashmap.get(&decoded_str[1]) {
                            println!("Hashmap: {:?}", timed_hashmap);

                            let response = encode_resp_bulk_string(key);

                            println!("Attempting to send response: {:?}", response);

                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                eprintln!("GET: Failed to write to client: {}", e);
                                break;
                            }
                        } else {
                            if let Err(_) = socket.write_all("$-1\r\n".as_bytes()).await {
                                eprintln!("Null bulk string.");
                                break;
                            }
                        }
                    }
                    "info" => {
                        println!("Entering info command.");

                        let encoded_role =
                            encode_resp_bulk_string(format!("role:{}", role).as_str());
                        let encoded_master_replid = encode_resp_bulk_string(
                            format!(
                                "master_replid:{}",
                                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                            )
                            .as_str(),
                        );

                        let encoded_master_repl_offset =
                            encode_resp_bulk_string("master_repl_offset:0");

                        let response = format!(
                            "{}{}{}",
                            encoded_role, encoded_master_replid, encoded_master_repl_offset
                        );

                        println!("Response is: response: {}", response);
                        let response = encode_resp_bulk_string(response.as_str());
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("INFO: Failed to write to client: {}", e);
                            break;
                        }
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
        // String::from("$"),
        insert_correct_protocol("bulk_strings").unwrap(),
        length,
        String::from("\r\n"),
        input,
        String::from("\r\n")
    );
    response
}

fn encode_resp_array(input: &str) -> String {
    let length = input.len().to_string();
    let response = format!(
        "{}{}{}{}{}",
        // String::from("*"),
        insert_correct_protocol("arrays").unwrap(),
        length,
        String::from("\r\n"),
        input,
        String::from("\r\n")
    );
    response
}

// TODO: create enum
fn insert_correct_protocol(input: &str) -> Result<String, &'static str> {
    match input {
        "simple_strings" => Ok("+".to_string()),
        "errors" => Ok("-".to_string()),
        "integers" => Ok(":".to_string()),
        "bulk_strings" => Ok("$".to_string()),
        "arrays" => Ok("*".to_string()),
        _ => Err("Unable to determine correct Redis protocol."),
    }
}
