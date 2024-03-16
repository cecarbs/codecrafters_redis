mod timed_hashmap;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use self::timed_hashmap::TimedHashMap;

#[warn(dead_code)]
enum Command {
    Echo,
    Ping,
    Set,
    Get,
    Info,
    Unknown,
}

pub async fn start_master(port: &str) {
    let listener: TcpListener = TcpListener::bind(port).await.unwrap();
    println!("Master started on port: {}", port);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    handle_connection(socket, "master").await;
                });
            }
            Err(_) => eprintln!("Failed to start master instance."),
        }
    }
}

pub async fn start_replica(master_address: &str, port: &str) {
    let listener = TcpListener::bind(port).await.unwrap();
    println!("Replica started on port: {}", port);

    let master_stream = TcpStream::connect(master_address).await.unwrap();
    send_ping_to_master(master_stream).await;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    handle_connection(socket, "slave").await;
                });
            }
            Err(_) => eprintln!("Failed to start replica instance."),
        }
    }
}

async fn send_ping_to_master(mut stream: TcpStream) {
    let ping = encode_resp_array("PING");
    if let Err(e) = stream.write_all(ping.as_bytes()).await {
        eprintln!("Failed to send Ping to master with error: {:?}", e);
    }
}

async fn handle_connection(mut socket: TcpStream, role: &str) {
    let mut buf = [0; 1024];
    let mut timed_hashmap: TimedHashMap<String, String> = TimedHashMap::new();

    loop {
        match socket.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("Connection closed by client.")
                }

                // let decoded_string =
                //     parse_command_from_request(String::from_utf8(&buf[..bytes_read]).unwrap());

                let request = String::from_utf8_lossy(&buf[..bytes_read]);
                let decoded_str: Vec<String> =
                    decode_resp_bulk_string(request.to_string()).unwrap();
                let command: Command = match decoded_str[0].as_str() {
                    "echo" => Command::Echo,
                    "ping" => Command::Ping,
                    "set" => Command::Set,
                    "get" => Command::Get,
                    "info" => Command::Info,
                    _ => Command::Unknown,
                };

                match command {
                    Command::Echo => {
                        let response = encode_resp_bulk_string(&decoded_str[1]);
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("ECHO: Failed to write to client: {}", e);
                            break;
                        }
                    }
                    Command::Ping => {
                        if role == "master" {
                            let response = encode_resp_bulk_string("PONG");
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                eprintln!("PING - Master: Failed to write to client: {}", e);
                                break;
                            }
                        } else if role == "slave" {
                            let response = encode_resp_array("PONG");
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
                                eprintln!("PING - Slave: Failed to write to client: {}", e);
                                break;
                            }
                        } else {
                            eprintln!("Could not determine role.");
                            break;
                        }
                    }
                    Command::Set => match decoded_str.len() {
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
                    Command::Get => {
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
                            match socket.write_all("$-1\r\n".as_bytes()).await {
                                Ok(_) => (),
                                Err(_) => {
                                    eprintln!("Null bulk string.");
                                    break;
                                }
                            }
                        }
                    }
                    Command::Info => {
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
                    Command::Unknown => {
                        eprintln!("Failed to parse command: unknown command.");
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

// TODO: might need to change this later for more complex commands
// fn parse_command_from_request(request: String) -> Vec<String> {
//     let decoded_str: Vec<String> = decode_resp_bulk_string(request.to_string()).unwrap();
//     let command: &String = &decoded_str[0];
//     println!("Received command: {}", command);
//     decoded_str
// }

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

fn encode_resp_array(input: &str) -> String {
    // TODO: modify this to to get the length of the array
    let resp_bulk_string = encode_resp_bulk_string(input);
    let response = format!("{}{}", String::from("*1\r\n"), resp_bulk_string);
    response
}

// fn insert_correct_protocol(input: &str) -> Result<String, &'static str> {
//     match input {
//         "simple_strings" => Ok("+".to_string()),
//         "errors" => Ok("-".to_string()),
//         "integers" => Ok(":".to_string()),
//         "bulk_strings" => Ok("$".to_string()),
//         "arrays" => Ok("*".to_string()),
//         _ => Err("Unable to determine correct Redis protocol."),
//     }
// }
