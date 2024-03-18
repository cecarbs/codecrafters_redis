pub mod master;
pub mod replica;
mod timed_hashmap;

use std::borrow::Cow;
use std::fmt::Display;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use self::timed_hashmap::TimedHashMap;

#[warn(dead_code)]
enum Command {
    Echo,
    Ping,
    Set,
    Get,
    Info,
    Replconf,
    Psync,
    Unknown,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Echo => write!(f, "echo"),
            Command::Ping => write!(f, "ping"),
            Command::Set => write!(f, "set"),
            Command::Get => write!(f, "get"),
            Command::Info => write!(f, "info"),
            Command::Replconf => write!(f, "replconf"),
            Command::Psync => write!(f, "psync"),
            Command::Unknown => write!(f, "unknown"),
        }
    }
}

async fn handle_connection(mut socket: TcpStream, role: &str, replication_id: &String) {
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

                let request: Cow<'_, str> = String::from_utf8_lossy(&buf[..bytes_read]);
                let decoded_str: Vec<String> =
                    decode_resp_bulk_string(request.to_string()).unwrap();
                // TODO: refactor this so that all you need to do is make changes to Enum
                let command: Command = match decoded_str[0].to_lowercase().as_str() {
                    "echo" => Command::Echo,
                    "ping" => Command::Ping,
                    "set" => Command::Set,
                    "get" => Command::Get,
                    "info" => Command::Info,
                    "replconf" => Command::Replconf,
                    "psync" => Command::Psync,
                    _ => Command::Unknown,
                };

                match command {
                    Command::Echo => {
                        let response: String = encode_resp_bulk_string(&decoded_str[1]);
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
                            let response = encode_resp_array(&["PONG"]);
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

                            let response = encode_simple_string("OK");
                            if let Err(e) = socket.write_all(response.as_bytes()).await {
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

                            let response: String = encode_resp_bulk_string(key);

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

                        let encoded_role: String =
                            encode_resp_bulk_string(format!("role:{}", role).as_str());
                        // TODO: Move this to master mod or initialize this when creating master
                        // instance
                        let encoded_master_replid = encode_resp_bulk_string(
                            format!("master_replid:{}", replication_id).as_str(),
                        );

                        let encoded_master_repl_offset: String =
                            encode_resp_bulk_string("master_repl_offset:0");

                        let response = format!(
                            "{}{}{}",
                            encoded_role, encoded_master_replid, encoded_master_repl_offset
                        );

                        let response: String = encode_resp_bulk_string(response.as_str());
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("INFO: Failed to write to client: {}", e);
                            break;
                        }
                    }
                    Command::Replconf => {
                        let response: String = encode_simple_string("OK");
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("REPLCONF: Failed to write to client: {}", e);
                            break;
                        }
                    }
                    Command::Psync => {
                        //TODO: Swap out '0' with offset
                        let response = encode_simple_string(
                            format!("FULLRESYNC {} 0", replication_id).as_str(),
                        );
                        if let Err(e) = socket.write_all(response.as_bytes()).await {
                            eprintln!("PSYNC: Failed to write to to client: {}", e);
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
    let mut ptr: usize = 0;
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

fn encode_simple_string(input: &str) -> String {
    let response = format!("+{}{}", input, String::from("\r\n"));
    response
}

fn encode_resp_bulk_string(input: &str) -> String {
    let length: String = input.len().to_string();
    let response = format!(
        "${}{}{}{}",
        length,
        String::from("\r\n"),
        input,
        String::from("\r\n")
    );
    response
}

fn encode_resp_array(input: &[&str]) -> String {
    let mut response = String::new();
    response.push_str(format!("*{}{}", input.len(), String::from("\r\n")).as_str());
    for el in input.iter() {
        response.push_str(encode_resp_bulk_string(el).as_str());
    }
    response
}
