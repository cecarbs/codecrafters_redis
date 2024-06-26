pub mod master;
pub mod replica;
mod timed_hashmap;

use std::borrow::Cow;
use std::fmt::Display;
use std::net::SocketAddr;
// use std::fs;
// use std::io;
use std::num::ParseIntError;
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

pub trait ConnectionHandler {
    async fn handle_echo(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_ping(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_set(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_get(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_info(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_replconf(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn handle_psync(
        &mut self,
        stream: &mut TcpStream,
        replication_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

async fn handle_connection<H: ConnectionHandler>(
    mut stream: TcpStream,
    mut handler: H,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0; 1024];

    loop {
        match stream.read(&mut buf).await {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    println!("Connection closed by client.");
                    return Ok(());
                }

                let request: Cow<'_, str> = String::from_utf8_lossy(&buf[..bytes_read]);
                let decoded_str: Vec<String> = decode_resp_bulk_string(request.to_string())?;

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
                    Command::Echo => handler.handle_echo(&decoded_str, &mut stream)?,
                    Command::Ping => handler.handle_ping(&mut stream)?,
                    Command::Set => handler.handle_set(&decoded_str, &mut stream)?,
                    Command::Get => handler.handle_get(&decoded_str, &mut stream)?,
                    Command::Info => handler.handle_info(&mut stream)?,
                    Command::Replconf => handler.handle_replconf(&mut stream)?,
                    Command::Psync => handler.handle_psync(&mut stream, &handler.replication_id)?,
                    Command::Unknown => {
                        eprintln!("Failed to parse command: unknown command.");
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading from client: {}", e);
                return Err(e.into());
            }
        }
    }
}

// async fn handle_connection(mut stream: TcpStream, role: &str, replication_id: &String) {
//     let mut buf = [0; 1024];
//     let mut timed_hashmap: TimedHashMap<String, String> = TimedHashMap::new();
//
//     let mut replica_address = String::new();
//
//     loop {
//         match stream.read(&mut buf).await {
//             Ok(bytes_read) => {
//                 if bytes_read == 0 {
//                     println!("Connection closed by client.")
//                 }
//
//                 let request: Cow<'_, str> = String::from_utf8_lossy(&buf[..bytes_read]);
//                 let decoded_str: Vec<String> =
//                     decode_resp_bulk_string(request.to_string()).unwrap();
//                 // TODO: refactor this so that all you need to do is make changes to Enum
//                 let command: Command = match decoded_str[0].to_lowercase().as_str() {
//                     "echo" => Command::Echo,
//                     "ping" => Command::Ping,
//                     "set" => Command::Set,
//                     "get" => Command::Get,
//                     "info" => Command::Info,
//                     "replconf" => Command::Replconf,
//                     "psync" => Command::Psync,
//                     _ => Command::Unknown,
//                 };
//
//                 match command {
//                     Command::Echo => {
//                         let response: String = encode_resp_bulk_string(&decoded_str[1]);
//                         if let Err(e) = stream.write_all(response.as_bytes()).await {
//                             eprintln!("ECHO: Failed to write to client: {}", e);
//                             break;
//                         }
//                     }
//                     Command::Ping => {
//                         if role == "master" {
//                             println!("Master: received PING.");
//                             let response = encode_resp_bulk_string("PONG");
//                             if let Err(e) = stream.write_all(response.as_bytes()).await {
//                                 eprintln!("PING - Master: Failed to write to client: {}", e);
//                                 break;
//                             }
//                         } else if role == "slave" {
//                             println!("Slave: received PING.");
//                             let response = encode_resp_array(&["PONG"]);
//                             if let Err(e) = stream.write_all(response.as_bytes()).await {
//                                 eprintln!("PING - Slave: Failed to write to client: {}", e);
//                                 break;
//                             }
//                         } else {
//                             eprintln!("Could not determine role.");
//                             break;
//                         }
//                     }
//                     // TODO: use replica_address to propogate
//                     // TODO: clean up this method, don't use length, have separate logic for slave
//                     // and master; otherwise, even slave will propogate to itself
//                     Command::Set => match decoded_str.len() {
//                         // With expiration
//                         3 => {
//                             // TODO: refactor and separate master and slave commands
//                             let mut replica_stream =
//                                 TcpStream::connect(replica_address.clone()).await.unwrap();
//                             timed_hashmap.insert(
//                                 decoded_str[1].to_owned(),
//                                 decoded_str[2].to_owned(),
//                                 None,
//                             );
//
//                             println!("Inserted into hashmap: {:?} with no expiry.", timed_hashmap);
//
//                             let response = encode_simple_string("OK");
//                             if let Err(e) = stream.write_all(response.as_bytes()).await {
//                                 eprintln!("SET: Failed to write to client: {}", e);
//                                 break;
//                             }
//                         }
//                         // With expiration
//                         5 => {
//                             // TODO: refactor and separate master and slave commands
//                             // length of 5 means there's an expiration
//                             let mut replica_stream =
//                                 TcpStream::connect(replica_address.clone()).await.unwrap();
//                             // TODO: only works for 'px' (millisecomnds); 'px' means seconds
//                             let milliseconds: u64 = decoded_str[4].to_owned().parse().unwrap();
//                             let ttl: Duration = Duration::from_millis(milliseconds);
//
//                             timed_hashmap.insert(
//                                 decoded_str[1].to_owned(),
//                                 decoded_str[2].to_owned(),
//                                 Some(ttl),
//                             );
//
//                             println!("Inserted into hashmap: {:?} with expiry.", timed_hashmap);
//
//                             if let Err(e) = stream.write_all("+OK\r\n".as_bytes()).await {
//                                 eprintln!("SET: Failed to write to client: {}", e);
//                                 break;
//                             }
//                         }
//                         _ => {
//                             println!("Unable to insert into either hashmaps.");
//                         }
//                     },
//                     Command::Get => {
//                         println!("Entering get command.");
//                         timed_hashmap.remove_expired_entries();
//
//                         if let Some(key) = timed_hashmap.get(&decoded_str[1]) {
//                             println!("Hashmap: {:?}", timed_hashmap);
//
//                             let response: String = encode_resp_bulk_string(key);
//
//                             println!("Attempting to send response: {:?}", response);
//
//                             if let Err(e) = stream.write_all(response.as_bytes()).await {
//                                 eprintln!("GET: Failed to write to client: {}", e);
//                                 break;
//                             }
//                         } else {
//                             match stream.write_all("$-1\r\n".as_bytes()).await {
//                                 Ok(_) => (),
//                                 Err(_) => {
//                                     eprintln!("Null bulk string.");
//                                     break;
//                                 }
//                             }
//                         }
//                     }
//                     // Used by both master and slave
//                     Command::Info => {
//                         println!("Entering info command.");
//
//                         let encoded_role: String =
//                             encode_resp_bulk_string(format!("role:{}", role).as_str());
//                         // TODO: Move this to master mod or initialize this when creating master
//                         // instance
//                         let encoded_master_replid = encode_resp_bulk_string(
//                             format!("master_replid:{}", replication_id).as_str(),
//                         );
//
//                         let encoded_master_repl_offset: String =
//                             encode_resp_bulk_string("master_repl_offset:0");
//
//                         let response = format!(
//                             "{}{}{}",
//                             encoded_role, encoded_master_replid, encoded_master_repl_offset
//                         );
//
//                         let response: String = encode_resp_bulk_string(response.as_str());
//                         if let Err(e) = stream.write_all(response.as_bytes()).await {
//                             eprintln!("INFO: Failed to write to client: {}", e);
//                             break;
//                         }
//                     }
//                     // Used by master
//                     Command::Replconf => {
//                         println!("Master received REPLCONF...");
//                         let response: String = encode_simple_string("OK");
//                         if let Err(e) = stream.write_all(response.as_bytes()).await {
//                             eprintln!("REPLCONF: Failed to write to client: {}", e);
//                             break;
//                         }
//                     }
//                     // Used by master
//                     Command::Psync => {
//                         // TODO: create method to keep track of replica address
//                         let peer_socket_address: SocketAddr = stream.peer_addr().unwrap();
//                         println!("Socket address of replica is: {}", peer_socket_address);
//
//                         //TODO: Swap out '0' with offset
//                         let response = encode_simple_string(
//                             format!("FULLRESYNC {} 0", replication_id).as_str(),
//                         );
//                         if let Err(e) = stream.write_all(response.as_bytes()).await {
//                             eprintln!("PSYNC: Failed to write to to client: {}", e);
//                             break;
//                         }
//
//                         let hex_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
//                         match hex_to_binary(hex_rdb) {
//                             Ok(binary) => {
//                                 // binary now contains the RDB file contents as a Vec<u8>
//                                 let length = binary.len();
//                                 let response = format!("${}{}", length, String::from("\r\n"));
//                                 let response_bytes = response.as_bytes();
//
//                                 if stream.write(response_bytes).await.is_err() {
//                                     println!("Unable to write length prefix into buffer.");
//                                 }
//                                 if stream.write(binary.as_slice()).await.is_err() {
//                                     println!("Unable to write RDB data into buffer.");
//                                 }
//                                 if stream.flush().await.is_err() {
//                                     eprint!("Unable to flush.")
//                                 };
//                             }
//
//                             Err(err) => {
//                                 eprintln!("Error parsing hexadecimal string: {}", err);
//                             }
//                         }
//                     }
//                     Command::Unknown => {
//                         eprintln!("Failed to parse command: unknown command.");
//                         break;
//                     }
//                 }
//             }
//             Err(e) => {
//                 eprintln!("Error reading from client: {}", e);
//                 break;
//             }
//         }
//     }
// }
//
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

pub fn encode_resp_array(input: &[&str]) -> String {
    let mut response = String::new();
    response.push_str(format!("*{}{}", input.len(), String::from("\r\n")).as_str());
    for el in input.iter() {
        response.push_str(encode_resp_bulk_string(el).as_str());
    }
    response
}

fn hex_to_binary(hex_str: &str) -> Result<Vec<u8>, ParseIntError> {
    // since each byte is is represented by two hexadecimal characters
    let mut binary: Vec<u8> = Vec::with_capacity(hex_str.len() / 2);

    // iterate over hexadecimal string two chars at a time
    for i in (0..hex_str.len()).step_by(2) {
        // parse each pair of hexadecimal char as u8
        let byte = u8::from_str_radix(&hex_str[i..i + 2], 16)?;
        binary.push(byte);
    }

    Ok(binary)
}

// fn send_rdb_file(file: Vec<u8>) {
//     let length = file.len();
//     let response = format!("${}{}", length, String::from("\r\n"));
//
//     if let Err(e) = stream.write
// }
