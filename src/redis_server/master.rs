use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::redis_server::{
    connection_handler, encode_resp_bulk_string, encode_simple_string, handle_connection,
    hex_to_binary, replica,
};

use super::{timed_hashmap::TimedHashMap, ConnectionHandler};

pub async fn start_master(port: &str, replication_id: String) {
    let listener: TcpListener = TcpListener::bind(port).await.unwrap();
    println!("Master started on port: {}", port);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let handler = MasterConnectionHandler {
                    timed_hashmap: TimedHashMap::new(),
                    replication_id: replication_id.clone(),
                    replicas: Vec::new(),
                };
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, handler).await {
                        eprintln!("Error in master connection: {}", e);
                    }
                });
            }
            Err(_) => eprintln!("Failed to start master instance."),
        }
    }
}

// pub async fn start_master(port: &str, replication_id: String) {
//     let listener: TcpListener = TcpListener::bind(port).await.unwrap();
//     println!("Master started on port: {}", port);
//
//     loop {
//         match listener.accept().await {
//             Ok((socket, _)) => {
//                 let replication_id_clone = replication_id.clone();
//                 tokio::spawn(async move {
//                     handle_connection(socket, "master", &replication_id_clone).await;
//                 });
//             }
//             Err(_) => eprintln!("Failed to start master instance."),
//         }
//     }
// }

struct MasterConnectionHandler {
    timed_hashmap: TimedHashMap<String, String>,
    replication_id: String,
    replicas: Vec<String>,
}

impl ConnectionHandler for MasterConnectionHandler {
    async fn handle_echo(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let response: String = encode_resp_bulk_string(&decoded_str[1]);
        if stream.write_all(response.as_bytes()).await.is_ok() {
            Ok(())
        } else {
            println!("Master: unable to send response back in ECHO command.");
            Err("Master: unable to send successful ECHO response".into())
        }
    }

    async fn handle_ping(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Master: received PING.");
        let response = encode_simple_string("PONG");
        if stream.write_all(response.as_bytes()).await.is_ok() {
            println!("Master: sent PONG.");
            Ok(())
        } else {
            eprintln!("Master: failed to reply with PONG.");
            Err("Master: failed to send PONG response.".into())
        }
    }

    async fn handle_set(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match decoded_str.len() {
            3 => {
                println!("Inserting into key-value store with no expiry...");
                // TODO: handle situation with more than one replica
                let mut replica_stream = TcpStream::connect(self.replicas[0]).await.unwrap();
                self.timed_hashmap.insert(
                    decoded_str[1].to_owned(),
                    decoded_str[2].to_owned(),
                    None,
                );
                let response = encode_simple_string("OK");
                println!("Successfully inserted into key-value store with no expiry.");
                if stream.write_all(response.as_bytes()).await.is_ok() {
                    Ok(())
                } else {
                    Err("Master: unable to send OK response in SET command.".into())
                }
            }
            5 => {
                println!("Inserting into key-value store with expiry");
                // TODO: handle situation with more than one replica
                let mut replica_stream = TcpStream::connect(self.replicas[0]).await.unwrap();
                let milliseconds: u64 = decoded_str[4].to_owned().parse().unwrap();
                let ttl: Duration = Duration::from_millis(milliseconds);

                self.timed_hashmap.insert(
                    decoded_str[1].to_owned(),
                    decoded_str[2].to_owned(),
                    Some(ttl),
                );

                println!("Successfully inserted into key-value store with expiry.");
                if stream.write_all("+OK\r\n".as_bytes()).await.is_ok() {
                    Ok(())
                } else {
                    Err("Master: unable to send OK response in SET command".into())
                }
            }
            _ => Err("Unable to insert into key-value store.".into()),
        }
    }

    async fn handle_get(
        &mut self,
        decoded_str: &[String],
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Entering into GET command...");
        println!("Removing expired entries...");
        self.timed_hashmap.remove_expired_entries();

        if let Some(key) = self.timed_hashmap.get(&decoded_str[1]) {
            let response: String = encode_resp_bulk_string(key);
            if stream.write_all(response.as_bytes()).await.is_ok() {
                Ok(())
            } else {
                Err("Master: unable to send successful response in GET commmand.")
            }
        } else {
            match stream.write_all("$-1\r\n".as_bytes()).await {
                Ok(_) => Ok(()),
                Err(_) => Err("Master: null bulk string.".into()),
            }
        }
    }

    async fn handle_info(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Master: entering INFO command...");
        let encoded_role: String = encode_resp_bulk_string("master");
        let encoded_master_replid =
            encode_resp_bulk_string(format!("master_replid:{}", self.replication_id).as_str());
        // TODO: Modify to use dynamic offset values
        let encoded_master_repl_offset: String = encode_resp_bulk_string("master_repl_offset:0");
        let response: String = format!(
            "{}{}{}",
            encoded_role, encoded_master_replid, encoded_master_repl_offset
        );

        if stream
            .write_all(encode_resp_bulk_string(response.as_str()).as_bytes())
            .await
            .is_ok()
        {
            Ok(())
        } else {
            Err("Master: failed to send successful response in INFO command.".into())
        }
    }

    async fn handle_replconf(
        &mut self,
        stream: &mut TcpStream,
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Master: received REPLCONF...");
        let response: String = encode_simple_string("OK");
        if stream.write_all(response.as_bytes()).await.is_ok() {
            Ok(())
        } else {
            Err("Master: failed to send successful response in REPLCONF command.".into())
        }
    }

    async fn handle_psync(
        &mut self,
        stream: &mut TcpStream,
        replication_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // TODO: create method to keep track of replica address
        let peer_socket_address: SocketAddr = stream.peer_addr().unwrap();
        println!("Socket address of replica is: {}", peer_socket_address);
        // TODO: assign / add replica address to 'replicas'

        //TODO: Swap out '0' with offset
        let response = encode_simple_string(format!("FULLRESYNC {} 0", replication_id).as_str());
        if let Err(e) = stream.write_all(response.as_bytes()).await {
            eprintln!("PSYNC: Failed to write to to client: {}", e);
        }

        let hex_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        match hex_to_binary(hex_rdb) {
            Ok(binary) => {
                // binary now contains the RDB file contents as a Vec<u8>
                let length = binary.len();
                let response = format!("${}{}", length, String::from("\r\n"));
                let response_bytes = response.as_bytes();

                if stream.write(response_bytes).await.is_err() {
                    println!("Unable to write length prefix into buffer.");
                }
                if stream.write(binary.as_slice()).await.is_err() {
                    println!("Unable to write RDB data into buffer.");
                }
                if stream.flush().await.is_err() {
                    eprint!("Unable to flush.")
                };
                Ok(())
            }

            Err(err) => {
                eprintln!("Error parsing hexadecimal string: {}", err);
                Err("Master: error parsing hexadecimal string.".into())
            }
        }
    }
}
