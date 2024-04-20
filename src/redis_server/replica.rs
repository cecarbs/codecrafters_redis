use std::time::Duration;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

use crate::redis_server::{encode_simple_string, handle_connection};

use super::{
    encode_resp_array, encode_resp_bulk_string, timed_hashmap::TimedHashMap, ConnectionHandler,
};

// pub async fn start_replica(master_address: &str, address: &str, replication_id: String) {
//     let listener = TcpListener::bind(address).await.unwrap();
//     println!("Replica started on port: {}", address);
//
//     let mut master_stream = TcpStream::connect(master_address).await.unwrap();
//     send_handshake_to_master(&mut master_stream, address).await;
//
//     loop {
//         match listener.accept().await {
//             Ok((socket, _)) => {
//                 let replication_id_clone = replication_id.clone();
//                 tokio::spawn(async move {
//                     handle_connection(socket, "slave", &replication_id_clone).await;
//                 });
//             }
//             Err(_) => eprintln!("Failed to start replica instance."),
//         }
//     }
// }
pub async fn start_replica(master_address: &str, address: &str, replication_id: String) {
    let listener = TcpListener::bind(address).await.unwrap();
    println!("Replica started on port: {}", address);

    let mut master_stream: TcpStream = TcpStream::connect(master_address).await.unwrap();
    let mut shared_master_stream = send_handshake_to_master(&mut master_stream, address).await;
    let shared_master_stream = RwLock::new(master_stream);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let replication_id: String = replication_id.clone();
                let handler = SlaveConnectionHandler {
                    timed_hashmap: TimedHashMap::new(),
                    replication_id: replication_id.clone(),
                    master_address: master_address.to_string().clone(),
                };
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(socket, handler).await {
                        eprintln!("Error in replica connection: {}", e);
                    }
                });
            }
            Err(_) => eprintln!("Failed to start replica instance."),
        }
    }
}

struct SlaveConnectionHandler {
    timed_hashmap: TimedHashMap<String, String>,
    replication_id: String,
    master_address: String,
}

impl ConnectionHandler for SlaveConnectionHandler {
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
        println!("Replica: received PING.");
        let response = encode_resp_array(&["PONG"]);
        if stream.write_all(response.as_bytes()).await.is_ok() {
            println!("Replica: sent PONG.");
            Ok(())
        } else {
            eprintln!("Replica: failed to reply with PONG.");
            Err("Replica: failed to send PONG response.".into())
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
                let mut replica_stream = TcpStream::connect(&self.master_address).await.unwrap();
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
                    Err("Replica: unable to send OK response in SET command.".into())
                }
            }
            5 => {
                println!("Inserting into key-value store with expiry");
                // TODO: use same stream as handshake
                let mut replica_stream = TcpStream::connect(&self.master_address).await.unwrap();
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
                    Err("Replica: unable to send OK response in SET command".into())
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
                Err("Replica: unable to send successful response in GET commmand.".into())
            }
        } else {
            match stream.write_all("$-1\r\n".as_bytes()).await {
                Ok(_) => Ok(()),
                Err(_) => Err("Replica: null bulk string.".into()),
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
        todo!()
    }

    async fn handle_psync(
        &mut self,
        stream: &mut TcpStream,
        replication_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!()
    }
}

async fn send_handshake_to_master<'a>(stream: &'a mut TcpStream, port: &str) -> &'a mut TcpStream {
    send_ping_to_master(stream).await;
    send_replconf_to_master(stream, port).await;
    send_psync_to_master(stream).await;
    stream
}

async fn send_ping_to_master(stream: &mut TcpStream) {
    println!("Sending PING to master...");
    let ping = encode_resp_array(&["PING"]);
    if let Err(e) = stream.write_all(ping.as_bytes()).await {
        eprintln!("Failed to send Ping to master with error: {:?}", e);
    }

    let ping_response = read_from_stream(stream).await;
    println!("PING response: {}", ping_response);
}

// Notifies master what port replica is lstening on & notifying the master of its capabilities
async fn send_replconf_to_master(stream: &mut TcpStream, port: &str) {
    println!("Sending REPLCONF with port information to master...");
    let replconf_listening_port = encode_resp_array(&[
        "REPLCONF",
        "listening-port",
        &get_port_from_address(port).as_str(),
    ]);

    if let Err(e) = stream.write_all(replconf_listening_port.as_bytes()).await {
        eprintln!(
            "Failed to send replconf_listening_port to master with error: {}",
            e
        )
    }

    let replconf_listening_port_response = read_from_stream(stream).await;
    println!(
        "REPLCONF port information response: {}",
        replconf_listening_port_response
    );

    println!("Sending REPLCONF with capabilities to master...");
    let replconf_capabilities = encode_resp_array(&["REPLCONF", "capa", "psync2"]);
    if let Err(e) = stream.write_all(replconf_capabilities.as_bytes()).await {
        eprintln!(
            "Failed to send replconf_capabilities to master with error: {}",
            e
        );
    }

    let replconf_capabilities_response = read_from_stream(stream).await;
    println!(
        "REPLCONF capabilities response: {}",
        replconf_capabilities_response
    );
}

// Used to synchronize with the state of the replica with master
async fn send_psync_to_master(stream: &mut TcpStream) {
    // First command (after PSYNC) should be ID of master or ? if it is first time connecting
    // Second command is the offset of the master or -1 if it is first time connecting to master
    println!("Sending PSYNC to master...");
    let replconf_psync = encode_resp_array(&["PSYNC", "?", "-1"]);
    if let Err(e) = stream.write_all(replconf_psync.as_bytes()).await {
        eprintln!("Failed to send replconf_psync to master with error: {}", e);
    }

    let psync_response = read_from_stream(stream).await;
    println!("PSYNC response: {}", psync_response);
}

// TODO: Remove (doesn't work)
#[allow(dead_code)]
async fn read_response_from_master(
    stream: &mut TcpStream,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut buf = Vec::new();
    // will hang, likely due to waiting for entire response to be available
    stream.read_to_end(&mut buf).await?;

    let response = String::from_utf8_lossy(&buf);
    println!("Received response from Master: {}", response);

    Ok(response.to_string())
}

async fn read_from_stream(stream: &mut TcpStream) -> String {
    let mut buf = [0; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let received = String::from_utf8_lossy(&buf[..n]).to_string();
    println!("Received: {}", received);
    received
}

fn get_port_from_address(address: &str) -> String {
    let colon_idx = address.find(':').unwrap();
    let port: String = address[colon_idx + 1..].to_string();
    port
}
