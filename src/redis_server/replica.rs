use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use crate::redis_server::{connection_handler, handle_connection};

use super::{encode_resp_array, timed_hashmap::TimedHashMap, ConnectionHandler};

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

    let mut master_stream = TcpStream::connect(master_address).await.unwrap();
    send_handshake_to_master(&mut master_stream, address).await;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let handler = SlaveConnectionHandler {
                    timed_hashmap: TimedHashMap::new(),
                    replication_id: replication_id.clone(),
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
}

impl ConnectionHandler for SlaveConnectionHandler {}

async fn send_handshake_to_master(stream: &mut TcpStream, port: &str) {
    send_ping_to_master(stream).await;
    send_replconf_to_master(stream, port).await;
    send_psync_to_master(stream).await;
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
