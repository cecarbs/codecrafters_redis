use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::redis_server::handle_connection;

use super::encode_resp_array;

pub async fn start_replica(master_address: &str, address: &str, replication_id: String) {
    let listener = TcpListener::bind(address).await.unwrap();
    println!("Replica started on port: {}", address);

    let mut master_stream = TcpStream::connect(master_address).await.unwrap();
    send_handshake_to_master(&mut master_stream, address).await;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let replication_id_clone = replication_id.clone();
                tokio::spawn(async move {
                    handle_connection(socket, "slave", &replication_id_clone).await;
                });
            }
            Err(_) => eprintln!("Failed to start replica instance."),
        }
    }
}

async fn send_handshake_to_master(stream: &mut TcpStream, port: &str) {
    send_ping_to_master(stream).await;
    send_replconf_to_master(stream, port).await;
    send_psync_to_master(stream).await;
}

async fn send_ping_to_master(stream: &mut TcpStream) {
    let ping = encode_resp_array(&["PING"]);
    if let Err(e) = stream.write_all(ping.as_bytes()).await {
        eprintln!("Failed to send Ping to master with error: {:?}", e);
    }
}

// Notifies master what port replica is lstening on & notifying the master of its capabilities
async fn send_replconf_to_master(stream: &mut TcpStream, port: &str) {
    let replconf_listening_port = encode_resp_array(&[
        "REPLCONF",
        "listening-port",
        get_port_from_address(port).as_str(),
    ]);
    if let Err(e) = stream.write_all(replconf_listening_port.as_bytes()).await {
        eprintln!(
            "Failed to send replconf_listening_port to master with error: {}",
            e
        );
    }
    let replconf_capabilities = encode_resp_array(&["REPLCONF", "capa", "psync2"]);
    if let Err(e) = stream.write_all(replconf_capabilities.as_bytes()).await {
        eprintln!(
            "Failed to send replconf_capabilities to master with error: {}",
            e
        );
    }
}

// Used to synchronize with the state of the replica with master
async fn send_psync_to_master(stream: &mut TcpStream) {
    // First command (after PSYNC) should be ID of master or ? if it is first time connecting
    // Second command is the offset of the master or -1 if it is first time connecting to master
    let replconf_psync = encode_resp_array(&["PSYNC", "?", "-1"]);
    if let Err(e) = stream.write_all(replconf_psync.as_bytes()).await {
        eprintln!("Failed to send replconf_psync to master with error: {}", e);
    }
}

fn get_port_from_address(address: &str) -> String {
    let colon_idx = address.find(':').unwrap();
    let port: String = address[colon_idx + 1..].to_string();
    port
}
