use std::time::Duration;

use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
};

use crate::redis_server::{
    connection_handler, encode_resp_bulk_string, encode_simple_string, handle_connection, replica,
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
            Err("hello".into())
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
        todo!()
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
