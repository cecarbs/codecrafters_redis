use tokio::net::{TcpListener, TcpStream};

use crate::redis_server::{connection_handler, handle_connection};

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
} // pub async fn start_master(port: &str, replication_id: String) {
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

// async fn handle_client() {
//     todo!("placeholder");
// }
//
async fn master_connection_handler(mut stream: TcpStream, role: &str, replication_id: &String) {
    todo!("implement methods ")
}

struct MasterConnectionHandler {
    timed_hashmap: TimedHashMap<String, String>,
    replication_id: String,
}

impl ConnectionHandler for MasterConnectionHandler {
    // fn handle_echo(
    //     &mut self,
    //     decoded_str: &[String],
    //     stream: &mut TcpStream,
    // ) -> Result<(), Box<dyn std::error::Error>> {
    //     // Master-specific echo implementation
    // }

    // Implement other trait methods for the master
}

// impl Master {
//     todo!("impl");
//     pub async fn
// }
