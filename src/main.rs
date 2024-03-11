use std::{env, error::Error};

use redis_starter_rust::handle_connection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();

    let mut port: String = String::from("127.0.0.1:");
    if args.len() > 2 && args[1] == "--port" {
        port.push_str(args[1].as_str());
    } else {
        port.push_str("6379");
    }

    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind(port).await?;

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                println!("Established connection with client.");

                tokio::spawn(handle_connection(socket));
            }
            Err(e) => println!("Failed to establish connection with client: {:?}", e),
        }
    }
}
