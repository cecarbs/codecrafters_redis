mod cli;
use std::{env, error::Error};

use cli::CommandLineArgs;
use redis_starter_rust::handle_connection;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let cli_args: cli::CommandLineArgs = cli::CommandLineArgs::new(args);

    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind(&cli_args.port).await?;

    println!("Server started on port: {}", cli_args.port);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                println!("Established connection with client.");

                tokio::spawn(handle_connection(socket, cli_args));
            }
            Err(e) => println!("Failed to establish connection with client: {:?}", e),
        }
    }
}
