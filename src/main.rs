mod helper_cli;
use std::{env, error::Error};

use redis_starter_rust::handle_connection;
use tokio::net::TcpListener;

use crate::helper_cli::HelperCLI;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let cli_args = HelperCLI::new(args);
    let role = cli_args.role;
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind(&cli_args.port).await?;

    println!("Server started on port: {}", cli_args.port);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                let role = match &role {
                    helper_cli::Role::Master(master_role) => master_role,
                    helper_cli::Role::Slave(slave_role) => slave_role,
                };

                println!("Role is: {}", role);
                println!("Established connection with client.");

                tokio::spawn(handle_connection(socket, role.to_owned()));
            }
            Err(e) => println!("Failed to establish connection with client: {:?}", e),
        }
    }
}
