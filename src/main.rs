mod cli;
mod redis_server;
use std::{env, error::Error};

use cli::{Cli, Role};
use redis_server::{master, replica};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli_args = Cli::new(env::args().collect());
    let role: Role = cli_args.role.clone();
    let replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
    // TODO: create a struct for master configurations (offset and replication id)
    // TODO: create a struct for slave configurations

    if role.to_string() == "master" {
        master::start_master(&cli_args.port, replication_id).await;
    } else {
        replica::start_replica(&cli_args.master_server, &cli_args.port, replication_id).await;
    }

    Ok(())
}
