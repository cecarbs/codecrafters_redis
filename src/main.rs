mod cli;
mod redis_server;
use std::{env, error::Error};

use cli::{Cli, Role};
use redis_server::{master, replica};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli_args = Cli::new(env::args().collect());
    let role: Role = cli_args.role.clone();

    if role.to_string() == "master" {
        master::start_master(&cli_args.port).await;
    } else {
        replica::start_replica(&cli_args.master_server, &cli_args.port).await;
    }

    Ok(())
}
