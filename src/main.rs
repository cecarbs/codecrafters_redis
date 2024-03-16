mod cli;
mod redis_server;
use std::{env, error::Error};

use cli::{Role, CLI};
use redis_server::{start_master, start_replica};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli_args = CLI::new(env::args().collect());
    let role: Role = cli_args.role.clone();

    if role.to_string() == "master" {
        start_master(&cli_args.port).await;
    } else {
        start_replica(&cli_args.master_server, &cli_args.port).await;
    }

    Ok(())
}
