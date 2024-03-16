use tokio::net::TcpListener;

use crate::redis_server::handle_connection;

pub async fn start_master(port: &str) {
    let listener: TcpListener = TcpListener::bind(port).await.unwrap();
    println!("Master started on port: {}", port);

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    handle_connection(socket, "master").await;
                });
            }
            Err(_) => eprintln!("Failed to start master instance."),
        }
    }
}

async fn handle_client() {
    todo!("placeholder");
}
