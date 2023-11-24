use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Sender};

const BUFFER_SIZE: usize = 1024;

async fn pull_from_server(tx: Sender<Vec<u8>>) {
    let remote_host = std::env::var("REMOTE_HOST").expect("REMOTE_HOST is not set");
    let remote_port = std::env::var("REMOTE_PORT").expect("REMOTE_PORT is not set");

    let external_server_addr = format!("{remote_host}:{remote_port}")
        .parse::<SocketAddr>()
        .unwrap();

    let mut external_server = TcpStream::connect(external_server_addr).await.unwrap();

    let mut buffer = [0u8; BUFFER_SIZE];

    while let Ok(n) = external_server.read(&mut buffer).await {
        let data = buffer[..n].to_vec();
        tx.send(data).unwrap();
    }
}

async fn push_to_clients(tx: Sender<Vec<u8>>) {
    let host = std::env::var("HOST").expect("HOST is not set");
    let port = std::env::var("PORT").expect("PORT is not set");

    let listener_addr = format!("{host}:{port}").parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(listener_addr)
        .await
        .expect("Failed to bind TCP listener");

    while let Ok((mut stream, addr)) = listener.accept().await {
        dbg!(addr);

        tokio::spawn({
            let mut rx = tx.subscribe();
            async move {
                while let Ok(data) = rx.recv().await {
                    stream.write_all(&data).await.unwrap();
                }
            }
        });
    }
}

const MAX_CHANNEL_MESSAGES: usize = 10;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, _) = broadcast::channel::<Vec<u8>>(MAX_CHANNEL_MESSAGES);

    tokio::spawn(pull_from_server(tx.clone()));

    tokio::spawn(push_to_clients(tx));

    tokio::signal::ctrl_c().await?;

    Ok(())
}
