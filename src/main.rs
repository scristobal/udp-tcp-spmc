use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Receiver, Sender};

async fn broadcast_data(mut rx: Receiver<Vec<u8>>, tx: Arc<Mutex<Vec<Sender<Vec<u8>>>>>) {
    loop {
        let data = rx.recv().await.unwrap();
        let tx = tx.lock().unwrap();
        for tx in tx.iter() {
            tx.send(data.clone()).unwrap();
        }
    }
}

async fn handle_client(connection: TcpStream, mut rx: Receiver<Vec<u8>>) {
    let (_, mut writer) = connection.into_split();

    loop {
        let data = rx.recv().await.unwrap();
        writer.write_all(&data).await.unwrap();
    }
}

const BUFFER_SIZE: usize = 1024;

async fn push_server_data(tx: Sender<Vec<u8>>) {
    let remote_host = std::env::var("REMOTE_HOST").expect("REMOTE_HOST is not set");
    let remote_port = std::env::var("REMOTE_PORT").expect("REMOTE_PORT is not set");

    let external_server_addr: SocketAddr = format!("{remote_host}:{remote_port}").parse().unwrap();
    let mut external_server = TcpStream::connect(external_server_addr).await.unwrap();

    loop {
        let mut buffer = [0u8; BUFFER_SIZE];
        let n = external_server.read(&mut buffer).await.unwrap();

        let data = buffer[..n].to_vec();

        tx.send(data).unwrap();
    }
}

async fn enlist(
    listener: TcpListener,
    tx: Sender<Vec<u8>>,
    clients: Arc<Mutex<Vec<Sender<Vec<u8>>>>>,
) {
    loop {
        let (connection, _) = listener
            .accept()
            .await
            .expect("Failed to accept incoming connection");

        let tx = tx.clone();

        let clients = Arc::clone(&clients);
        let mut clients = clients.lock().unwrap();

        clients.push(tx.clone());

        tokio::spawn(handle_client(connection, tx.subscribe()));
    }
}

const MAX_CHANNEL_MESSAGES: usize = 10;

#[tokio::main]
async fn main() {
    let host = std::env::var("HOST").expect("HOST is not set");
    let port = std::env::var("PORT").expect("PORT is not set");

    let listener_addr: SocketAddr = format!("{host}:{port}").parse().unwrap();

    let (tx, _) = broadcast::channel::<Vec<u8>>(MAX_CHANNEL_MESSAGES);

    tokio::spawn(push_server_data(tx.clone()));

    let listener = TcpListener::bind(listener_addr)
        .await
        .expect("Failed to bind TCP listener");

    let clients = Arc::new(Mutex::new(Vec::new()));

    tokio::spawn(broadcast_data(tx.subscribe(), Arc::clone(&clients)));

    tokio::spawn(enlist(listener, tx, clients));

    tokio::signal::ctrl_c()
        .await
        .expect("Failed to install CTRL+C signal handler");
}
