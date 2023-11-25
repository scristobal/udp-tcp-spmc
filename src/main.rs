use clap::Parser;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Sender};
use tokio_util::sync::CancellationToken;

/// Simple TCP broadcaster
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Host for clients to connect and get data pushed
    #[arg(short, long, default_value_t = String::from("localhost"), env = "HOST")]
    host: String,
    /// Port for clients to connect and get data pushed
    #[arg(short, long, default_value_t = 8080u16, env = "PORT")]
    port: u16,

    /// Remote host to pull data from
    #[arg(short, long, env = "REMOTE_HOST")]
    remote_host: String,

    /// Remote port to pull data from
    #[arg(short, long, env = "REMOTE_PORT")]
    remote_port: u16,
}

const BUFFER_SIZE: usize = 1024;

async fn pull_from_server(host: String, port: u16, tx: Sender<Vec<u8>>, cancel: CancellationToken) {
    let external_server_addr = format!("{host}:{port}").parse::<SocketAddr>().unwrap();

    let mut external_server = TcpStream::connect(external_server_addr).await.unwrap();

    let mut buffer = [0u8; BUFFER_SIZE];

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {break}
            r =  external_server.read(&mut buffer) => match r {
                Err(e)  => {
                    dbg!("error", e);
                },
                Ok(n) => {
                    let data = buffer[..n].to_vec();
                    tx.send(data).unwrap();
                }
            }
        }
    }
}

async fn push_to_clients(host: String, port: u16, tx: Sender<Vec<u8>>, _cancel: CancellationToken) {
    let listener_addr = format!("{host}:{port}").parse::<SocketAddr>().unwrap();

    let listener = TcpListener::bind(listener_addr)
        .await
        .expect("Failed to bind TCP listener");

    loop {
        tokio::select! {
            _ = _cancel.cancelled() => {break}
            _ =  accept_clients(&listener, &tx) => {}
        }
    }
}

async fn accept_clients(listener: &TcpListener, tx: &Sender<Vec<u8>>) {
    while let Ok((mut stream, addr)) = listener.accept().await {
        dbg!("new connection", addr);

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
    let Args {
        port,
        host,
        remote_port,
        remote_host,
    } = Args::parse();
    dbg!("passed args", Args::parse());

    let cancel = CancellationToken::new();

    let (tx, _) = broadcast::channel::<Vec<u8>>(MAX_CHANNEL_MESSAGES);

    tokio::spawn(pull_from_server(
        remote_host,
        remote_port,
        tx.clone(),
        cancel.clone(),
    ));

    tokio::spawn(push_to_clients(host, port, tx.clone(), cancel.clone()));

    tokio::signal::ctrl_c().await?;

    cancel.cancel();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn test_pull_from_server() {}
}
