use clap::Parser;
use tokio::sync::broadcast::{self, Sender};

use tokio_util::sync::CancellationToken;
use tracing::{error, warn, Level};
use tracing_subscriber::FmtSubscriber;

use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, Result};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::bytes::{Bytes, BytesMut};

/// Simple TCP broadcaster, connects to a remote TCP host and broadcast to a local TCP socket
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Local host for clients to connect and get data pushed
    #[arg(short = 'i', long, default_value_t = String::from("localhost"), env = "HOST")]
    local_host: String,
    /// Local port for clients to connect and get data pushed
    #[arg(short = 'p', long, default_value_t = 8080u16, env = "PORT")]
    local_port: u16,

    /// Remote host to pull data from
    #[arg(short = 'j', long, env = "REMOTE_HOST")]
    remote_host: String,

    /// Remote port to pull data from
    #[arg(short = 'q', long, env = "REMOTE_PORT")]
    remote_port: u16,
}

const BUFFER_SIZE: usize = 1024;

async fn stream_to_tx<'a>(stream: TcpStream, tx: Sender<Bytes>, cancel: CancellationToken) {
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
    let mut reader = BufReader::new(stream);

    loop {
        let read = reader.read_buf(&mut buffer);

        tokio::select! {
            _ = cancel.cancelled() => { break; }
            Ok(n) = read => {
                if n==0 {continue;}
                let data = buffer.split_to(n).freeze();
                if let Err(e) =  tx.send(data) {
                    error!("when sending data to the channel {}",e);
                }
            }
        }
    }
}

async fn tx_to_streams(listener: TcpListener, tx: Sender<Bytes>, cancel: CancellationToken) {
    let handle_listener = |mut stream: TcpStream| {
        tokio::spawn({
            let mut rx = tx.subscribe();

            async move {
                while let Ok(mut data) = rx.recv().await {
                    match stream.write_buf(&mut data).await {
                        Ok(n) => {
                            warn!("we wrote n bytes: {}", n)
                        }
                        Err(e) => {
                            error!("when writing buffer to the stream: {}", e)
                        }
                    };
                }
            }
        });
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {break}
            Ok(( stream,_)) = listener.accept() => handle_listener(stream)
        }
    }
}

const MAX_CHANNEL_MESSAGES: usize = 10;

async fn run(stream: TcpStream, listener: TcpListener, cancel: CancellationToken) -> Result<()> {
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    tokio::spawn(stream_to_tx(stream, tx.clone(), cancel.clone()));
    tokio::spawn(tx_to_streams(listener, tx.clone(), cancel.clone()));

    tokio::signal::ctrl_c().await?;

    cancel.cancel();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let Args {
        local_port,
        local_host,
        remote_port,
        remote_host,
    } = Args::parse();

    println!("running with passed args {:?}", Args::parse());

    let stream = format!("{remote_host}:{remote_port}")
        .parse::<SocketAddr>()
        .expect("Failed to parse remote host address");

    let stream = TcpStream::connect(stream)
        .await
        .expect("Failed to connect to remote host");

    let listener = format!("{local_host}:{local_port}")
        .parse::<SocketAddr>()
        .expect("Failed to parse local host address");

    let listener = TcpListener::bind(listener)
        .await
        .expect("Failed to bind TCP listener");

    let cancel = CancellationToken::new();

    tokio::spawn(run(stream, listener, cancel.clone()));

    tokio::signal::ctrl_c().await?;

    cancel.cancel();

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::{thread::sleep, time::Duration};

    use super::*;
    use tokio::{join, net::TcpStream};

    #[test_log::test(tokio::test)]
    async fn run_test() {
        let cancel = CancellationToken::new();

        let listener_addr = "127.0.0.1:8081";
        let stream_addr = "127.0.0.1:9091";

        let stream_listener = TcpListener::bind(&stream_addr).await.unwrap();
        let listener = TcpListener::bind(&listener_addr).await.unwrap();

        let data = b"test data";

        tokio::spawn({
            async move {
                let mut remote_stream = stream_listener.accept().await.unwrap().0;
                remote_stream.write_all(data).await.unwrap();
            }
        });

        // TODO: wait for `remote_stream` properly
        sleep(Duration::from_secs(2));

        let stream = TcpStream::connect(&stream_addr).await.unwrap();

        tokio::spawn(run(stream, listener, cancel.clone()));

        // TODO: wait for `run` to wire everything up properly
        sleep(Duration::from_secs(2));

        let handle_1 = tokio::spawn({
            let mut sink = TcpStream::connect(&listener_addr).await.unwrap();

            async move {
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = sink.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
                assert!(buffer[..n].to_vec() != b"wrong test data");
            }
        });

        let handle_2 = tokio::spawn({
            let mut sink = TcpStream::connect(&listener_addr).await.unwrap();

            async move {
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = sink.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
            }
        });

        let (r, s) = join!(handle_1, handle_2);

        r.unwrap();
        s.unwrap();

        cancel.cancel()
    }
}
