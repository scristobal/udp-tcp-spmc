use clap::Parser;

use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter, Result};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::broadcast::{self, Sender};
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;

/// Simple TCP broadcaster, connects to a remote TCP host and broadcast to a local TCP socket
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Local host for clients to connect and get data pushed
    #[arg(short, long, default_value_t = String::from("localhost"), env = "HOST")]
    host: String,
    /// Local port for clients to connect and get data pushed
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

async fn stream_to_tx<'a>(stream: TcpStream, tx: Sender<Bytes>, cancel: CancellationToken) {
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
    let mut reader = BufReader::new(stream);

    loop {
        let read = reader.read_buf(&mut buffer);

        tokio::select! {
            _ = cancel.cancelled() => { break; }
            Ok(n) = read => {
                let data = buffer.split_to(n).freeze();
                match  tx.send(data) {
                    Ok(n) => {
                        // n is the number of subscribed receivers when it was send, there is no warranty they will see the message as they can be dropped or lag before
                        dbg!("sent to ", n);
                    }
                    Err(e) => {
                        // this happens when all receivers have been dropped
                        dbg!("failed to send", e);
                    }
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
                while let Ok(data) = rx.recv().await {
                    match stream.write_all(&data).await {
                        Ok(_) => {
                            continue;
                        }
                        Err(e) => {
                            // this error is the first error the io::Write found type io::Error
                            dbg!("failed to write", e);
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

#[tokio::main]
async fn main() -> Result<()> {
    let Args {
        port,
        host,
        remote_port,
        remote_host,
    } = Args::parse();

    dbg!("passed args", Args::parse());

    let cancel = CancellationToken::new();

    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    let remote = format!("{remote_host}:{remote_port}")
        .parse::<SocketAddr>()
        .unwrap();
    let remote = TcpStream::connect(remote).await.unwrap();

    tokio::spawn(stream_to_tx(remote, tx.clone(), cancel.clone()));

    let listener = format!("{host}:{port}").parse::<SocketAddr>().unwrap();
    let listener = TcpListener::bind(listener)
        .await
        .expect("Failed to bind TCP listener");

    tokio::spawn(tx_to_streams(listener, tx.clone(), cancel.clone()));

    tokio::signal::ctrl_c().await?;

    cancel.cancel();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;
    use tokio::sync::broadcast;
    use tokio_util::bytes::BufMut;

    #[tokio::test]
    async fn test_stream_to_tx() {
        let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);
        let cancel = CancellationToken::new();

        // create a tests server
        let listener = TcpListener::bind("127.0.0.1:8082").await.unwrap();

        let remote = TcpStream::connect("127.0.0.1:8082").await.unwrap();

        tokio::spawn(stream_to_tx(remote, tx.clone(), cancel.clone()));

        // create a test tx subscriber
        let mut rx = tx.subscribe();

        tokio::spawn(async move {
            let data = rx.recv().await.unwrap();

            assert_eq!(data, vec![1, 2, 3]);
        });

        // send data on listener and check rx receives it
        let data = vec![1, 2, 3];

        listener
            .accept()
            .await
            .unwrap()
            .0
            .write_all(&data)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_tx_to_streams() {
        let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);
        let cancel = CancellationToken::new();
        let listener = TcpListener::bind("127.0.0.1:8084").await.unwrap();

        tokio::spawn(tx_to_streams(listener, tx.clone(), cancel.clone()));

        // Connect to the listener
        let mut stream_1 = TcpStream::connect("127.0.0.1:8084").await.unwrap();
        let mut stream_2 = TcpStream::connect("127.0.0.1:8084").await.unwrap();

        // Send data to the broadcast channel
        let mut data = BytesMut::with_capacity(3);

        data.put_u8(1);
        data.put_u8(2);
        data.put_u8(3);

        let data = data.freeze();

        tokio::spawn({
            let data = data.clone();
            async move {
                // Read data from the stream
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = stream_1.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
            }
        });

        tokio::spawn({
            let data = data.clone();

            async move {
                // Read data from the stream
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = stream_2.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
            }
        });

        tx.send(data).unwrap();
    }
}
