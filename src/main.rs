use clap::Parser;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, Result};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_util::bytes::BytesMut;

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

async fn run(stream: TcpStream, listener: TcpListener, cancel: CancellationToken) -> Result<()> {
    let sinks: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(vec![]));

    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
    let mut reader = BufReader::new(stream);

    loop {
        let read = reader.read_buf(&mut buffer);

        tokio::select! {
            _ =  cancel.cancelled() => {
                break;
            }
            Ok(n) = read => {

                let mut sinks = sinks.lock().await;
                let mut failed_sink_indexes = vec![];

                let read = buffer.split_to(n);

                for (index, sink) in  sinks.iter_mut().enumerate() {

                    let addr = sink.peer_addr().unwrap();

                    let mut data = read.clone().freeze();

                    let mut writer = BufWriter::new(sink);

                    let mut m = 0;

                    while m < n {

                        m += match writer.write_buf(&mut data).await {
                            Err(e) => {
                                error!("Error writing to {}: {}", addr,  e);
                                failed_sink_indexes.push(index);
                                break;
                            },
                            Ok(0) => {
                                info!("Could not write to {}, disconnected", addr);
                                // Ok(0) means nothing was be written, so most likely the client socket disconnected and needs to be removed
                                failed_sink_indexes.push(index);
                                break;
                            },
                            Ok(r) => {
                                println!("read something {}", &r);
                                r
                            },
                        }
                    }

                }

                for index in failed_sink_indexes {
                    sinks.remove(index);
                }
            }
            Ok(( stream,_)) = listener.accept() => {
                let mut sinks = sinks.lock().await;
                sinks.push(stream);
            }
        }
    }

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
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn run_test() {
        // create a tests server

        let cancel = CancellationToken::new();

        let listener_addr = "127.0.0.1:8081";
        let stream_addr = "127.0.0.1:9091";

        let stream_listener = TcpListener::bind(&stream_addr).await.unwrap();
        let listener = TcpListener::bind(&listener_addr).await.unwrap();

        let data = vec![1, 2, 3];

        tokio::spawn({
            let test_data = data.clone();

            async move {
                let mut remote_stream = stream_listener.accept().await.unwrap().0;

                println!("after connect");

                let r = remote_stream.write(&test_data).await.unwrap();

                println!("after sending");

                assert_eq!(r, test_data.len());
            }
        });

        sleep(Duration::from_secs(2));

        let stream = TcpStream::connect(&stream_addr).await.unwrap();

        tokio::spawn(run(stream, listener, cancel.clone()));

        sleep(Duration::from_secs(2));

        let handle_1 = tokio::spawn({
            let data = data.clone();

            let mut sink = TcpStream::connect(&listener_addr).await.unwrap();

            async move {
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = sink.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
                assert_eq!(buffer[..n].to_vec(), vec![1, 2]);
            }
        });

        let handle_2 = tokio::spawn({
            let data = data.clone();

            let mut sink = TcpStream::connect(&listener_addr).await.unwrap();

            async move {
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = sink.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data);
            }
        });

        handle_1.await.unwrap();
        handle_2.await.unwrap();

        // let (r, s) = join!(handle_1, handle_2);

        cancel.cancel()
    }
}
