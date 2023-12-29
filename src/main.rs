#![feature(async_closure)]

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, Result};
use tokio::net::TcpListener;
use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::{self, Sender};
use tokio::{net::TcpStream, try_join};
use tokio_util::bytes::{Bytes, BytesMut};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Simple TCP broadcaster, connects to a remote TCP host and broadcast to a local TCP socket
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Local host:port for clients to connect and get data pushed
    #[arg(short = 'l', long)]
    local: String,

    /// Remote host:port to pull data from
    #[arg(short = 'r', long)]
    remote: String,
}

const BUFFER_SIZE: usize = 3;

/// Continuously reads data from a TCP stream and sends it to a channel of bytes.
async fn stream_to_tx<'a>(stream: TcpStream, tx: Sender<Bytes>, cancel: CancellationToken) {
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);
    let mut reader = BufReader::new(stream);

    loop {
        let read = reader.read_buf(&mut buffer);

        tokio::select! {
            _ = cancel.cancelled() => { break; }
            Ok(n) = read => {
                if n==0 {
                    continue;
                }

                let data = buffer.split_to(n).freeze();

                match tx.send(data) {
                    Ok(r) => info!("send {n} bytes to {r} receivers"),
                    Err(SendError(b)) => warn!("no listeners subscribed when received {} bytes to the channel", b.len())
                }
            }
        }
    }
}

/// Handles the transmission of data from a channel to multiple TCP streams asynchronously.
///
/// It continuously listens for new connections and spawns tasks to handle the transmission
/// for each connected stream.
async fn tx_to_streams(listener: TcpListener, tx: Sender<Bytes>, cancel: CancellationToken) {
    // handles the transmission of data from a channel to a TCP stream asynchronously.
    let handle_listener = |mut stream: TcpStream| {
        let mut rx = tx.subscribe();

        async move || {
            while let Ok(mut data) = rx.recv().await {
                info!("received {} bytes from the channel", data.len());

                match stream.write_all_buf(&mut data).await {
                    Ok(_) => {
                        info!("success writing all buffer bytes");
                    }
                    Err(e) => {
                        warn!("when writing buffer to the stream: {e}, dropping receiver");
                        break;
                    }
                };
            }
        }
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            Ok(( stream,_)) = listener.accept() => {
                tokio::spawn(handle_listener(stream)());
            }
        }
    }
}

const MAX_CHANNEL_MESSAGES: usize = 1024;

async fn broadcast(
    stream: TcpStream,
    listener: TcpListener,
    cancel: CancellationToken,
) -> Result<()> {
    // create the channel to share data between streams
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    // spawn the tasks to handle the data transmission
    let stream_to_tx = tokio::spawn(stream_to_tx(stream, tx.clone(), cancel.clone()));
    let tx_to_stream = tokio::spawn(tx_to_streams(listener, tx.clone(), cancel.clone()));

    // wait for the tasks to complete
    try_join!(stream_to_tx, tx_to_stream)?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // setup tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // parse arguments
    let Args { local, remote } = Args::parse();

    println!("running with passed args {:?}", Args::parse());

    // setup remote TCP stream
    let stream = TcpStream::connect(remote)
        .await
        .expect("Failed to connect to remote host");

    // setup local TCP listener
    let listener = TcpListener::bind(local)
        .await
        .expect("Failed to bind TCP listener");

    // setup cancellation token
    let cancel = CancellationToken::new();

    //  setup tasks
    let broadcast = tokio::spawn(broadcast(stream, listener, cancel.clone()));
    let signal = tokio::spawn(tokio::signal::ctrl_c());

    // wait for any of the tasks to complete
    let (broadcast, signal) = try_join!(signal, broadcast)?;

    // cancel tasks
    cancel.cancel();

    // propagate errors upstream
    broadcast?;
    signal?;

    Ok(())
}

#[cfg(test)]
mod tests {

    use rand::Rng;
    use std::{
        sync::atomic::{AtomicU16, Ordering},
        thread::sleep,
        time::Duration,
    };

    use super::*;
    use tokio::{net::TcpStream, try_join};

    #[test_log::test(tokio::test)]
    async fn run_test() {
        let cancel = CancellationToken::new();
        static NUM_ASSERTS: AtomicU16 = AtomicU16::new(0);

        let listener_addr = "127.0.0.1:8081";
        let stream_addr = "127.0.0.1:9091";

        let stream_listener = TcpListener::bind(&stream_addr).await.unwrap();
        let listener = TcpListener::bind(&listener_addr).await.unwrap();

        const NUM_BUFFERS: usize = 3;

        let mut data = [0_u8; BUFFER_SIZE * NUM_BUFFERS];

        let mut rng = rand::thread_rng();

        data.iter_mut().for_each(|b| *b = rng.gen());

        tokio::spawn(async move {
            let mut remote_stream = stream_listener.accept().await.unwrap().0;
            remote_stream.write_all(&data).await.unwrap();
        });

        // TODO: wait for `remote_stream` properly
        sleep(Duration::from_secs(2));

        let stream = TcpStream::connect(&stream_addr).await.unwrap();

        tokio::spawn(broadcast(stream, listener, cancel.clone()));

        // TODO: wait for `run` to wire everything up properly
        sleep(Duration::from_secs(2));

        tokio::spawn(async move {
            TcpStream::connect(listener_addr).await.unwrap();
        });

        let launch_client = || async move {
            let mut sink = TcpStream::connect(&listener_addr).await.unwrap();
            let mut count_read = 0;
            while count_read < data.len() {
                let mut buffer = [0u8; BUFFER_SIZE];
                let n = sink.read(&mut buffer).await.unwrap();

                assert_eq!(buffer[..n].to_vec(), data[count_read..(count_read + n)]);
                NUM_ASSERTS.fetch_add(1, Ordering::Relaxed);

                count_read += n;
            }
        };

        let handle_1 = tokio::spawn(launch_client());
        let handle_2 = tokio::spawn(launch_client());

        try_join!(handle_1, handle_2).unwrap();

        assert_eq!(NUM_ASSERTS.load(Ordering::Relaxed), 2 * NUM_BUFFERS as u16);

        cancel.cancel()
    }
}
