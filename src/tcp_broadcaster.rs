use tokio::io::Result;
use tokio::net::TcpListener;

use tokio::sync::broadcast::{self, Sender};
use tokio::{net::TcpStream, try_join};
use tokio_util::bytes::Bytes;
use tracing::instrument;

use crate::async_sender::{reader_to_tx, tx_to_writer};

/// Continuously reads data from a TCP stream and sends it to a channel of bytes.
#[instrument(name = "tcp_broadcaster", skip_all)]
async fn stream_to_tx(stream: TcpStream, tx: Sender<Bytes>) {
    reader_to_tx(stream, tx).await
}

/// Handles the transmission of data from a channel to multiple TCP streams asynchronously.
#[instrument(name = "tcp_broadcaster", skip_all)]
async fn tx_to_streams(listener: TcpListener, tx: Sender<Bytes>) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(tx_to_writer(stream, tx.clone()));
    }
}

const MAX_CHANNEL_MESSAGES: usize = 1024;

/// Broadcasts data from a TCP stream to multiple TCP streams.
#[instrument(name = "tcp_broadcaster", skip_all)]
pub async fn tcp_broadcaster(stream: TcpStream, listener: TcpListener) -> Result<()> {
    // create the channel to share data between streams
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    // spawn the tasks to handle the data transmission
    let stream_to_tx = tokio::spawn(stream_to_tx(stream, tx.clone()));
    let tx_to_stream = tokio::spawn(tx_to_streams(listener, tx.clone()));

    // wait for the tasks to complete
    try_join!(stream_to_tx, tx_to_stream)?;

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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        try_join,
    };

    const CHUNK_SIZE: usize = 1024;
    const NUM_CHUNKS: usize = 3;
    static COUNT_ASSERTS: AtomicU16 = AtomicU16::new(0);

    async fn launch_client(listener_addr: &str, data: [u8; CHUNK_SIZE * NUM_CHUNKS]) {
        let mut sink = TcpStream::connect(listener_addr).await.unwrap();
        let mut count_read = 0;

        while count_read < data.len() {
            let mut buffer = [0u8; CHUNK_SIZE];

            let n = sink.read(&mut buffer).await.unwrap();

            assert_eq!(buffer[..n].to_vec(), data[count_read..(count_read + n)]);
            COUNT_ASSERTS.fetch_add(1, Ordering::Relaxed);

            count_read += n;
        }
    }

    #[test_log::test(tokio::test)]
    async fn broadcast_test() {
        let listener_addr = "127.0.0.1:8081";
        let stream_addr = "127.0.0.1:9091";

        let stream_listener = TcpListener::bind(&stream_addr).await.unwrap();
        let listener = TcpListener::bind(&listener_addr).await.unwrap();

        let mut data = [0_u8; CHUNK_SIZE * NUM_CHUNKS];

        let mut rng = rand::thread_rng();

        data.iter_mut().for_each(|b| *b = rng.gen());

        tokio::task::spawn(async move {
            let mut remote_stream = stream_listener.accept().await.unwrap().0;
            remote_stream.write_all(&data).await.unwrap();
        });

        // TODO: wait for `remote_stream` properly
        sleep(Duration::from_secs(1));

        let stream = TcpStream::connect(&stream_addr).await.unwrap();

        tokio::spawn(tcp_broadcaster(stream, listener));

        // TODO: wait for `run` to wire everything up properly
        sleep(Duration::from_secs(1));

        tokio::spawn(TcpStream::connect(listener_addr));

        let handle_1 = tokio::spawn(launch_client(listener_addr, data));
        let handle_2 = tokio::spawn(launch_client(listener_addr, data));

        try_join!(handle_1, handle_2).unwrap();

        assert_eq!(COUNT_ASSERTS.load(Ordering::Relaxed), 2 * NUM_CHUNKS as u16);
    }
}
