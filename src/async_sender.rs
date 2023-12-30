use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio::sync::broadcast::error::SendError;
use tokio::sync::broadcast::Sender;

use tokio_util::bytes::{Bytes, BytesMut};
use tracing::{info, instrument, warn};

const BUFFER_SIZE: usize = 1024;

/// Continuously reads data from an async reader and sends it to a channel of bytes.
#[instrument(name = "async_sender", skip_all)]
pub async fn reader_to_tx<R: AsyncReadExt + Unpin>(mut reader: R, tx: Sender<Bytes>) {
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);

    while let Ok(n) = reader.read_buf(&mut buffer).await {
        if n == 0 {
            continue;
        }

        let data = buffer.split_to(n).freeze();

        match tx.send(data) {
            Ok(r) => info!("send {n} bytes to {r} receivers"),
            Err(SendError(b)) => warn!(
                "no listeners subscribed when received {} bytes to the channel",
                b.len()
            ),
        }
    }
}

/// Handles the transmission of data from a channel to an async writer.
#[instrument(name = "async_sender", skip_all)]
pub async fn tx_to_writer<W: AsyncWriteExt + Unpin + std::fmt::Debug>(
    mut writer: W,
    tx: Sender<Bytes>,
) {
    info!("starting writer");

    let mut rx = tx.subscribe();

    while let Ok(mut data) = rx.recv().await {
        info!("received {} bytes from the channel", data.len());

        match writer.write_all_buf(&mut data).await {
            Ok(_) => {
                info!("success writing all buffer bytes");
            }
            Err(e) => {
                warn!("when writing buffer to the stream: {e}, dropping receiver");
                break;
            }
        }
    }

    info!("writer dropped");
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
        try_join,
    };

    const DATA_SIZE: usize = 1024;

    #[test_log::test(tokio::test)]
    async fn test_reader_to_tx() {
        let mut data = [0_u8; DATA_SIZE];
        let mut rng = rand::thread_rng();
        data.iter_mut().for_each(|b| *b = rng.gen());

        static COUNT_ASSERTS: AtomicU16 = AtomicU16::new(0);

        let (tx, _) = tokio::sync::broadcast::channel(1);

        let (reader, mut writer) = tokio::io::duplex(BUFFER_SIZE);

        tokio::spawn(reader_to_tx(reader, tx.clone()));

        sleep(Duration::from_secs(1));

        let write = tokio::spawn(async move {
            let _ = writer.write(&data).await.unwrap();
        });

        let read = tokio::spawn({
            let mut rx = tx.subscribe();

            async move {
                let received = rx.recv().await.unwrap();
                assert_eq!(data[..], received);
                COUNT_ASSERTS.fetch_add(1, Ordering::Relaxed);
            }
        });

        try_join!(write, read).unwrap();

        assert_eq!(COUNT_ASSERTS.load(Ordering::Relaxed), 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_tx_to_writer() {
        static COUNT_ASSERTS: AtomicU16 = AtomicU16::new(0);

        let mut data = [0_u8; DATA_SIZE];
        let mut rng = rand::thread_rng();
        data.iter_mut().for_each(|b| *b = rng.gen());

        let (tx, _) = tokio::sync::broadcast::channel(1);

        let (mut reader, writer) = tokio::io::duplex(BUFFER_SIZE);

        tokio::spawn(tx_to_writer(writer, tx.clone()));

        sleep(Duration::from_secs(1));

        let read = tokio::spawn(async move {
            let mut received = vec![0; DATA_SIZE];
            reader.read_exact(&mut received).await.unwrap();

            assert_eq!(data[..], received);
            COUNT_ASSERTS.fetch_add(1, Ordering::Relaxed);
        });

        let send = tokio::spawn(async move {
            let data_bytes = Bytes::from(data[..].to_vec());
            tx.send(data_bytes).unwrap();
        });

        try_join!(read, send).unwrap();

        assert_eq!(COUNT_ASSERTS.load(Ordering::Relaxed), 1);
    }
}
