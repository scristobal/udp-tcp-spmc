use tokio::io::AsyncRead;
use tokio::io::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::net::UdpSocket;
use tokio::sync::broadcast::{self, Sender};
use tokio::try_join;
use tokio_util::bytes::Bytes;
use tokio_util::bytes::BytesMut;
use tracing::instrument;
use tracing::{debug, warn};

// In udp, if a message is larger than the buffer remaining bytes will be discarded
// https://docs.rs/tokio/latest/tokio/net/struct.UdpSocket.html#method.recv_buf
const BUFFER_SIZE: usize = 8 * 1024;

/// Continuously reads data from an async reader and sends it to a channel of bytes.
#[instrument(skip_all)]
pub async fn reader_to_tx<R: AsyncReadExt + Unpin>(mut reader: R, tx: Sender<Bytes>) {
    let mut buffer = BytesMut::with_capacity(BUFFER_SIZE);

    while let Ok(n) = reader.read_buf(&mut buffer).await {
        if n == 0 {
            debug!("no bytes read from the reader");
            continue;
        }

        let data = buffer.split_to(n).freeze();

        match tx.send(data) {
            Ok(r) => debug!("send {n} bytes to {r} receivers"),
            Err(_) => warn!("no listeners subscribed when sending, lost {n} bytes of data"),
            // alternatively we could try put the Bytes back, eg. Err(SendError(b)) => buffer.put(b)
        }
    }
}

/// Handles the transmission of data from a channel to an async writer.
#[instrument(skip_all)]
pub async fn tx_to_writer<W: AsyncWriteExt + Unpin + std::fmt::Debug>(
    mut writer: W,
    tx: Sender<Bytes>,
) {
    let mut rx = tx.subscribe();

    while let Ok(mut data) = rx.recv().await {
        debug!("received {} bytes from the channel", data.len());

        match writer.write_all_buf(&mut data).await {
            Ok(_) => debug!("success writing all buffer bytes"),
            Err(e) => {
                warn!("when writing buffer to the stream: {e}, dropping receiver");
                break;
            }
        }
    }
}

/// Handles the transmission of data from a channel to multiple TCP streams asynchronously.
#[instrument(skip_all)]
async fn tx_to_streams(listener: TcpListener, tx: Sender<Bytes>) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(tx_to_writer(stream, tx.clone()));
    }
}

/// Thin wrapper around Tokio's UdpSocket to implement `AsyncRead`` trait, and by extension `ASyncReadExt`
struct AsyncUdpSocket(UdpSocket);

impl From<UdpSocket> for AsyncUdpSocket {
    fn from(socket: UdpSocket) -> Self {
        Self(socket)
    }
}

impl AsyncRead for AsyncUdpSocket {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.0.poll_recv(cx, buf)
    }
}

/// Lagging is ignored, https://docs.rs/tokio/1.35.1/tokio/sync/broadcast/#lagging
const MAX_CHANNEL_MESSAGES: usize = 1024;

/// Broadcasts data from a TCP stream to multiple TCP streams.
#[instrument(skip_all)]
pub async fn udp_broadcaster(socket: UdpSocket, listener: TcpListener) -> Result<()> {
    // create the channel to share data between streams
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    // spawn the tasks to handle the data transmission
    let socket_to_tx = tokio::spawn(reader_to_tx::<AsyncUdpSocket>(socket.into(), tx.clone()));
    let tx_to_stream = tokio::spawn(tx_to_streams(listener, tx.clone()));

    // wait for the tasks to complete
    try_join!(socket_to_tx, tx_to_stream)?;

    Ok(())
}

/// Broadcasts data from a TCP stream to multiple TCP streams.
#[instrument(skip_all)]
pub async fn tcp_broadcaster(stream: TcpStream, listener: TcpListener) -> Result<()> {
    // create the channel to share data between streams
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    // spawn the tasks to handle the data transmission
    let stream_to_tx = tokio::spawn(reader_to_tx(stream, tx.clone()));
    let tx_to_stream = tokio::spawn(tx_to_streams(listener, tx.clone()));

    // wait for the tasks to complete
    try_join!(stream_to_tx, tx_to_stream)?;

    Ok(())
}

#[cfg(test)]
mod test;
