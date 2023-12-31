use crate::asyncsender::{reader_to_tx, tx_to_writer};
use tokio::io::AsyncRead;
use tokio::io::Result;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::broadcast::{self, Sender};
use tokio::try_join;
use tokio_util::bytes::Bytes;
use tracing::instrument;

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

/// Transfers data from a Sender of Bytes into multiple TCP streams.
#[instrument(name = "udp_broadcaster", skip_all)]
async fn tx_to_streams(listener: TcpListener, tx: Sender<Bytes>) {
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(tx_to_writer(stream, tx.clone()));
    }
}

/// Transfers data from a UDP Socket into a Sender of Bytes
#[instrument(name = "udp_broadcaster", skip_all)]
async fn socket_to_tx<A: Into<AsyncUdpSocket>>(socket: A, tx: Sender<Bytes>) {
    reader_to_tx(socket.into(), tx).await
}

const MAX_CHANNEL_MESSAGES: usize = 1024;

/// Broadcasts data from a TCP stream to multiple TCP streams.
#[instrument(name = "udp_broadcaster", skip_all)]
pub async fn udp_broadcaster(socket: UdpSocket, listener: TcpListener) -> Result<()> {
    // create the channel to share data between streams
    let (tx, _) = broadcast::channel::<Bytes>(MAX_CHANNEL_MESSAGES);

    // spawn the tasks to handle the data transmission
    let socket_to_tx = tokio::spawn(socket_to_tx(socket, tx.clone()));
    let tx_to_stream = tokio::spawn(tx_to_streams(listener, tx.clone()));

    // wait for the tasks to complete
    try_join!(socket_to_tx, tx_to_stream)?;

    Ok(())
}

#[cfg(test)]
mod test {

    use rand::Rng;
    use std::{
        future::Future,
        sync::atomic::{AtomicU16, Ordering},
        thread::sleep,
        time::Duration,
    };

    use super::*;
    use tokio::{io::AsyncReadExt, net::TcpStream, try_join};

    const CHUNK_SIZE: usize = 256;
    const NUM_CHUNKS: usize = 2; // <-- total data CHUNK_SIZE * NUM_CHUNKS must fit on UDP buffer

    struct WaitForTest(AtomicU16);
    static COUNT_ASSERTS: WaitForTest = WaitForTest(AtomicU16::new(0));

    impl Future for WaitForTest {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            if self.0.load(Ordering::Relaxed) == 2 * NUM_CHUNKS as u16 {
                std::task::Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }

    #[instrument(name = "udp_broadcaster", skip_all)]
    async fn launch_client(listener_addr: &str, data: [u8; CHUNK_SIZE * NUM_CHUNKS]) {
        let mut sink = TcpStream::connect(listener_addr).await.unwrap();
        let mut count_read = 0;

        while count_read < data.len() {
            let mut buffer = [0u8; CHUNK_SIZE];

            let n = sink.read(&mut buffer).await.unwrap();

            assert_eq!(buffer[..n].to_vec(), data[count_read..(count_read + n)]);
            COUNT_ASSERTS.0.fetch_add(1, Ordering::Relaxed);

            count_read += n;
        }
    }

    // #[ignore = "reason"]
    #[test_log::test(tokio::test)]
    async fn broadcast_test() {
        // prepare test data
        let mut data = [0_u8; CHUNK_SIZE * NUM_CHUNKS];
        let mut rng = rand::thread_rng();
        data.iter_mut().for_each(|b| *b = rng.gen());

        // bind addresses to mock servers and clients
        let listener_addr = "127.0.0.1:8081"; // <-- TCP server address, used to listen to incoming TCP connections
        let read_addr = "127.0.0.1:8091"; // <-- UDP socket address, used to listen to messages that will be distributed to TCP connections at previous address
        let write_addr = "127.0.0.1:8092"; // <-- used to send test data to `read_addr`

        let listener = TcpListener::bind(&listener_addr).await.unwrap();
        let read_socket: UdpSocket = UdpSocket::bind(read_addr).await.unwrap();
        let write_socket: UdpSocket = UdpSocket::bind(write_addr).await.unwrap();

        write_socket.connect(read_addr).await.unwrap();

        tokio::spawn(udp_broadcaster(read_socket, listener));
        sleep(Duration::from_secs(1));

        let client_1 = tokio::spawn(launch_client(listener_addr, data));
        let client_2 = tokio::spawn(launch_client(listener_addr, data));

        let sender = tokio::task::spawn(async move {
            sleep(Duration::from_secs(1));
            write_socket.send(&data).await.unwrap();
        });

        try_join!(client_1, client_2, sender).unwrap();

        assert_eq!(
            COUNT_ASSERTS.0.load(Ordering::Relaxed),
            2 * NUM_CHUNKS as u16
        );
    }
}
