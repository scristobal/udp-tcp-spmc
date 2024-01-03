use super::*;
use once_cell::sync::Lazy;
use rand::Rng;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    thread::sleep,
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct WaitForTest<const N: usize>(Arc<AtomicUsize>);

impl<const N: usize> WaitForTest<N> {
    pub fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }
    fn add(&self, n: usize) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    fn check(&self) -> bool {
        self.0.load(Ordering::Relaxed) == N
    }
}

impl<const N: usize> Future for WaitForTest<N> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.0.load(Ordering::Relaxed) == N {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_reader_to_tx() {
    // prepare test data
    const DATA_SIZE: usize = 1024;

    let mut data = [0_u8; DATA_SIZE];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    // setup async assert counter
    static COUNT_ASSERTS: Lazy<WaitForTest<1>> = Lazy::new(WaitForTest::new);

    // setup function under test
    let (tx, _) = tokio::sync::broadcast::channel(1);
    let (reader, mut writer) = tokio::io::duplex(BUFFER_SIZE);
    tokio::spawn(reader_to_tx(reader, tx.clone())); // <- function under test

    // give `reader_to_tx` a break to wire everything up
    sleep(Duration::from_secs(1));

    tokio::spawn(async move {
        let _ = writer.write(&data).await.unwrap();
    });

    tokio::spawn({
        let mut rx = tx.subscribe();

        async move {
            let received = rx.recv().await.unwrap();
            assert_eq!(data[..], received);
            COUNT_ASSERTS.add(1);
        }
    });

    COUNT_ASSERTS.clone().await;
    assert!(COUNT_ASSERTS.check());
}

#[test_log::test(tokio::test)]
async fn test_tx_to_writer() {
    // prepare test data
    const DATA_SIZE: usize = 1024;

    let mut data = [0_u8; DATA_SIZE];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    // setup async assert counter
    static COUNT_ASSERTS: Lazy<WaitForTest<1>> = Lazy::new(WaitForTest::new);

    // setup function under test
    let (tx, _) = tokio::sync::broadcast::channel(1);
    let (mut reader, writer) = tokio::io::duplex(BUFFER_SIZE);
    tokio::spawn(tx_to_writer(writer, tx.clone())); // <- function under test

    // give `tx_to_writer` a break to wire everything up
    sleep(Duration::from_secs(1));

    tokio::spawn(async move {
        let mut received = vec![0; DATA_SIZE];
        reader.read_exact(&mut received).await.unwrap();

        assert_eq!(data[..], received);
        COUNT_ASSERTS.add(1);
    });

    tokio::spawn(async move {
        let data_bytes = Bytes::from(data[..].to_vec());
        tx.send(data_bytes).unwrap();
    });

    COUNT_ASSERTS.clone().await;
    assert!(COUNT_ASSERTS.check());
}

#[test_log::test(tokio::test)]
async fn tcp_broadcast_test() {
    // prepare test data
    const CHUNK_SIZE: usize = 256;
    const NUM_CHUNKS: usize = 3;

    let mut data = [0_u8; CHUNK_SIZE * NUM_CHUNKS];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    // helper function to mock a client
    async fn launch_client(listener_addr: &str, data: [u8; CHUNK_SIZE * NUM_CHUNKS]) {
        let mut sink = TcpStream::connect(listener_addr).await.unwrap();
        let mut count_read = 0;

        while count_read < data.len() {
            let mut buffer = [0u8; CHUNK_SIZE];

            let n = sink.read(&mut buffer).await.unwrap();

            assert_eq!(buffer[..n].to_vec(), data[count_read..(count_read + n)]);
            COUNT_ASSERTS.add(1);

            count_read += n;
        }
    }

    const NUM_CLIENTS: usize = 2;

    // setup async assert counter
    static COUNT_ASSERTS: Lazy<WaitForTest<{ NUM_CLIENTS * NUM_CHUNKS }>> =
        Lazy::new(WaitForTest::new);

    // bind addresses to mock servers and clients
    let listener_addr = "127.0.0.1:9081"; // <-- tests TCP clients will connect here
    let stream_addr = "127.0.0.1:9091"; // <-- test data will be send here

    let stream = TcpListener::bind(&stream_addr).await.unwrap();
    let listener = TcpListener::bind(&listener_addr).await.unwrap();

    tokio::task::spawn(async move {
        let mut remote_stream = stream.accept().await.unwrap().0;
        remote_stream.write_all(&data).await.unwrap();
    });

    // give `remote_stream` a break to setup
    sleep(Duration::from_secs(1));

    let stream = TcpStream::connect(&stream_addr).await.unwrap();

    tokio::spawn(tcp_broadcaster(stream, listener)); // <- function under test

    // give `tcp_broadcaster` a break to wire things up
    sleep(Duration::from_secs(1));
    tokio::spawn(TcpStream::connect(listener_addr));

    // launch mock clients
    let client_1 = tokio::spawn(launch_client(listener_addr, data));
    let client_2 = tokio::spawn(launch_client(listener_addr, data));

    try_join!(client_1, client_2).unwrap();

    COUNT_ASSERTS.clone().await;
    assert!(COUNT_ASSERTS.check());
}

// #[ignore = "reason"]
#[test_log::test(tokio::test)]
async fn udp_broadcast_test() {
    // prepare test data
    const CHUNK_SIZE: usize = 256;
    const NUM_CHUNKS: usize = 3;

    let mut data = [0_u8; CHUNK_SIZE * NUM_CHUNKS];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    // helper function to mock a client
    async fn launch_client(listener_addr: &str, data: [u8; CHUNK_SIZE * NUM_CHUNKS]) {
        let mut sink = TcpStream::connect(listener_addr).await.unwrap();
        let mut count_read = 0;

        while count_read < data.len() {
            let mut buffer = [0u8; CHUNK_SIZE];

            let n = sink.read(&mut buffer).await.unwrap();

            assert_eq!(buffer[..n].to_vec(), data[count_read..(count_read + n)]);
            COUNT_ASSERTS.add(1);

            count_read += n;
        }
    }

    const NUM_CLIENTS: usize = 2;

    // setup async assert counter
    static COUNT_ASSERTS: Lazy<WaitForTest<{ NUM_CLIENTS * NUM_CHUNKS }>> =
        Lazy::new(WaitForTest::new);

    // bind addresses to mock servers and clients
    let listener_addr = "127.0.0.1:8081"; // <-- TCP server address, used to listen to incoming TCP connections
    let read_addr = "127.0.0.1:8091"; // <-- UDP socket address, used to listen to messages that will be distributed to TCP connections at previous address
    let write_addr = "127.0.0.1:8092"; // <-- used to send test data to `read_addr`

    let listener = TcpListener::bind(&listener_addr).await.unwrap();
    let read_socket: UdpSocket = UdpSocket::bind(read_addr).await.unwrap();
    let write_socket: UdpSocket = UdpSocket::bind(write_addr).await.unwrap();

    write_socket.connect(read_addr).await.unwrap();

    tokio::spawn(udp_broadcaster(read_socket, listener));

    // give `udp_broadcaster` a break to wire things up
    sleep(Duration::from_secs(1));

    // launch mock clients
    let client_1 = tokio::spawn(launch_client(listener_addr, data));
    let client_2 = tokio::spawn(launch_client(listener_addr, data));

    // send test data
    let sender = tokio::spawn(async move {
        sleep(Duration::from_secs(1));
        write_socket.send(&data).await.unwrap();
    });

    try_join!(client_1, client_2, sender).unwrap();

    COUNT_ASSERTS.clone().await;
    assert!(COUNT_ASSERTS.check());
}
