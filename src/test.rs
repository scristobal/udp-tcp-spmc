use super::*;
use rand::Rng;
use std::{
    future::Future,
    sync::atomic::{AtomicU16, Ordering},
    thread::sleep,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    try_join,
};

struct WaitForTest<const N: usize>(AtomicU16);

impl<const N: usize> Future for WaitForTest<N> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.0.load(Ordering::Relaxed) == N as u16 {
            std::task::Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    }
}

#[test_log::test(tokio::test)]
async fn test_reader_to_tx() {
    const DATA_SIZE: usize = 1024;

    let mut data = [0_u8; DATA_SIZE];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    static COUNT_ASSERTS: AtomicU16 = AtomicU16::new(0);

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let (reader, mut writer) = tokio::io::duplex(BUFFER_SIZE);

    tokio::spawn(reader_to_tx(reader, tx.clone()));

    // give `reader_to_tx` a break to wire everything up
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
    const DATA_SIZE: usize = 1024;

    static COUNT_ASSERTS: AtomicU16 = AtomicU16::new(0);

    let mut data = [0_u8; DATA_SIZE];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    let (tx, _) = tokio::sync::broadcast::channel(1);

    let (mut reader, writer) = tokio::io::duplex(BUFFER_SIZE);

    tokio::spawn(tx_to_writer(writer, tx.clone()));

    // give `tx_to_writer` a break to wire everything up
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

#[test_log::test(tokio::test)]
async fn tcp_broadcast_test() {
    // prepare test data
    const CHUNK_SIZE: usize = 256;
    const NUM_CHUNKS: usize = 3;

    let mut data = [0_u8; CHUNK_SIZE * NUM_CHUNKS];
    let mut rng = rand::thread_rng();
    data.iter_mut().for_each(|b| *b = rng.gen());

    static COUNT_ASSERTS: WaitForTest<{ 2 * NUM_CHUNKS }> = WaitForTest(AtomicU16::new(0));

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

    tokio::spawn(tcp_broadcaster(stream, listener));

    // give `tcp_broadcaster` a break to wire things up
    sleep(Duration::from_secs(1));

    tokio::spawn(TcpStream::connect(listener_addr));

    let client_1 = tokio::spawn(launch_client(listener_addr, data));
    let client_2 = tokio::spawn(launch_client(listener_addr, data));

    try_join!(client_1, client_2).unwrap();

    assert_eq!(
        COUNT_ASSERTS.0.load(Ordering::Relaxed),
        2 * NUM_CHUNKS as u16
    );
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

    static COUNT_ASSERTS: WaitForTest<{ 2 * NUM_CHUNKS }> = WaitForTest(AtomicU16::new(0));

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
