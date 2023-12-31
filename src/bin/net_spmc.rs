use clap::Parser;
use tcp_broadcast::socket::udp_broadcaster;
use tcp_broadcast::stream::tcp_broadcaster;
use tokio::io::Result;
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;
use tokio::{net::TcpStream, try_join};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Simple TCP broadcaster, connects to a remote TCP host and broadcast to a local TCP socket
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// host:port for consumers to connect and get data pushed
    #[arg(short = 'c', long)]
    consumer: String,

    /// protocol://host:port for producer to pull(TCP) or listen(UDP) data from
    #[arg(short = 'p', long)]
    producer: String,
}

/// Utility function to use with `try_join!` in `main`
async fn flatten<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => panic!("Error joining task: {:?}", err),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // setup tracing
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // parse arguments
    let Args { consumer, producer } = Args::parse();

    info!("Running with passed args {:?}", Args::parse());

    // setup local TCP listener
    let listener = TcpListener::bind(&producer)
        .await
        .expect("Failed to bind TCP listener");

    // get protocol from producer
    let protocol = *producer
        .split("://")
        .collect::<Vec<_>>()
        .first()
        .expect("Invalid producer parameter, missing protocol: proto://<domain>:<port>");

    let broadcast = match protocol {
        "tcp" => {
            // setup remote TCP stream
            let stream = TcpStream::connect(consumer)
                .await
                .expect("Failed to connect to remote host");

            //  setup tasks
            tokio::spawn(tcp_broadcaster(stream, listener))
        }

        "udp" => {
            // setup UDP listener
            let socket = UdpSocket::bind(consumer)
                .await
                .expect("Failed to connect to remote host");

            //  setup tasks
            tokio::spawn(udp_broadcaster(socket, listener))
        }
        _ => panic!("Unsupported protocol: {}", protocol),
    };

    let signal = tokio::spawn(tokio::signal::ctrl_c());

    // wait for any of the tasks to complete
    try_join!(flatten(signal), flatten(broadcast))?;

    Ok(())
}
