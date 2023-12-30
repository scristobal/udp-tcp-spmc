use clap::Parser;
use tcp_broadcast::tcp_broadcaster::tcp_broadcaster;
use tokio::io::Result;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio::{net::TcpStream, try_join};
use tracing::{info, Level};
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
    let Args { local, remote } = Args::parse();

    info!("Running with passed args {:?}", Args::parse());

    // setup remote TCP stream
    let stream = TcpStream::connect(remote)
        .await
        .expect("Failed to connect to remote host");

    // setup local TCP listener
    let listener = TcpListener::bind(local)
        .await
        .expect("Failed to bind TCP listener");

    //  setup tasks
    let broadcast = tokio::spawn(tcp_broadcaster(stream, listener));
    let signal = tokio::spawn(tokio::signal::ctrl_c());

    // wait for any of the tasks to complete
    try_join!(flatten(signal), flatten(broadcast))?;

    Ok(())
}
