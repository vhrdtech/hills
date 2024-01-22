use anyhow::Result;
use hills::sync_server::HillsServer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    let stdout_printer = tracing_subscriber::fmt::Layer::new();
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(stdout_printer)
        .init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;

    let server = HillsServer::start("server_play_db", "0.0.0.0:7070", &runtime)?;
    runtime.block_on(server.join)?;
    Ok(())
}
