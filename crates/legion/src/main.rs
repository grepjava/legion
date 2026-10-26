//! `legion`: the daemon (`legion daemon`) and the CLI that talks to it.

mod cli;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "legion",
    version,
    about = "Container orchestration for FreeBSD"
)]
struct Args {
    /// Manager to talk to.
    #[arg(
        long,
        global = true,
        env = "LEGION_SERVER",
        default_value = "127.0.0.1:7470"
    )]
    server: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the node daemon (manager role).
    Daemon {
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7470")]
        listen: SocketAddr,
        /// Address other managers reach this one at. Defaults to --listen.
        #[arg(long)]
        advertise: Option<String>,
    },
    /// Start a new cluster with the reached manager as its first member.
    Init,
    /// Show the reached manager's view of the cluster.
    Status,
    /// Create or update objects from YAML or JSON files ("-" reads stdin).
    Apply {
        #[arg(short, long = "filename", required = true)]
        files: Vec<PathBuf>,
    },
    /// Show one object.
    Get {
        kind: String,
        name: String,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Yaml)]
        output: cli::Format,
    },
    /// List objects of a kind.
    List {
        kind: String,
        #[arg(short, long, conflicts_with = "all_namespaces")]
        namespace: Option<String>,
        #[arg(short = 'A', long)]
        all_namespaces: bool,
        /// Label selector, `key=value` (repeatable; all must match).
        #[arg(short = 'l', long = "selector")]
        selector: Vec<String>,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
    },
    /// Delete one object.
    Delete {
        kind: String,
        name: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Stream changes, optionally of one kind.
    Watch {
        kind: Option<String>,
        /// Start after this version (default: changes from now on).
        #[arg(long)]
        from: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Daemon {
            data_dir,
            listen,
            advertise,
        } => daemon(data_dir, listen, advertise).await,
        command => cli::run(&args.server, command).await,
    }
}

async fn daemon(
    data_dir: PathBuf,
    listen: SocketAddr,
    advertise: Option<String>,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,openraft=warn".into()),
        )
        .init();
    let manager = legion_manager::Manager::open(legion_manager::ManagerConfig {
        data_dir,
        advertise: advertise.unwrap_or_else(|| listen.to_string()),
        raft: legion_manager::default_raft_config(),
    })
    .await
    .map_err(|e| anyhow::anyhow!(e))
    .context("opening the store")?;
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("listening on {listen}"))?;
    tracing::info!(%listen, node_id = manager.id(), "serving");
    manager
        .serve(listener, shutdown_signal())
        .await
        .map_err(|e| anyhow::anyhow!(e))
}

/// SIGTERM (rc.d stop) or Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
