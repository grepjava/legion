//! `legion`: the daemon (`legion daemon`) and the CLI that talks to it.

mod cli;
mod daemon;
mod pki;

use std::net::SocketAddr;
use std::path::PathBuf;

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
    /// Credentials: a PEM file with a client certificate, its key and the
    /// cluster CA. The daemon writes `<data-dir>/admin.pem`.
    #[arg(
        long,
        global = true,
        env = "LEGION_IDENTITY",
        default_value = "/var/db/legion/admin.pem"
    )]
    identity: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the node daemon: the manager, and on FreeBSD the agent.
    Daemon {
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// Runtime files: Podman's runroot, tmpdir and socket.
        #[arg(long, default_value = "/var/run/legion")]
        run_dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:7470")]
        listen: SocketAddr,
        /// Address other managers reach this one at. Defaults to --listen.
        #[arg(long)]
        advertise: Option<String>,
        /// Run the manager only; no tasks run on this node.
        #[arg(long)]
        no_agent: bool,
        /// Name of this node (default: the hostname's first label).
        #[arg(long)]
        node_name: Option<String>,
        /// Image for pod infra containers.
        #[arg(long)]
        infra_image: Option<String>,
        /// ZFS dataset for volumes, e.g. `zroot/legion`; without it, tasks
        /// using volumes are rejected.
        #[arg(long)]
        zfs_root: Option<String>,
        /// Workers: a manager to call, `host:port` (repeatable). Defaults to
        /// the managers recorded at join and learned since.
        #[arg(long = "manager")]
        managers: Vec<String>,
    },
    /// Start a new cluster with the reached manager as its first member.
    Init,
    /// Print the token that lets a node join as a manager or a worker.
    JoinToken {
        #[arg(value_parser = ["manager", "worker"])]
        role: String,
        /// Replace the token; the old one stops working.
        #[arg(long)]
        rotate: bool,
    },
    /// Join a cluster. Run on the new node before its daemon starts, with
    /// the daemon's --data-dir; the daemon then starts as a member.
    Join {
        token: String,
        /// Any manager of the cluster, `host:port`.
        #[arg(long)]
        manager: String,
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// Managers only: the address other managers reach this one at.
        #[arg(long)]
        advertise: Option<String>,
        /// Name of this node (default: the hostname's first label).
        #[arg(long)]
        node_name: Option<String>,
    },
    /// Show the reached manager's view of the cluster.
    Status,
    /// Create or update objects from YAML or JSON files ("-" reads stdin).
    Apply {
        #[arg(short, long = "filename", required = true)]
        files: Vec<PathBuf>,
    },
    /// Show what `apply` would change, as a diff of each object's labels and
    /// spec. Exits with status 1 when there are differences.
    Diff {
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
    /// Roll a service back to the spec its latest update came from.
    Rollback {
        service: String,
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
            run_dir,
            listen,
            advertise,
            no_agent,
            node_name,
            infra_image,
            zfs_root,
            managers,
        } => {
            daemon::run(daemon::Options {
                data_dir,
                run_dir,
                listen,
                advertise,
                agent: !no_agent,
                node_name,
                infra_image,
                zfs_root,
                managers,
            })
            .await
        }
        Command::Join {
            token,
            manager,
            data_dir,
            advertise,
            node_name,
        } => daemon::join(&token, &manager, &data_dir, advertise.as_deref(), node_name).await,
        command => cli::run(&args.server, &args.identity, command).await,
    }
}

/// SIGTERM (rc.d stop) or Ctrl-C.
pub(crate) async fn shutdown_signal() {
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
