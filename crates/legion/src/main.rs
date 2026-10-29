//! `legion`: the daemon (`legion daemon`) and the CLI that talks to it.

mod autolock;
mod cli;
mod daemon;
mod metrics;
mod pki;
mod session;

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
// Parsed once per run: the daemon's many flags cost nothing to keep inline.
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the node daemon: the manager, and on FreeBSD the agent.
    Daemon {
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// Runtime files: tasks' secrets and configs.
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
        /// Serve Prometheus metrics at `http://<addr>/metrics`, without TLS
        /// or authentication: the names of every namespace's services and
        /// tasks show there.
        #[arg(long)]
        metrics_listen: Option<SocketAddr>,
        /// Managers: a floating address for the API, held by one manager at
        /// a time (the leader when it can) with CARP on --vip-interface.
        /// Every manager on the link sets the same address, vhid and
        /// password; listen on 0.0.0.0 so the API answers on it.
        #[arg(long, requires_all = ["vip_interface", "vip_password_file"])]
        vip: Option<std::net::Ipv4Addr>,
        #[arg(long)]
        vip_interface: Option<String>,
        /// The CARP virtual host id, 1 to 255.
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..))]
        vip_vhid: u8,
        /// A file holding the CARP password.
        #[arg(long)]
        vip_password_file: Option<PathBuf>,
        /// Serve HTTP routes (`Route` objects) here, e.g. `0.0.0.0:80`.
        #[arg(long)]
        ingress_listen: Option<SocketAddr>,
        /// Serve HTTPS for routes with `tls` here, e.g. `0.0.0.0:443`.
        #[arg(long)]
        ingress_tls_listen: Option<SocketAddr>,
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
    /// Autolock: managers seal their private keys with an unlock key and,
    /// after a restart, wait for `legion unlock` before they start.
    /// `enable` prints the key (keep it safe: without it a restarted
    /// manager does not start), `rotate` replaces it, `disable` drops it,
    /// `show` prints it.
    Autolock {
        #[arg(value_parser = ["enable", "disable", "rotate", "show"])]
        action: String,
    },
    /// Give a locked manager on this host its unlock key, read from
    /// --key-file or else from stdin.
    Unlock {
        #[arg(long)]
        key_file: Option<PathBuf>,
    },
    /// Save every object of the cluster, read at one version, to a file.
    /// It holds Secrets and join tokens: keep it as safe as the managers'
    /// disks. The managers' keys are not in it; keep `<data-dir>/pki` too.
    Backup {
        #[arg(short, long)]
        output: PathBuf,
    },
    /// Restore a stopped manager's store, from a backup or, without FILE,
    /// from its own store when the other managers are lost. The manager
    /// becomes the only member of the cluster; start its daemon after, and
    /// have other managers join it again from empty data directories. The
    /// old store is kept beside the new one.
    Restore {
        file: Option<PathBuf>,
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// The address other managers reach this one at. Defaults to the
        /// one the backup records for this manager.
        #[arg(long)]
        advertise: Option<String>,
        /// With autolock on, a file holding the unlock key.
        #[arg(long)]
        unlock_key_file: Option<PathBuf>,
    },
    /// Images on this node.
    Image {
        #[command(subcommand)]
        command: ImageCommand,
    },
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
    /// Deploy, convert or remove compose stacks.
    Stack {
        #[command(subcommand)]
        command: StackCommand,
    },
    /// People's access: user certificates and their roles.
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    /// Set a node's availability: `drain` moves its tasks elsewhere (and
    /// stateful tasks' Volumes with them), `pause` keeps its tasks but
    /// places no new ones, `activate` undoes either.
    Node {
        #[arg(value_parser = ["drain", "pause", "activate"])]
        availability: String,
        node: String,
    },
    /// Volumes: where their data is.
    Volume {
        #[command(subcommand)]
        command: VolumeCommand,
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
    /// Print a container's output. A service name picks one of its running
    /// tasks.
    Logs {
        task: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// The container, when the pod has more than one.
        #[arg(short, long)]
        container: Option<String>,
        /// Keep printing until the container stops.
        #[arg(short, long)]
        follow: bool,
        /// Only the last lines.
        #[arg(long)]
        tail: Option<u32>,
    },
    /// Run a command in a task's container; exits with its exit code. A
    /// service name picks one of its running tasks.
    Exec {
        task: String,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long)]
        container: Option<String>,
        /// Pass stdin to the command.
        #[arg(short, long)]
        interactive: bool,
        /// Give the command a terminal.
        #[arg(short, long)]
        tty: bool,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Forward local ports to a task's pod: `LOCAL:REMOTE`, or `PORT` for
    /// both. Each connection may reach another running task when TASK names
    /// a service.
    PortForward {
        task: String,
        #[arg(required = true)]
        ports: Vec<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Where to listen.
        #[arg(long, default_value = "127.0.0.1")]
        address: String,
    },
}

#[derive(Subcommand)]
pub enum ImageCommand {
    /// Load an image from an OCI archive (`podman save --format
    /// oci-archive`) into this node's image store, beside a running daemon
    /// or not: for images no registry serves (`localhost/...`).
    Load {
        file: PathBuf,
        /// Its name; by default the one the archive gives.
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// The daemon's `--zfs-root`.
        #[arg(long)]
        zfs_root: String,
    },
}

#[derive(Subcommand)]
pub enum VolumeCommand {
    /// Move a Volume's data to NODE (its `spec.node`): copied while its
    /// tasks run, which then stop and start again there once the rest has
    /// gone. It stays there, and returns there after a drain.
    Move {
        volume: String,
        node: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum UserCommand {
    /// Certify a person as user NAME and write their credentials to a file
    /// to hand them; with --role, also give them that role in a namespace
    /// (a RoleBinding named after them). The key is made here and never
    /// sent anywhere.
    Create {
        name: String,
        #[arg(long, value_parser = ["view", "edit", "admin"], requires = "namespace")]
        role: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Where to write the credentials (default: NAME.pem).
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// How long the certificate is valid.
        #[arg(long, default_value_t = 90)]
        days: u32,
    },
}

#[derive(Subcommand)]
pub enum StackCommand {
    /// Create or update a stack's services, volumes, secrets and configs
    /// from a compose file.
    Deploy {
        stack: String,
        #[arg(short = 'c', long = "compose-file", required = true)]
        file: PathBuf,
        /// Namespace of the stack's objects (default: the stack's name).
        #[arg(short, long)]
        namespace: Option<String>,
        /// Remove the stack's services the file no longer has.
        #[arg(long)]
        prune: bool,
        /// Fail on anything Legion cannot honour instead of warning.
        #[arg(long)]
        strict: bool,
    },
    /// Print the objects a compose file becomes, as Legion YAML.
    Convert {
        stack: String,
        #[arg(short = 'c', long = "compose-file", required = true)]
        file: PathBuf,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Remove a stack's services, secrets and configs; its volumes and
    /// their data stay.
    Rm {
        stack: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
}

/// The jail runtime's supervisor (`legion jail-shim ...`, see
/// `legion_jail::shim`): run before anything else starts, as it forks and
/// must be single-threaded.
pub const JAIL_SHIM: &str = "jail-shim";

fn main() -> anyhow::Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some(JAIL_SHIM) {
        std::process::exit(legion_jail::shim::main(&argv[2..]));
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> anyhow::Result<()> {
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
            metrics_listen,
            vip,
            vip_interface,
            vip_vhid,
            vip_password_file,
            ingress_listen,
            ingress_tls_listen,
        } => {
            let vip = match (vip, vip_interface, vip_password_file) {
                (Some(addr), Some(interface), Some(file)) => {
                    let password = std::fs::read_to_string(&file)
                        .map_err(|e| anyhow::anyhow!("reading {}: {e}", file.display()))?
                        .trim()
                        .to_owned();
                    Some(legion_manager::vip::VipConfig {
                        interface,
                        vhid: vip_vhid,
                        addr,
                        password,
                    })
                }
                _ => None,
            };
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
                metrics_listen,
                vip,
                ingress_listen,
                ingress_tls_listen,
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
        Command::Restore {
            file,
            data_dir,
            advertise,
            unlock_key_file,
        } => daemon::restore(
            file.as_deref(),
            &data_dir,
            advertise,
            unlock_key_file.as_deref(),
        ),
        Command::Image {
            command:
                ImageCommand::Load {
                    file,
                    name,
                    data_dir,
                    zfs_root,
                },
        } => daemon::load_image(&file, name.as_deref(), &data_dir, &zfs_root).await,
        Command::Unlock { key_file } => cli::unlock(&args.server, key_file.as_deref()).await,
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
