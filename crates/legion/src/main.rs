//! `legion`: the daemon (`legion daemon`) and the CLI that talks to it.

mod autolock;
mod ca;
mod cli;
mod daemon;
mod doctor;
mod metrics;
mod pki;
mod resolver;
mod session;
mod top;
mod volume_backup;

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
    /// The node resolver, run by the agent (see `resolver.rs`).
    #[command(hide = true)]
    NodeResolver {
        #[arg(long)]
        listen: std::net::SocketAddr,
        #[arg(long)]
        zone: PathBuf,
        #[arg(long)]
        pid: PathBuf,
    },
    /// Run the node daemon: the manager, and on FreeBSD the agent.
    Daemon {
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// Runtime files: tasks' secrets and configs.
        #[arg(long, default_value = "/var/run/legion")]
        run_dir: PathBuf,
        /// Where a manager serves the API. Every call but `Join` (which
        /// takes a token) needs a cluster certificate.
        #[arg(long, default_value = "0.0.0.0:7470")]
        listen: SocketAddr,
        /// Managers: the address other nodes reach this one at; defaults to
        /// --listen, or with --listen on every address to this host's
        /// address on its default route. Workers: where other nodes'
        /// WireGuard reaches this one; defaults to the address it reaches
        /// its managers from.
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
        /// A registry, `host[:port]`, reached over plain HTTP rather than
        /// HTTPS (repeatable): by managers resolving tags, and by this node
        /// pulling. For internal registries without TLS: anyone on the
        /// path can read and change what is pulled. Set it on every node.
        #[arg(long = "insecure-registry", value_parser = registry_host)]
        insecure_registries: Vec<String>,
        /// Managers: how many days audit events are kept.
        #[arg(long, default_value_t = 30)]
        audit_retention_days: u64,
        /// Managers: the most audit events kept; the oldest go first.
        #[arg(long, default_value_t = 100_000)]
        audit_max_events: u64,
        /// Workers: a manager to call, `host:port` (repeatable). Defaults to
        /// the managers recorded at join and learned since.
        #[arg(long = "manager")]
        managers: Vec<String>,
        /// Serve Prometheus metrics at `http://<addr>/metrics`, without TLS
        /// or authentication: the names of every namespace's services and
        /// tasks show there.
        #[arg(long)]
        metrics_listen: Option<SocketAddr>,
        /// Managers: serve the web console here over HTTPS, e.g.
        /// `0.0.0.0:7480` (DESIGN.md §3.9). Users sign in with `legion
        /// console login`.
        #[arg(long)]
        console_listen: Option<SocketAddr>,
        /// The console's certificate (PEM, its chain included) for browsers
        /// to trust; the manager's own, of the cluster CA, without it.
        #[arg(long, requires = "console_key")]
        console_cert: Option<PathBuf>,
        /// The key of --console-cert (PEM).
        #[arg(long, requires = "console_cert")]
        console_key: Option<PathBuf>,
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
        /// Run Linux images too (where an image offers no FreeBSD one),
        /// through FreeBSD's Linux ABI: loads linux64 and the file systems
        /// Linux programs expect. Placement can ask for such nodes with
        /// `node.platform.linux == true`.
        #[arg(long)]
        linux: bool,
        /// This node is behind a NAT that no other node gets through (one
        /// that maps each destination apart, say): it reaches other nodes,
        /// but they reach it only once it has, and two such nodes never
        /// reach each other directly. Their traffic goes through a relay
        /// (`--mesh-relay`) instead.
        #[arg(long)]
        mesh_nat: bool,
        /// This node relays WireGuard traffic between nodes behind NAT
        /// (`--mesh-nat`). Give it to a node every node reaches: a manager
        /// with a public address, say.
        #[arg(long)]
        mesh_relay: bool,
        /// The interface load balancer addresses are held on (services'
        /// `endpoint.loadBalancer`): the one on the network they are
        /// addresses of. The default route's if not given.
        #[arg(long)]
        load_balancer_interface: Option<String>,
        /// Where the node's volume plugins are: executables, by the name
        /// a Volume's `plugin.name` gives [default:
        /// /usr/local/libexec/legion/volume].
        #[arg(long)]
        volume_plugins: Option<PathBuf>,
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
        /// Managers only: the address other managers reach this one at
        /// [default: the address this host reaches --manager from, port
        /// 7470].
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
    /// The cluster CA. `rotate` makes a new one: every manager signs with
    /// it at once and every node gets a certificate of it, the old CA
    /// trusted until all have, then no more. `status` says where the
    /// cluster is: the CA that issues, and the CA of each manager's and
    /// node's certificate. Users' certificates and copies of admin.pem
    /// issued before are refused once the rotation ends: issue them again.
    Ca {
        #[arg(value_parser = ["rotate", "status"])]
        action: String,
    },
    /// The web console (DESIGN.md §3.9). `login` prints a link that signs
    /// you in as this CLI's identity: good once, for 60 s.
    Console {
        #[arg(value_parser = ["login"])]
        action: String,
        /// The console's address, as a browser reaches it [default: the
        /// --server host, port 7480].
        #[arg(long)]
        url: Option<String>,
    },
    /// Give a locked manager on this host its unlock key, read from
    /// --key-file or else from stdin.
    Unlock {
        #[arg(long)]
        key_file: Option<PathBuf>,
    },
    /// Check whether this host can run Legion, and say how to fix what it
    /// can't. Needs no daemon and changes nothing; run it as root, as pf's
    /// rules can't be read otherwise. Exits nonzero if a requirement is
    /// missing or could not be read.
    Doctor {
        /// The dataset the daemon would use.
        #[arg(long)]
        zfs_root: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Save every object of the cluster, read at one version, to a file.
    /// It holds Secrets and join tokens: keep it as safe as the managers'
    /// disks. The managers' keys are not in it; keep `<data-dir>/pki` too.
    /// Nor is any volume's data: back each up with `legion volume backup`.
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
    /// Nodes: their availability and labels.
    Node {
        #[command(subcommand)]
        command: NodeCommand,
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
    /// The audit trail, oldest first: who changed what, opened which
    /// sessions, and what was refused (the cluster admin's only). Events
    /// name objects, never what they hold.
    Audit {
        /// Only events this recent, e.g. `30m`, `24h`.
        #[arg(long)]
        since: Option<legion_api::quantity::Duration>,
        /// Only this caller's, by the name in their certificate.
        #[arg(long)]
        actor: Option<String>,
        /// Only this action: apply, delete, exec, logs, port-forward,
        /// init, join, join-token, issue-user, volume-backup,
        /// volume-restore, autolock.
        #[arg(long)]
        action: Option<String>,
        /// Only objects in this namespace.
        #[arg(short, long)]
        namespace: Option<String>,
        /// Only objects of this kind.
        #[arg(long)]
        kind: Option<String>,
        /// Only refusals.
        #[arg(long)]
        denied: bool,
        /// At most this many events.
        #[arg(long, default_value_t = 1000)]
        limit: u32,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
    },
    /// The cluster live in the terminal: nodes, services and Jobs, tasks,
    /// what they use, and what changes.
    Top {
        /// Only this namespace (default: every one you may read).
        #[arg(short, long)]
        namespace: Option<String>,
        /// Print nodes, services and Jobs, and tasks once, as text, and
        /// exit.
        #[arg(long)]
        once: bool,
    },
    /// What may reach what among a namespace's services, as the nodes
    /// enforce it: its isolation and NetworkPolicies, ingress and egress.
    /// `(other namespaces)` stands for the tasks of every other namespace.
    Reach {
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// What to act on now: crash-looping tasks, nodes down or suspect,
    /// lost Volumes, the CA near expiry, Raft without a leader or quorum.
    /// Any manager answers, from what it holds, one cut off from the
    /// leader too. With --watch, the list again each time it changes.
    Alerts {
        /// Only this namespace's.
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long)]
        watch: bool,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
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
pub enum NodeCommand {
    /// Move the node's tasks elsewhere (and stateful tasks' Volumes with
    /// them), and place no new ones there.
    Drain {
        node: String,
        /// Wait until nothing runs on the node and none of its data is
        /// moving, printing what is left as it changes. Exits 0 once the
        /// drain completes, 2 if --timeout passes first, and 1 if the drain
        /// is cancelled meanwhile (`legion node activate`).
        #[arg(long)]
        wait: bool,
        /// With --wait: give up after this many seconds.
        #[arg(long, requires = "wait")]
        timeout: Option<u64>,
        /// With --wait: `json` prints only the drain's last status, as the
        /// node's `status.drain` has it, with the node's name.
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table, requires = "wait")]
        output: cli::Format,
    },
    /// Keep the node's tasks but place no new ones there.
    Pause { node: String },
    /// Undo a drain or a pause.
    Activate { node: String },
    /// Make a worker a manager, in place: its daemon takes it up within a
    /// heartbeat or two and starts again as a manager, its tasks running
    /// on. It joins Raft as a learner and votes once caught up.
    Promote { node: String },
    /// Make a manager a worker, in place: it leaves the Raft membership
    /// (never the last voter), gives up the CA key and its store, and its
    /// daemon starts again as a worker, its tasks running on.
    Demote { node: String },
    /// Set labels (`key=value`) or remove them (`key-`), for placement
    /// constraints and preferences; `legion.ingress=true` marks a node
    /// that serves HTTPS routes (see the README).
    Label {
        node: String,
        #[arg(required = true)]
        labels: Vec<String>,
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
    /// Back up a Volume's data to a file: a snapshot taken as its tasks
    /// run, so crash-consistent, as after a power cut. The cluster's
    /// backup (`legion backup`) holds no volume's data.
    Backup {
        volume: String,
        #[arg(short, long)]
        output: PathBuf,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Restore a Volume from a backup onto NODE, which must not have its
    /// data. The Volume is made once all of it is there, and must not
    /// exist: delete it first, or give another --name. With --replace, it
    /// restores into the Volume of that name lost with its node (removed
    /// with the data on it): the Volume and what uses it are kept, and its
    /// tasks start on NODE.
    Restore {
        file: PathBuf,
        #[arg(long)]
        node: String,
        /// The namespace to restore into (default: the backup's).
        #[arg(short, long)]
        namespace: Option<String>,
        /// The Volume's name (default: the backup's).
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        replace: bool,
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

/// A registry as `--insecure-registry` takes it: `host[:port]`, no scheme
/// or path, as image references name registries.
fn registry_host(s: &str) -> Result<String, String> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() || s.contains("://") || s.contains('/') {
        return Err(format!(
            "{s:?}: a registry is host[:port], as in an image reference"
        ));
    }
    Ok(s)
}

async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::NodeResolver { listen, zone, pid } => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| "info".into()),
                )
                .init();
            resolver::run(listen, zone, &pid).await
        }
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
            console_listen,
            console_cert,
            console_key,
            vip,
            vip_interface,
            vip_vhid,
            vip_password_file,
            ingress_listen,
            ingress_tls_listen,
            linux,
            mesh_nat,
            mesh_relay,
            load_balancer_interface,
            volume_plugins,
            insecure_registries,
            audit_retention_days,
            audit_max_events,
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
            let read = |p: &PathBuf| {
                std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("reading {}: {e}", p.display()))
            };
            let console = match console_listen {
                Some(listen) => Some(legion_manager::Console {
                    listen,
                    cert_pem: console_cert.as_ref().map(read).transpose()?,
                    key_pem: console_key.as_ref().map(read).transpose()?,
                }),
                None => None,
            };
            daemon::run(daemon::Options {
                console,
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
                linux,
                mesh_nat,
                mesh_relay,
                load_balancer_interface,
                volume_plugins,
                insecure_registries,
                audit: legion_manager::AuditRetention {
                    age: std::time::Duration::from_secs(audit_retention_days * 24 * 3600),
                    events: audit_max_events,
                    ..Default::default()
                },
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
        Command::Doctor { zfs_root, json } => {
            doctor::run(&legion_sys::Shell::new(), zfs_root.as_deref(), json)
        }
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

#[cfg(test)]
mod tests {
    use super::registry_host;

    #[test]
    fn an_insecure_registry_is_a_host_and_port() {
        assert_eq!(
            registry_host("Registry.lan:5000").as_deref(),
            Ok("registry.lan:5000")
        );
        assert_eq!(registry_host("10.78.0.1").as_deref(), Ok("10.78.0.1"));
        for bad in ["", "http://r.lan:5000", "r.lan:5000/v2", " "] {
            assert!(registry_host(bad).is_err(), "{bad:?}");
        }
    }
}
