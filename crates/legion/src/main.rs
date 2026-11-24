//! `legion`: the daemon (`legion daemon`) and the CLI that talks to it.

mod activity;
mod app;
mod autolock;
mod ca;
mod cli;
mod daemon;
mod doctor;
mod local;
mod metrics;
mod pki;
mod resolver;
mod session;
mod top;
mod topology;
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
        /// A witness (docs/HA.md): a manager that votes, so a quorum may
        /// count it, but runs no tasks and never leads. Implied when the
        /// node joined with `legion join --witness`.
        #[arg(long)]
        witness: bool,
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
        /// Where the node's replication plugins are: executables, by the
        /// name a Service's `replication.plugin` gives, mounted read-only
        /// into the containers that run them [default:
        /// /usr/local/libexec/legion/replication].
        #[arg(long)]
        replication_plugins: Option<PathBuf>,
        /// The file of iSCSI targets this node writes for the LUNs it
        /// provides (`iscsi: { node }`), which ctld's own UCL
        /// configuration includes (`.include "FILE"`); without it the node
        /// provides none. ctld is reloaded when it changes.
        #[arg(long)]
        iscsi_targets: Option<PathBuf>,
        /// The portal group, of ctld's configuration, its targets are on.
        #[arg(long, default_value = "legion")]
        iscsi_portal_group: String,
        /// `host[:port]` the nodes reach that portal group at [default:
        /// the node's address, port 3260].
        #[arg(long)]
        iscsi_portal: Option<String>,
        /// The program a builder node (labelled `legion.builder=true`) builds
        /// images with, for Builds [default: this legion, as `legion local`].
        #[arg(long)]
        builder: Option<String>,
        /// An argument put before each of the builder's commands, such as
        /// `--state-dir=/var/db/legion-builds`; may be given again.
        #[arg(long = "builder-arg", allow_hyphen_values = true)]
        builder_args: Vec<String>,
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
        /// Join as a witness (docs/HA.md), with a manager token: it votes
        /// in the managers' quorum but runs no tasks and never leads, so a
        /// small machine (no ZFS, no pf) makes two managers three.
        #[arg(long)]
        witness: bool,
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
    /// you in as this CLI's identity: good once, for 60 s. `settings`
    /// prints the console's settings as YAML; `configure -f FILE` sets them
    /// (the cluster admin only): a banner every page shows, side-bar links,
    /// the namespace users start in and how long a session lasts, e.g.
    ///
    ///   banner: { text: Production, level: danger }   # info, warning
    ///   links: [{ label: Runbook, url: "https://wiki.example/run" }]
    ///   defaultNamespace: shop
    ///   session: 8h                                  # 5m to 168h
    ///
    /// `oidc` shows single sign-on's provider; `oidc -f FILE` sets it, the
    /// client secret read from the file and never shown again (left out,
    /// the one set stays); `oidc --remove` turns it off:
    ///
    ///   issuer: https://idp.example
    ///   clientId: legion
    ///   clientSecret: ...
    ///   consoleUrl: https://console.example:7480   # allow <it>/api/oidc/callback
    ///   usernameClaim: email      # the user is oidc:<it>; groupsClaim: groups
    Console {
        #[arg(value_parser = ["login", "settings", "configure", "oidc"])]
        action: String,
        /// The console's address, as a browser reaches it [default: the
        /// --server host, port 7480].
        #[arg(long)]
        url: Option<String>,
        /// For `configure` and `oidc`: the settings, as YAML; `-` for stdin.
        #[arg(short, long)]
        file: Option<PathBuf>,
        /// For `oidc`: turn single sign-on off.
        #[arg(long)]
        remove: bool,
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
    /// missing or could not be read. `--fix pf` puts pf right instead:
    /// adds the lines calling Legion's anchors to /etc/pf.conf (keeping a
    /// dated copy, checked by pfctl before it is loaded), sets
    /// pf_enable=YES and loads it, after saying what it changes and asking.
    Doctor {
        /// The dataset the daemon would use.
        #[arg(long)]
        zfs_root: Option<String>,
        #[arg(long)]
        json: bool,
        /// Put this right on the host, rather than only check it.
        #[arg(long, value_parser = ["pf"])]
        fix: Option<String>,
        /// With --fix: change it without asking.
        #[arg(long, short)]
        yes: bool,
        /// With --fix pf: the file to change.
        #[arg(long, default_value = "/etc/pf.conf")]
        pf_conf: String,
    },
    /// The cluster's single points of failure (docs/HA.md): too few
    /// managers, one node running every task, services with one replica or
    /// all on one node, Volumes whose data is on one node, each with how to
    /// put it right. Exits nonzero if any is a risk (losing one machine
    /// stops something, or loses data).
    Ha {
        /// Only this namespace's (the cluster's own findings too).
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// What `legion doctor` finds on every node of the cluster, or on
    /// `--node`, asked of their agents (the cluster admin's). Exits nonzero
    /// unless every node asked is ready.
    Diagnose {
        #[arg(long)]
        node: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// A file to send with a report of trouble (the cluster admin's): the
    /// cluster's objects without its Secrets, and with environment values
    /// and what else could be secret masked; its alerts, the last hour's
    /// events and each node's diagnosis. Read it before sending it.
    SupportBundle {
        /// Where to write it; `legion-support-<time>.json` here by default.
        #[arg(short, long)]
        output: Option<PathBuf>,
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
    /// Delete one object. A node's removal and a Service's deletion are
    /// checked first (as --dry-run says): refused if the check finds it
    /// would leave work no node takes or lose data on a node still up.
    Delete {
        kind: String,
        name: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// For a node or a Service: say what removing it would do (the
        /// data that goes with a node, the Volumes a Service leaves, what
        /// no node would take) and change nothing: exits 1 if anything
        /// would block it.
        #[arg(long)]
        dry_run: bool,
        /// Go ahead even though the check made first finds it would not
        /// complete.
        #[arg(long, conflicts_with = "dry_run")]
        force: bool,
    },
    /// Deploy, convert or remove compose stacks.
    Stack {
        #[command(subcommand)]
        command: StackCommand,
    },
    /// Ready-to-deploy applications from the daemonless catalog (FreeBSD
    /// images, read from the catalog when asked, nothing of it kept):
    /// listed, shown with what they ask for, deployed as a stack.
    App {
        #[command(subcommand)]
        command: AppCommand,
    },
    /// Images, builds and containers on this host alone, without a cluster.
    #[command(name = "local")]
    Local(local::Cli),
    /// Build a FreeBSD OCI image from a Containerfile.
    Build(local::BuildCli),
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
    /// Replace a service's tasks with new ones on the same spec, as an
    /// update replaces them, under its update config: its pod's
    /// `restartedAt` set to now.
    Restart {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Stop a service: its tasks go, and its spec (the replica count
    /// included) stays for `legion start`.
    Stop {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Start a stopped service again.
    Start {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Pause a service: its tasks stay where they are, frozen (a jail's
    /// processes stopped, a VM's virtual CPUs held), out of its endpoints
    /// and their probes held, until `legion resume`.
    Pause {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Resume a paused service: its tasks go on where they were.
    Resume {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// CronJobs: a run now.
    Cronjob {
        #[command(subcommand)]
        command: CronJobCommand,
    },
    /// A replicated database's members (DESIGN.md §6.9): where each stands,
    /// and a new primary by switchover or failover.
    Replication {
        #[command(subcommand)]
        command: ReplicationCommand,
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
    /// Each namespace's quota and what it uses of it now: its placed
    /// tasks' requests and its Volumes.
    Quota {
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
    },
    /// What tasks used over the last hour, as the leader sampled it every
    /// 15 s, or further back with --range: now, on average and at most,
    /// per task; every sample with -o json or yaml.
    Usage {
        /// Only this task (in --namespace).
        task: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        /// How far back, up to `30d`: past an hour, from what the managers
        /// keep, a sample a minute within a day and every 15 minutes
        /// beyond, e.g. `24h`, `7d`.
        #[arg(long)]
        range: Option<legion_api::quantity::Duration>,
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
    /// The connections a namespace's tasks open or answer, as the nodes'
    /// pf keeps them: those open, or closed in about the last minute and a
    /// half, by what opened them, what answered, the port and how it was
    /// sent (`service`: to the service's address; `published`: to a port a
    /// node publishes). `(other namespaces)` stands for the tasks of every
    /// other namespace; `node/NAME` for a node itself (its probes, or a
    /// connection it forwards).
    Connections {
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// A namespace as the console's Topology page draws it, in text: each
    /// service with its ports, Routes, tasks and Volumes, what may reach it
    /// and the connections the nodes saw.
    Topology {
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// What happened to objects lately, oldest first, as the manager asked
    /// saw it (kept in its memory for an hour): a namespace's, or one
    /// object's (`service/web` with its tasks'; `node/n1`). `-w` follows.
    Events {
        /// `kind/name`, or a kind (`tasks`, `node`) for all of it.
        object: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long)]
        watch: bool,
    },
    /// Long operations, as the leader follows them: drains, Volume moves,
    /// restores and replicas' whole copies, those under way and those
    /// ended in the last hour, each with its stage (a move: preparing,
    /// full-copy, stopping, switch-over, cleanup), what it has sent, how
    /// long it has run, who asked (for the cluster admin) and why it waits
    /// or how it ended. `-w` follows, a line per change. With an ID (or
    /// the start of one, or the name of what it is done to), that one's
    /// stages and all it said.
    Activity {
        id: Option<String>,
        #[arg(short, long)]
        watch: bool,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
    },
    /// A namespace's load balancer addresses and which node holds each:
    /// the nodes that may, by CARP skew (the lowest meant to be master),
    /// each with the CARP state it last reported.
    #[command(name = "load-balancers", alias = "lb")]
    LoadBalancers {
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// What to act on now: crash-looping tasks, nodes down or suspect,
    /// lost Volumes, the CA near expiry, Raft without a leader or quorum.
    /// Any manager answers, from what it holds, one cut off from the
    /// leader too. With --watch, the list again each time it changes.
    /// The leader sends them to the AlertReceivers that choose them.
    Alerts {
        #[command(subcommand)]
        command: Option<AlertsCommand>,
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
    /// Print a Build's log: as it is written with --follow while the
    /// build runs, else what is kept of it.
    BuildLogs {
        build: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Keep printing until the build ends.
        #[arg(short, long)]
        follow: bool,
    },
    /// Search a Service's or Job's logs across its tasks: the lines from
    /// --since (and before --until), holding TEXT if given, oldest first,
    /// each with its time and task.
    LogsSearch {
        service: String,
        /// Lines holding this, without regard to case.
        text: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        /// How far back, e.g. `30m`, `24h`.
        #[arg(long, default_value = "1h")]
        since: legion_api::quantity::Duration,
        /// Only lines older than this, e.g. `5m`.
        #[arg(long)]
        until: Option<legion_api::quantity::Duration>,
        /// At most this many lines, the newest.
        #[arg(long, default_value_t = 1000)]
        limit: usize,
    },
    /// Run a command in a task's container; exits with its exit code. A
    /// service name picks one of its running tasks.
    /// List a directory in a task's container (its Volumes' mounts
    /// included). Symbolic links are shown, never followed. Takes the
    /// rights a shell in it would.
    Files {
        task: String,
        /// Below the container's root.
        #[arg(default_value = "/")]
        path: String,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long)]
        container: Option<String>,
    },
    /// Copy a file out of a task's container or into it: `TASK:PATH LOCAL`
    /// or `LOCAL TASK:PATH`; `-` for standard output or input. One written
    /// into a container replaces what was at its path whole, or nothing.
    Cp {
        from: String,
        to: String,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(short, long)]
        container: Option<String>,
    },
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
    /// or not (`--zfs-root`), or stream it to a cluster node (`--node`).
    Load {
        file: PathBuf,
        /// Its name; by default the one the archive gives.
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "/var/db/legion")]
        data_dir: PathBuf,
        /// The daemon's `--zfs-root` (when loading directly on this host).
        #[arg(long, required_unless_present = "node")]
        zfs_root: Option<String>,
        /// Stream the archive over the cluster API onto NODE instead.
        #[arg(long)]
        node: Option<String>,
    },
    /// List cached images on ready nodes (or on `--node`).
    Ls {
        #[arg(long)]
        node: Option<String>,
        #[arg(short, long, value_enum, default_value_t = cli::Format::Table)]
        output: cli::Format,
    },
    /// Pre-pull an image onto `--node` (or every ready node when omitted).
    Pull {
        image: String,
        #[arg(long)]
        node: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Optional Secret holding `.dockerconfigjson` registry auth.
        #[arg(long)]
        secret: Option<String>,
    },
    /// Push a cached image from `--node` to a target registry (`ghcr.io/owner/repo:tag`).
    Push {
        image: String,
        target: String,
        #[arg(long)]
        node: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Optional Secret holding `.dockerconfigjson` registry auth.
        #[arg(long)]
        secret: Option<String>,
    },
    /// Remove a cached image reference from `--node` (or every ready node).
    Rm {
        image: String,
        #[arg(long)]
        node: Option<String>,
    },
    /// Prune unused image layers and blobs on `--node` (or every ready node).
    Prune {
        #[arg(long)]
        node: Option<String>,
        /// Prune the build store on builders instead: the layers and cache
        /// no image there uses (`legion local prune`).
        #[arg(long)]
        builds: bool,
    },
    /// Audit a cached image on `--node`: its FreeBSD packages checked
    /// against FreeBSD's VuXML database (`pkg audit`), and what uses it.
    /// Exits nonzero when a package has a known vulnerability.
    Scan {
        image: String,
        #[arg(long)]
        node: String,
        /// Every package too, as JSON (an SBOM, with what is known of it).
        #[arg(long)]
        json: bool,
    },
    /// Export a cached image from `--node` to a local OCI/Docker `.tar` archive.
    Export {
        image: String,
        #[arg(long)]
        node: String,
        #[arg(short, long)]
        output: PathBuf,
        #[arg(long)]
        tag: Option<String>,
    },
    /// Search the FreeBSD jail image catalog and Docker Hub.
    Search {
        query: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: u32,
        /// Namespace holding `--secret` (for ghcr.io search).
        #[arg(short, long)]
        namespace: Option<String>,
        /// Docker-config Secret holding a GitHub PAT (`read:packages`) for ghcr.io.
        #[arg(long)]
        secret: Option<String>,
    },
    /// Show the SBOM attested for a registry image (`cosign attest
    /// --type spdxjson` or `cyclonedx`): its packages, and whether a key of
    /// an ImagePolicy covering the image signed it. Exits nonzero when it
    /// has none.
    Sbom {
        image: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// A Secret of the namespace holding the registry's login.
        #[arg(long)]
        secret: Option<String>,
        /// The packages as JSON.
        #[arg(long)]
        json: bool,
    },
    /// Inspect a remote registry image (tags, platforms, digest, cosign
    /// signature and attestations).
    Inspect {
        image: String,
        #[arg(short, long)]
        namespace: Option<String>,
        #[arg(long)]
        secret: Option<String>,
        /// Read NODE's cached copy instead of the registry (an image built
        /// or loaded there).
        #[arg(long)]
        node: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum AlertsCommand {
    /// Send a test through the AlertReceiver RECEIVER now, as it sends
    /// alerts: exits nonzero, saying why, if it was not taken.
    Test { receiver: String },
}

#[derive(Subcommand)]
pub enum NodeCommand {
    /// Move the node's tasks elsewhere (and stateful tasks' Volumes with
    /// them), and place no new ones there. Checked first (as --dry-run
    /// says): refused if it would not complete.
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
        /// Say what it would do (what moves, and its size; what no node
        /// would take; what a disruption budget holds back) and change
        /// nothing: exits 1 if anything would block it.
        #[arg(long, conflicts_with = "wait")]
        dry_run: bool,
        /// Go ahead even though the check made first finds it would not
        /// complete.
        #[arg(long, conflicts_with = "dry_run")]
        force: bool,
    },
    /// Keep the node's tasks but place no new ones there.
    Pause { node: String },
    /// Undo a drain or a pause.
    Activate { node: String },
    /// Make a worker a manager, in place: its daemon takes it up within a
    /// heartbeat or two and starts again as a manager, its tasks running
    /// on. It joins Raft as a learner and votes once caught up.
    Promote { node: String },
    /// Hand leadership to the manager NODE (not a witness), with nothing
    /// brought down: the leader goes quiet until the others' leases on it
    /// run out (about 10 s, when nothing is placed or changed; running
    /// tasks carry on), then NODE stands and is elected. Before upgrading
    /// or rebooting the leader's machine, say.
    Lead { node: String },
    /// Make a manager a worker, in place: it leaves the Raft membership
    /// (never the last voter, nor the last that may lead), gives up the CA
    /// key and its store, and its daemon starts again as a worker, its
    /// tasks running on. Asks the leader first what it leaves the managers
    /// with (an election if it leads, fewer than three voters).
    Demote {
        node: String,
        /// Print what it would do and change nothing; exits 1 if it blocks.
        #[arg(long)]
        dry_run: bool,
        /// Demote even if the check finds it would block.
        #[arg(long)]
        force: bool,
    },
    /// Set labels (`key=value`) or remove them (`key-`), for placement
    /// constraints and preferences; `legion.ingress=true` marks a node
    /// that serves HTTPS routes (see the README).
    Label {
        node: String,
        #[arg(required = true)]
        labels: Vec<String>,
    },
    /// Set which of `legion doctor`'s advised checks (`forwarding`,
    /// `racct`, `bridge.filtering`…) the node needs met to take new tasks,
    /// as it needs its required ones; none given, none. A node whose host
    /// lacks what it needs keeps what runs there (drain it to move that).
    Require { node: String, checks: Vec<String> },
    /// List the ZFS pools of NODE, or of every node: health, how full,
    /// errors, the last scrub (as each node's agent last read them).
    Pools { node: Option<String> },
    /// Take NODE's pools' errors as they stand as seen: `pool-errors` is
    /// raised again only for more. Clear them on the host with `zpool
    /// clear` once dealt with, and this starts over from none.
    ErrorsSeen { node: String },
    /// Have NODE's agent scrub its pools every DAYS days, and hold them to
    /// it (`scrub-overdue`); --never stops it.
    Scrub {
        node: String,
        #[arg(long, value_name = "DAYS", required_unless_present = "never", value_parser = clap::value_parser!(u32).range(1..))]
        every: Option<u32>,
        #[arg(long, conflicts_with = "every")]
        never: bool,
    },
}

#[derive(Subcommand)]
pub enum CronJobCommand {
    /// Run CRONJOB now, outside its schedule: the Job its schedule would
    /// make this minute, kept in its history. Its suspension and
    /// concurrency policy are for its schedule; a second run within the
    /// minute is refused.
    Run {
        cronjob: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum ReplicationCommand {
    /// The members of SERVICE: each one's role, position and lag, the
    /// primary Legion names, and any change of primary under way.
    Status {
        service: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Planned: the primary stops cleanly, and once the replica TO (or the
    /// one furthest ahead) holds all it wrote, that one is promoted.
    Switchover {
        service: String,
        #[arg(long)]
        to: Option<String>,
        /// Promote without waiting for it to catch up.
        #[arg(long)]
        force: bool,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// The primary is lost (`failover: manual`): promote TO once the old
    /// primary is fenced.
    Failover {
        service: String,
        #[arg(long)]
        to: String,
        /// Pass over the lag and synchronous checks: writes the others
        /// confirmed may be lost.
        #[arg(long)]
        force: bool,
        #[arg(short, long)]
        namespace: Option<String>,
    },
}

#[derive(Subcommand)]
pub enum VolumeCommand {
    /// Move a Volume's data to NODE (its `spec.node`): copied while its
    /// tasks run, which then stop and start again there once the rest has
    /// gone. It stays there, and returns there after a drain. Checked
    /// first (as --dry-run says): refused if it would stay where it is.
    Move {
        volume: String,
        node: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Say what it would do (what moves, and its size; what no node
        /// would take; what a disruption budget holds back) and change
        /// nothing: exits 1 if anything would block it.
        #[arg(long)]
        dry_run: bool,
        /// Go ahead even though the check made first finds it would not
        /// complete.
        #[arg(long, conflicts_with = "dry_run")]
        force: bool,
    },
    /// Hand a Volume whose node is down to its replica: it carries on from
    /// the replica's last copy of that node's data, on the replica's node,
    /// what was written there since being lost. Do it only once you know
    /// nothing runs on the node any more (or that it is gone): what still
    /// runs there writes to a copy no longer the Volume's, dropped once the
    /// node is back. `failover: automatic` does this on its own for tasks
    /// that stop where they are cut off (`onPartition: stop`).
    Promote {
        volume: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Go ahead without asking.
        #[arg(long)]
        yes: bool,
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
    /// tasks start on NODE. With --from TARGET, FILE is VOLUME@BACKUP, one
    /// of those `legion volume backups VOLUME` lists, restored from the
    /// BackupTarget TARGET.
    Restore {
        file: PathBuf,
        #[arg(long)]
        node: String,
        /// The BackupTarget the backup is on; FILE is then VOLUME@BACKUP.
        #[arg(long, conflicts_with = "replace")]
        from: Option<String>,
        /// The namespace to restore into (default: the backup's).
        #[arg(short, long)]
        namespace: Option<String>,
        /// The Volume's name (default: the backup's).
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        replace: bool,
    },
    /// List the backups of a Volume its BackupTarget holds (its
    /// `backup.target`, or --from), oldest first.
    Backups {
        volume: String,
        #[arg(long)]
        from: Option<String>,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// List ZFS snapshots of a Volume on the node holding its data.
    Snapshots {
        volume: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Take a ZFS snapshot of a Volume.
    Snapshot {
        volume: String,
        snapshot: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Roll a Volume's ZFS dataset back to a snapshot (only while no task
    /// uses it). Every later snapshot is destroyed with it, scheduled ones
    /// too, and the next replica copy or backup is whole: it says which
    /// and asks first.
    Rollback {
        volume: String,
        snapshot: String,
        #[arg(short, long)]
        namespace: Option<String>,
        /// Go ahead without asking.
        #[arg(long, short)]
        yes: bool,
    },
    /// Copy a snapshot's data into a new Volume NAME on the same node,
    /// leaving the Volume as it is: `legion volume clone db@legion-auto-1h-20261122T140000Z db-noon`.
    /// A copy, so the snapshot can go after.
    Clone {
        /// VOLUME@SNAPSHOT.
        source: String,
        name: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Delete a ZFS snapshot of a Volume.
    RmSnapshot {
        volume: String,
        snapshot: String,
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
pub enum AppCommand {
    /// The apps in the catalog.
    List {
        /// Only those of this category (Media, Utilities, …).
        #[arg(long)]
        category: Option<String>,
        /// The catalog to read.
        #[arg(long, default_value = legion_api::app::CATALOG)]
        catalog: String,
    },
    /// What an app is, and the variables it asks for.
    Show {
        app: String,
        #[arg(long, default_value = legion_api::app::CATALOG)]
        catalog: String,
    },
    /// Deploy an app as the stack NAME (default: the app's id) in
    /// namespace NAME, made if missing with what its containers ask to be
    /// allowed. Variables take their defaults unless set; a secret not set
    /// is made up; each storage variable names a Volume.
    Deploy {
        app: String,
        /// The stack's name.
        #[arg(long)]
        name: Option<String>,
        /// The namespace (default: the stack's name).
        #[arg(short, long)]
        namespace: Option<String>,
        /// A variable's value: --set WEB_PORT=8080; may be given again.
        #[arg(long = "set", value_name = "NAME=VALUE")]
        set: Vec<String>,
        /// The image's variant (its tag), as `legion app show` lists them.
        #[arg(long)]
        variant: Option<String>,
        /// Print the objects it would make, and make none.
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value = legion_api::app::CATALOG)]
        catalog: String,
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
    /// The stacks: each with its services, how many are ready, and whether
    /// they are stopped.
    Ls {
        /// One namespace's; every namespace you may read by default.
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Stop every service of a stack (`legion stop` on each).
    Stop {
        stack: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
    /// Start every service of a stack again.
    Start {
        stack: String,
        #[arg(short, long)]
        namespace: Option<String>,
    },
}

/// The jail runtime's supervisor (`legion jail-shim ...`, see
/// `legion_jail::shim`): run before anything else starts, as it forks and
/// must be single-threaded.
pub const JAIL_SHIM: &str = "jail-shim";

/// The program, and the arguments before each of its commands, a builder
/// node builds images with: `program` if given, else this legion's `local`.
fn builder_command(program: Option<String>, args: Vec<String>) -> Vec<String> {
    let program = program.map(|p| vec![p]).unwrap_or_else(|| {
        let exe = std::env::current_exe()
            .ok()
            .and_then(|p| p.into_os_string().into_string().ok())
            .unwrap_or_else(|| "legion".to_owned());
        vec![exe, "local".to_owned()]
    });
    program.into_iter().chain(args).collect()
}

fn main() -> anyhow::Result<()> {
    let argv: Vec<String> = std::env::args().collect();
    if argv.get(1).map(String::as_str) == Some(JAIL_SHIM) {
        std::process::exit(legion_jail::shim::main(&argv[2..]));
    }
    std::thread::Builder::new()
        .name("legion-main".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let args = Args::parse();
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(Box::pin(run(args)))
        })?
        .join()
        .unwrap_or_else(|p| std::panic::resume_unwind(p))
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

async fn run(args: Args) -> anyhow::Result<()> {
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
            witness,
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
            replication_plugins,
            iscsi_targets,
            iscsi_portal_group,
            iscsi_portal,
            builder,
            builder_args,
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
            Box::pin(daemon::run(daemon::Options {
                console,
                data_dir,
                run_dir,
                listen,
                advertise,
                agent: !no_agent,
                witness,
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
                replication_plugins,
                iscsi_targets,
                iscsi_portal_group,
                iscsi_portal,
                builder: builder_command(builder, builder_args),
                insecure_registries,
                audit: legion_manager::AuditRetention {
                    age: std::time::Duration::from_secs(audit_retention_days * 24 * 3600),
                    events: audit_max_events,
                    ..Default::default()
                },
            }))
            .await
        }
        Command::Join {
            token,
            manager,
            data_dir,
            advertise,
            node_name,
            witness,
        } => {
            daemon::join(
                &token,
                &manager,
                &data_dir,
                advertise.as_deref(),
                node_name,
                witness,
            )
            .await?;
            // A witness runs no pods: pf is not its business.
            if !witness {
                doctor::offer_pf_fix();
            }
            Ok(())
        }
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
                    zfs_root: Some(zfs_root),
                    node: None,
                },
        } => daemon::load_image(&file, name.as_deref(), &data_dir, &zfs_root).await,
        Command::App {
            command: AppCommand::List { category, catalog },
        } => app::list(&catalog, category.as_deref()).await,
        Command::App {
            command: AppCommand::Show { app, catalog },
        } => app::show(&catalog, &app).await,
        Command::App {
            command:
                AppCommand::Deploy {
                    app,
                    name,
                    namespace,
                    set,
                    variant,
                    dry_run: true,
                    catalog,
                },
        } => app::dry_run(&catalog, &app, &set, variant.as_deref(), name, namespace).await,
        Command::Doctor {
            fix: Some(_),
            yes,
            pf_conf,
            ..
        } => doctor::fix_pf(
            &legion_sys::Shell::new(),
            &pf_conf,
            &[doctor::LEGION_HOOKS],
            yes,
        ),
        Command::Doctor { zfs_root, json, .. } => {
            doctor::run(&legion_sys::Shell::new(), zfs_root.as_deref(), json)
        }
        Command::Local(cli) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
                )
                .with_target(false)
                .init();
            Box::pin(local::run_cli(&cli)).await
        }
        Command::Build(args) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
                )
                .with_target(false)
                .init();
            Box::pin(local::run_build(args)).await
        }
        Command::Unlock { key_file } => cli::unlock(&args.server, key_file.as_deref()).await,
        command => Box::pin(cli::run(&args.server, &args.identity, command)).await,
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
