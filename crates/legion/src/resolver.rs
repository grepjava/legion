//! `legion node-resolver`: the node resolver as a process of its own
//! (DESIGN.md §5.5), started by the agent and outliving the daemon as pods
//! do, so pods' name lookups go on through a daemon restart or upgrade.
//! It answers from the zone file the agent keeps, and stops once that file
//! is gone. A newer one binds the same address beside it, says so in the
//! pid file, and the agent then stops this one.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use legion_net::dns;

/// The pid file's line: `<pid> <version> <listen>`.
pub fn pid_line(listen: SocketAddr) -> String {
    format!(
        "{} {} {listen}\n",
        std::process::id(),
        env!("CARGO_PKG_VERSION")
    )
}

pub async fn run(listen: SocketAddr, zone: PathBuf, pid: &Path) -> anyhow::Result<()> {
    let (udp, tcp) = dns::bind_shared(listen)
        .with_context(|| format!("binding the node resolver to {listen}"))?;
    let upstream: Vec<SocketAddr> = std::fs::read_to_string("/etc/resolv.conf")
        .map(|t| dns::resolv_conf_servers(&t))
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s.ip() != listen.ip())
        .collect();
    tracing::info!(%listen, ?upstream, zone = %zone.display(), "node resolver");
    // Bound: the agent may now stop the one this replaces.
    let aside = pid.with_extension("new");
    std::fs::write(&aside, pid_line(listen)).context("writing the pid file")?;
    std::fs::rename(&aside, pid).context("writing the pid file")?;
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            () = dns::serve_file(udp, tcp, zone, upstream) => {}
            _ = term.recv() => tracing::info!("stopping, as asked"),
        }
    }
    #[cfg(not(unix))]
    dns::serve_file(udp, tcp, zone, upstream).await;
    Ok(())
}
