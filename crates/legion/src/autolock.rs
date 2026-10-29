//! Autolock on a manager (DESIGN.md §9): while its private keys are sealed
//! it waits for the unlock key, and once running it keeps them sealed with
//! the cluster's current unlock key, or in the clear without one.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_proto::v1::unlock_server::{Unlock, UnlockServer};
use legion_proto::v1::{UnlockRequest, UnlockResponse};
use legion_store::{Store, StoreKey};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

use crate::pki::Pki;

/// How often a manager checks the unlock key it should seal with.
const PERIOD: Duration = Duration::from_secs(5);

struct Unlocker {
    data_dir: PathBuf,
    unlocked: mpsc::Sender<StoreKey>,
}

#[tonic::async_trait]
impl Unlock for Unlocker {
    async fn unlock(
        &self,
        req: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        // The key travels in the clear: only from this host.
        if !req.remote_addr().is_some_and(|a| a.ip().is_loopback()) {
            return Err(Status::permission_denied(
                "unlock this manager from its own host",
            ));
        }
        let key = legion_manager::autolock::parse(&req.into_inner().key)
            .map_err(Status::invalid_argument)?;
        Pki::new(&self.data_dir)
            .unlock(key.clone())
            .map_err(|e| Status::permission_denied(e.to_string()))?;
        let _ = self.unlocked.send(key).await;
        Ok(Response::new(UnlockResponse {}))
    }
}

/// Serves `Unlock` on `listen` until a key opens this manager's sealed
/// keys; returns it.
pub async fn wait(listen: SocketAddr, data_dir: &Path) -> anyhow::Result<StoreKey> {
    let (tx, mut rx) = mpsc::channel(1);
    let unlocker = Unlocker {
        data_dir: data_dir.to_owned(),
        unlocked: tx,
    };
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::warn!(%listen, "locked: waiting for `legion unlock` on this host");
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(UnlockServer::new(unlocker))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = done_rx.await;
                },
            ),
    );
    let key = tokio::select! {
        key = rx.recv() => key.ok_or_else(|| anyhow::anyhow!("the unlock server stopped"))?,
        () = crate::shutdown_signal() => anyhow::bail!("stopped while locked"),
    };
    let _ = done_tx.send(());
    let _ = server.await;
    tracing::info!("unlocked");
    Ok(key)
}

/// Keeps `pki`'s private keys sealed with the unlock key the store holds,
/// or in the clear when it holds none, for as long as the task runs.
pub async fn follow(store: Store, pki: Arc<Mutex<Pki>>) {
    loop {
        match legion_manager::autolock::current(&store) {
            Ok(now) => {
                let wanted = now.map(|(key, _)| key);
                let mut pki = pki.lock().unwrap_or_else(|e| e.into_inner());
                if wanted.as_ref() != pki.lock() {
                    let sealing = wanted.is_some();
                    match pki.reseal(wanted) {
                        Ok(()) if sealing => tracing::info!("autolock: private keys sealed"),
                        Ok(()) => tracing::info!("autolock off: private keys in the clear"),
                        Err(e) => tracing::warn!("autolock: {e:#}"),
                    }
                }
            }
            Err(e) => tracing::warn!("autolock: reading the unlock key: {e}"),
        }
        tokio::time::sleep(PERIOD).await;
    }
}
