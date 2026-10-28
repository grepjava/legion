//! The managers' floating address (DESIGN.md §3.8): a CARP virtual host on
//! a link the managers share. Any manager answers the API, so the address
//! needs only to be on one that runs; the leader advertises the lowest
//! skew and takes the address, sparing calls a forward, while followers
//! hold it only when the leader cannot (it is down, or a manager without
//! the address leads). On shutdown the address goes, so another takes it
//! at once.

use std::sync::Arc;
use std::time::Duration;

use legion_store::NodeId;
use legion_sys::{Carp, CarpState, Sys};
use tokio::sync::watch;

/// Skews the leader and the followers advertise with.
pub const LEADER_SKEW: u8 = 0;
pub const FOLLOWER_SKEW: u8 = 100;
/// How often the address is set again, should something have changed it.
const PERIOD: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct VipConfig {
    pub interface: String,
    pub vhid: u8,
    pub addr: std::net::Ipv4Addr,
    pub password: String,
}

/// Holds the address for as long as the manager runs.
pub async fn run(
    sys: Arc<dyn Sys>,
    config: VipConfig,
    mut metrics: watch::Receiver<openraft::RaftMetrics<NodeId, openraft::BasicNode>>,
    id: NodeId,
    mut closing: watch::Receiver<bool>,
) {
    let mut failing = false;
    loop {
        let leading = metrics.borrow().current_leader == Some(id);
        match set(&sys, &config, leading).await {
            Ok(state) => {
                if failing {
                    tracing::info!("the floating address is set again");
                    failing = false;
                }
                tracing::debug!(leading, state = state.as_str(), "floating address");
            }
            Err(e) => {
                if !failing {
                    tracing::warn!(addr = %config.addr, "setting the floating address: {e}");
                    failing = true;
                }
            }
        }
        tokio::select! {
            changed = metrics.changed() => if changed.is_err() { break },
            _ = tokio::time::sleep(PERIOD) => {}
            _ = closing.wait_for(|c| *c) => break,
        }
    }
    let (sys2, config2) = (sys.clone(), config.clone());
    match tokio::task::spawn_blocking(move || sys2.carp_remove(&config2.interface, config2.addr))
        .await
    {
        Ok(Ok(())) => tracing::info!(addr = %config.addr, "floating address released"),
        Ok(Err(e)) => tracing::warn!("releasing the floating address: {e}"),
        Err(e) => tracing::warn!("releasing the floating address: {e}"),
    }
}

/// Advertises the skew for the manager's role; the leader takes the
/// address. Returns the virtual host's state.
async fn set(sys: &Arc<dyn Sys>, config: &VipConfig, leading: bool) -> Result<CarpState, String> {
    let (sys, config) = (sys.clone(), config.clone());
    tokio::task::spawn_blocking(move || {
        let carp = Carp {
            vhid: config.vhid,
            addr: config.addr,
            password: config.password.clone(),
            advskew: if leading { LEADER_SKEW } else { FOLLOWER_SKEW },
        };
        let state = sys
            .carp_state(&config.interface, config.vhid)
            .map_err(|e| e.to_string())?;
        // A follower that holds the address keeps it until the leader's
        // advertisements take it, so it is never left without a holder.
        let force = (leading && state != Some(CarpState::Master)).then_some(CarpState::Master);
        sys.carp_set(&config.interface, &carp, force)
            .map_err(|e| e.to_string())?;
        sys.carp_state(&config.interface, config.vhid)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| "the virtual host did not appear".to_owned())
    })
    .await
    .map_err(|e| e.to_string())?
}

#[cfg(test)]
mod tests {
    use super::*;
    use legion_sys::FakeSys;

    fn config() -> VipConfig {
        VipConfig {
            interface: "re0".into(),
            vhid: 9,
            addr: "192.0.2.100".parse().unwrap(),
            password: "pw".into(),
        }
    }

    #[tokio::test]
    async fn the_leader_takes_the_address_and_followers_stand_by() {
        let fake = Arc::new(FakeSys::new());
        let sys: Arc<dyn Sys> = fake.clone();
        assert_eq!(
            set(&sys, &config(), false).await.unwrap(),
            CarpState::Backup
        );
        let (carp, _) = fake.carp("re0", 9).unwrap();
        assert_eq!(
            (carp.advskew, carp.password.as_str()),
            (FOLLOWER_SKEW, "pw")
        );

        assert_eq!(set(&sys, &config(), true).await.unwrap(), CarpState::Master);
        assert_eq!(fake.carp("re0", 9).unwrap().0.advskew, LEADER_SKEW);

        // No longer leading, it keeps the address until another takes it.
        assert_eq!(
            set(&sys, &config(), false).await.unwrap(),
            CarpState::Master
        );
        assert_eq!(fake.carp("re0", 9).unwrap().0.advskew, FOLLOWER_SKEW);
        fake.set_carp_state("re0", 9, CarpState::Backup);
        assert_eq!(
            set(&sys, &config(), false).await.unwrap(),
            CarpState::Backup
        );
    }
}
