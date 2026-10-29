//! Containers' memory limits (DESIGN.md §6.3). rctl kills any process of a
//! container that alone goes past the container's limit; this keeps the
//! container's total under it, each page counted once
//! ([`legion_sys::JailMemory`]), since rctl would count memory its
//! processes share once for each of them. A container over its limit
//! loses its largest process, as with Linux's OOM killer in a container.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_sys::Sys;

/// The limits of the containers running here, and why the agent killed
/// a process of one, until the container's next exit is reported.
#[derive(Default)]
pub(crate) struct Limits {
    /// Bytes, by container jail.
    limits: Mutex<BTreeMap<String, u64>>,
    kills: Mutex<BTreeMap<String, String>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Limits {
    /// A container's jail started with this limit (or none).
    pub fn started(&self, jail: &str, bytes: Option<u64>) {
        match bytes {
            Some(b) => lock(&self.limits).insert(jail.to_owned(), b),
            None => lock(&self.limits).remove(jail),
        };
        lock(&self.kills).remove(jail);
    }

    /// The pod whose infra jail is `infra` is gone.
    pub fn removed(&self, infra: &str) {
        let prefix = format!("{infra}.");
        lock(&self.limits).retain(|j, _| !j.starts_with(&prefix));
        lock(&self.kills).retain(|j, _| !j.starts_with(&prefix));
    }

    /// Why the agent killed a process of the container in `jail`, if it did
    /// since the container started.
    pub fn killed(&self, jail: &str) -> Option<String> {
        lock(&self.kills).remove(jail)
    }
}

fn mib(bytes: u64) -> String {
    format!("{} MiB", bytes.div_ceil(1 << 20))
}

/// Every `every`, measures the containers with limits and kills the
/// largest process of each over its limit. Runs until aborted.
pub(crate) async fn enforce(sys: Arc<dyn Sys>, limits: Arc<Limits>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Warned once, not every second.
    let mut failing = false;
    loop {
        tick.tick().await;
        let wanted = lock(&limits.limits).clone();
        if wanted.is_empty() {
            continue;
        }
        let jails: Vec<String> = wanted.keys().cloned().collect();
        let l = limits.clone();
        let result = crate::task::blocking(&sys, move |sys| {
            let measured = sys.jail_memory(&jails)?;
            for (jail, m) in measured {
                let Some(&limit) = wanted.get(&jail) else {
                    continue;
                };
                let Some((pid, _)) = m.largest.filter(|_| m.bytes > limit) else {
                    continue;
                };
                let why = format!(
                    "out of memory: used {} of its {} limit",
                    mib(m.bytes),
                    mib(limit)
                );
                tracing::warn!(%jail, pid, "{why}; killing its largest process");
                match sys.kill(pid) {
                    Ok(()) => {
                        lock(&l.kills).insert(jail, why);
                    }
                    Err(e) => tracing::warn!(%jail, pid, "killing: {e}"),
                }
            }
            Ok::<_, legion_sys::Error>(())
        })
        .await;
        match result {
            Ok(()) => failing = false,
            Err(e) => {
                if !failing {
                    tracing::warn!(
                        "counting containers' memory: {e}; only each process's own use is limited"
                    );
                }
                failing = true;
            }
        }
    }
}
