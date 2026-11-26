//! Background tasks kept running. A daemon's loops (a manager's
//! controllers, an agent's heartbeats, a runtime's reaper) are meant to run
//! until it stops; one that panicked used to end without a word, the daemon
//! running on without it (found by an audit). Supervised, a loop that
//! panics is said loudly, counted ([`panics`], for the metrics), and
//! started again after a pause that grows with each panic in a row, up to
//! a minute; one that returns has ended, as when its daemon stops.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

/// The panics of each supervised loop, by its name, since the process
/// started.
static PANICS: Mutex<BTreeMap<&'static str, u64>> = Mutex::new(BTreeMap::new());

/// A task aborted when this is dropped: a supervisor aborted (its daemon
/// giving up) takes the loop it runs with it.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// How long after its `n`th panic in a row a loop starts again.
fn pause(n: u32) -> Duration {
    Duration::from_secs(1)
        .saturating_mul(2u32.saturating_pow(n.saturating_sub(1)))
        .min(Duration::from_secs(60))
}

/// Each supervised loop's panics so far, by name.
pub fn panics() -> Vec<(&'static str, u64)> {
    PANICS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .iter()
        .map(|(k, v)| (*k, *v))
        .collect()
}

/// What a panic said, as far as it says anything.
fn said(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("(no message)")
}

/// Runs the loop `make` makes, named `name`, making it again each time it
/// panics, until it returns or `closing` says the daemon stops. The handle
/// ends when the loop has ended, not when it panicked.
pub fn supervised<F, Fut>(
    name: &'static str,
    closing: Option<watch::Receiver<bool>>,
    make: F,
) -> JoinHandle<()>
where
    F: Fn() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let stopping = || closing.as_ref().is_some_and(|c| *c.borrow());
        let mut in_a_row = 0u32;
        loop {
            let mut running = AbortOnDrop(tokio::spawn(make()));
            let ran = (&mut running.0).await;
            let payload = match ran {
                Ok(()) => return,
                Err(e) if e.is_panic() => e.into_panic(),
                // Cancelled: its runtime is going away.
                Err(_) => return,
            };
            *PANICS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(name)
                .or_default() += 1;
            if stopping() {
                tracing::error!(task = name, "panicked while stopping: {}", said(&*payload));
                return;
            }
            in_a_row += 1;
            let wait = pause(in_a_row);
            tracing::error!(
                task = name,
                in_a_row,
                "panicked: {}; started again in {wait:?}",
                said(&*payload)
            );
            match closing.clone() {
                Some(mut c) => {
                    tokio::select! {
                        () = tokio::time::sleep(wait) => {}
                        _ = c.wait_for(|stop| *stop) => return,
                    }
                }
                None => tokio::time::sleep(wait).await,
            }
        }
    })
}

/// Runs `task`, named `name`, which cannot be made again (it owns what it
/// drains): a panic is said loudly and counted, as a supervised loop's,
/// though it does not start again.
pub fn watched<Fut>(name: &'static str, task: Fut) -> JoinHandle<()>
where
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        let mut running = AbortOnDrop(tokio::spawn(task));
        if let Err(e) = (&mut running.0).await
            && e.is_panic()
        {
            *PANICS
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(name)
                .or_default() += 1;
            tracing::error!(
                task = name,
                "panicked, and cannot start again: {}",
                said(&*e.into_panic())
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;

    /// A loop that panics is started again, and counted; one that returns
    /// has ended, and so has its handle.
    #[tokio::test(start_paused = true)]
    async fn a_loop_that_panics_is_started_again_until_it_returns() {
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let handle = supervised("test-flaky", None, move || {
            let r = r.clone();
            async move {
                if r.fetch_add(1, Ordering::SeqCst) < 2 {
                    panic!("not yet");
                }
            }
        });
        handle.await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 3);
        assert!(panics().contains(&("test-flaky", 2)), "{:?}", panics());
    }

    /// A daemon stopping: a loop that panics is not started again.
    #[tokio::test(start_paused = true)]
    async fn a_loop_that_panics_as_its_daemon_stops_is_left_ended() {
        let (stop, closing) = watch::channel(true);
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let handle = supervised("test-stopping", Some(closing), move || {
            let r = r.clone();
            async move {
                r.fetch_add(1, Ordering::SeqCst);
                panic!("going");
            }
        });
        handle.await.unwrap();
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        drop(stop);
    }

    #[tokio::test]
    async fn a_task_that_cannot_start_again_is_counted_when_it_panics() {
        watched("test-once", async { panic!("gone") })
            .await
            .unwrap();
        assert!(panics().contains(&("test-once", 1)), "{:?}", panics());
    }

    /// A supervisor aborted takes its loop with it.
    #[tokio::test]
    async fn an_aborted_supervisor_stops_its_loop() {
        let runs = Arc::new(AtomicU32::new(0));
        let r = runs.clone();
        let handle = supervised("test-aborted", None, move || {
            let r = r.clone();
            async move {
                loop {
                    r.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.abort();
        let _ = handle.await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let then = runs.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(runs.load(Ordering::SeqCst), then, "the loop still runs");
    }

    #[test]
    fn the_pause_grows_to_a_minute() {
        let secs: Vec<u64> = (1..=8).map(|n| pause(n).as_secs()).collect();
        assert_eq!(secs, [1, 2, 4, 8, 16, 32, 60, 60]);
    }
}
