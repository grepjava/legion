//! Long operations (`legion_api::activity`, `Cluster.Activity`): worked
//! out from the drains and Volume moves the replica sees, with what the
//! leader's movers say as they send (a move's transfer under way, its
//! bytes), and reported by those that are no object's state (a restore, a
//! replica's whole copy). Kept in memory, as events are: those under way,
//! and the ended ones for an hour, at most [`KEPT_ENDED`]. Every manager
//! follows the store; only the leader hears its movers, so the API asks
//! the leader.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use legion_api::activity::{Operation, OperationKind, OperationState, Stage};
use legion_api::audit::{Filter, Outcome, Target};
use legion_api::node::{Availability, DrainPhase, NodeSpec, NodeStatus};
use legion_api::volume::{MigrationPhase, VolumeStatus};
use legion_api::{Kind, Object, ObjectKey};
use legion_store::{Event as Change, Store};
use tokio::sync::{Notify, broadcast};

/// Ended operations kept, at most.
pub(crate) const KEPT_ENDED: usize = 200;
/// Ended operations are kept this long.
pub(crate) const KEPT: Duration = Duration::from_secs(3600);
/// How often bytes under way are taken in.
const TICK: Duration = Duration::from_secs(1);
/// How long after an operation began its asker is looked for in the
/// audit trail, which is written once a second.
const LOOK_FOR: i64 = 30_000;

fn now_ms() -> i64 {
    crate::console::now_ms()
}

/// What a move was seen to be, to tell its next state apart.
#[derive(Debug, Clone)]
struct Seen {
    from: String,
    to: String,
    phase: Option<MigrationPhase>,
}

#[derive(Default)]
struct Inner {
    /// Oldest first.
    ops: Vec<Operation>,
    /// The transfers of moves under way on this manager, by Volume: what
    /// they have sent, and whether one has ended well, which keeps its
    /// stage until the Volume's next phase is seen.
    sending: HashMap<ObjectKey, (Arc<AtomicU64>, bool)>,
    /// The counters of reported operations, by id.
    counters: HashMap<String, Arc<AtomicU64>>,
    /// Each running move's Volume as last seen, by id.
    moves: HashMap<String, Seen>,
    /// Each node's availability as last seen.
    availability: HashMap<String, Availability>,
    /// Operations whose asker was looked for, and found or given up.
    looked: HashSet<String>,
}

pub(crate) struct State {
    inner: Mutex<Inner>,
    /// Each operation as it changes.
    new: broadcast::Sender<Operation>,
    /// Woken as a transfer starts or ends.
    poke: Notify,
}

impl Default for State {
    fn default() -> State {
        State {
            inner: Mutex::default(),
            new: broadcast::channel(1024).0,
            poke: Notify::new(),
        }
    }
}

impl State {
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Every operation kept, oldest first.
    pub(crate) fn all(&self) -> Vec<Operation> {
        self.lock().ops.clone()
    }

    /// The operations still to change.
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Operation> {
        self.new.subscribe()
    }

    /// Changes the running operation `pick` finds with `f`, telling those
    /// following if it changed; whether one was found.
    fn change(
        &self,
        inner: &mut Inner,
        pick: impl Fn(&Operation) -> bool,
        f: impl FnOnce(&mut Operation),
    ) -> bool {
        let Some(op) = inner.ops.iter_mut().find(|o| o.running() && pick(o)) else {
            return false;
        };
        let before = op.clone();
        f(op);
        if *op != before {
            let _ = self.new.send(op.clone());
        }
        if !op.running() {
            let id = op.id.clone();
            inner.moves.remove(&id);
            inner.counters.remove(&id);
        }
        true
    }

    fn add(&self, inner: &mut Inner, op: Operation) {
        let _ = self.new.send(op.clone());
        inner.ops.push(op);
        prune(inner, now_ms());
    }

    /// A move's transfer starts on this manager: what it sends is counted
    /// on the counter given, until the guard goes.
    pub(crate) fn sending(self: &Arc<Self>, volume: &ObjectKey) -> Sending {
        let sent = Arc::new(AtomicU64::new(0));
        self.lock()
            .sending
            .insert(volume.clone(), (sent.clone(), false));
        self.poke.notify_one();
        Sending {
            state: self.clone(),
            volume: volume.clone(),
            sent,
            done: false,
        }
    }

    /// Begins an operation no object's state shows (a restore, a replica's
    /// copy), reported by its handle until it is finished.
    pub(crate) fn begin(
        self: &Arc<Self>,
        kind: OperationKind,
        target: Target,
        what: String,
        stage: Stage,
        by: String,
    ) -> Reported {
        let mut op = Operation::new(kind, target, what, now_ms(), stage);
        op.by = by;
        let id = op.id.clone();
        let counter = Arc::new(AtomicU64::new(0));
        let mut inner = self.lock();
        inner.looked.insert(id.clone());
        inner.counters.insert(id.clone(), counter.clone());
        self.add(&mut inner, op);
        Reported {
            state: self.clone(),
            id,
            counter,
            ended: false,
        }
    }

    /// Takes in what transfers have sent, and moves a move on to its full
    /// copy once its transfer starts or back once it ends.
    fn tick(&self) {
        let mut inner = self.lock();
        let now = now_ms();
        let running: Vec<(String, OperationKind, ObjectKey)> = inner
            .ops
            .iter()
            .filter(|o| o.running())
            .filter_map(|o| Some((o.id.clone(), o.kind, key_of(&o.target)?)))
            .collect();
        for (id, kind, key) in running {
            let (bytes, sending) = match kind {
                OperationKind::Move => match inner.sending.get(&key) {
                    Some((c, _)) => (Some(c.load(Ordering::Relaxed)), true),
                    None => (None, false),
                },
                _ => (
                    inner.counters.get(&id).map(|c| c.load(Ordering::Relaxed)),
                    false,
                ),
            };
            let phase = inner.moves.get(&id).and_then(|s| s.phase);
            self.change(
                &mut inner,
                |o| o.id == id,
                |op| {
                    if let Some(phase) = phase {
                        op.enter(now, Stage::of_move(phase, sending));
                    }
                    if let Some(b) = bytes {
                        op.bytes = b;
                    }
                },
            );
        }
        prune(&mut inner, now);
    }
}

/// Drops the ended operations past [`KEPT`] and [`KEPT_ENDED`].
fn prune(inner: &mut Inner, now: i64) {
    let kept = KEPT.as_millis() as i64;
    inner
        .ops
        .retain(|o| o.running() || o.ended_ms.is_some_and(|e| now - e <= kept));
    let ended = inner.ops.iter().filter(|o| !o.running()).count();
    let mut over = ended.saturating_sub(KEPT_ENDED);
    inner.ops.retain(|o| {
        if over > 0 && !o.running() {
            over -= 1;
            return false;
        }
        true
    });
    let ids: HashSet<&String> = inner.ops.iter().map(|o| &o.id).collect();
    let looked: HashSet<String> = inner
        .looked
        .iter()
        .filter(|id| ids.contains(id))
        .cloned()
        .collect();
    inner.looked = looked;
}

fn key_of(t: &Target) -> Option<ObjectKey> {
    let kind: Kind = t.kind.parse().ok()?;
    Some(ObjectKey::new(kind, &t.namespace, &t.name))
}

fn target(kind: &str, namespace: &str, name: &str) -> Target {
    crate::audit::target(kind, namespace, name)
}

/// A move's transfer under way; the move's stage goes back once it goes.
pub(crate) struct Sending {
    state: Arc<State>,
    volume: ObjectKey,
    sent: Arc<AtomicU64>,
    done: bool,
}

impl Sending {
    pub(crate) fn counter(&self) -> &AtomicU64 {
        &self.sent
    }

    /// It ended well: the stage stays until the Volume's next phase is
    /// seen, rather than going back to preparing in between.
    pub(crate) fn done(mut self) {
        self.done = true;
    }
}

impl Drop for Sending {
    fn drop(&mut self) {
        let mut inner = self.state.lock();
        if let Some((c, done)) = inner.sending.get_mut(&self.volume)
            && Arc::ptr_eq(c, &self.sent)
        {
            match self.done {
                true => *done = true,
                false => {
                    inner.sending.remove(&self.volume);
                }
            }
        }
        drop(inner);
        self.state.poke.notify_one();
    }
}

/// An operation reported by what runs it; failed if dropped unfinished.
pub(crate) struct Reported {
    state: Arc<State>,
    id: String,
    counter: Arc<AtomicU64>,
    ended: bool,
}

impl Reported {
    pub(crate) fn counter(&self) -> &AtomicU64 {
        &self.counter
    }

    /// Moves it on to `stage`.
    pub(crate) fn enter(&self, stage: Stage) {
        let bytes = self.counter.swap(0, Ordering::Relaxed);
        let mut inner = self.state.lock();
        self.state.change(
            &mut inner,
            |o| o.id == self.id,
            |op| {
                op.bytes = bytes;
                op.enter(now_ms(), stage);
            },
        );
    }

    /// Ends it: succeeded, or failed saying why.
    pub(crate) fn finish(mut self, result: Result<String, String>) {
        self.ended = true;
        let (state, message) = match result {
            Ok(m) => (OperationState::Succeeded, m),
            Err(e) => (OperationState::Failed, e),
        };
        self.end(state, message);
    }

    fn end(&self, state: OperationState, message: String) {
        let bytes = self.counter.load(Ordering::Relaxed);
        let mut inner = self.state.lock();
        self.state.change(
            &mut inner,
            |o| o.id == self.id,
            |op| {
                op.bytes = op.bytes.max(bytes);
                op.end(now_ms(), state, message);
            },
        );
    }
}

impl Drop for Reported {
    fn drop(&mut self) {
        if !self.ended {
            self.end(
                OperationState::Failed,
                "it stopped before it was done".into(),
            );
        }
    }
}

/// What still holds a draining node, in a line.
fn drain_left(status: &legion_api::node::DrainStatus) -> String {
    if status.remaining.is_empty() {
        return match status.reason.is_empty() {
            true => String::new(),
            false => format!("blocked ({})", status.reason),
        };
    }
    let items: Vec<String> = status
        .remaining
        .iter()
        .map(|i| {
            let mut s = format!("{} {}/{} {}", i.kind, i.namespace, i.name, i.reason);
            if !i.message.is_empty() {
                s.push_str(": ");
                s.push_str(&i.message);
            }
            s
        })
        .collect();
    format!("{} left: {}", status.remaining.len(), items.join(", "))
}

/// Takes in a Node as it is now (`None`: gone), its drain begun, moved on
/// or ended. `first`: the first look, where a drain under way is one this
/// manager comes upon, and one completed is none.
fn observe_node(state: &State, inner: &mut Inner, name: &str, node: Option<&Object>, first: bool) {
    let now = now_ms();
    let is_drain = |o: &Operation| o.kind == OperationKind::Drain && o.target.name == name;
    let Some(node) = node else {
        inner.availability.remove(name);
        state.change(inner, is_drain, |op| {
            op.end(now, OperationState::Cancelled, "the node was removed")
        });
        return;
    };
    let (Ok(spec), Ok(status)) = (
        legion_api::parse::<NodeSpec>(&node.spec),
        serde_json::from_value::<NodeStatus>(node.status.clone()),
    ) else {
        return;
    };
    let before = inner
        .availability
        .insert(name.to_owned(), spec.availability);
    let drain = status.drain.as_ref();
    let completed = drain.is_some_and(|d| d.phase == DrainPhase::Completed);
    if spec.availability != Availability::Drain {
        let word = match spec.availability {
            Availability::Active => "active",
            Availability::Pause => "paused",
            Availability::Drain => unreachable!(),
        };
        state.change(inner, is_drain, |op| {
            op.end(
                now,
                OperationState::Cancelled,
                format!("the node was made {word} again"),
            )
        });
        return;
    }
    let running = inner.ops.iter().any(|o| o.running() && is_drain(o));
    if !running {
        // Begun now, or under way when this manager first looked; one
        // completed before that is not seen begin.
        let begins = match first {
            true => !completed,
            false => before != Some(Availability::Drain),
        };
        if !begins {
            return;
        }
        let mut op = Operation::new(
            OperationKind::Drain,
            target("Node", "", name),
            format!("drain node {name}"),
            now,
            Stage::Draining,
        );
        if first {
            op.say(now, "under way when this manager began to follow it");
        }
        state.add(inner, op);
    }
    state.change(inner, is_drain, |op| {
        let stage = match drain.map(|d| d.phase) {
            Some(DrainPhase::Blocked) => Stage::Blocked,
            Some(DrainPhase::Completed) => Stage::Completed,
            _ => Stage::Draining,
        };
        op.enter(now, stage);
        let left = drain.map(drain_left).unwrap_or_default();
        op.note(now, &left);
        if stage == Stage::Completed {
            op.end(
                now,
                OperationState::Succeeded,
                "nothing runs on it and none of its data is moving",
            );
        }
    });
}

/// Takes in a Volume as it is now (`None`: gone): a move begun, moved on,
/// switched over, cleaned up or called off.
fn observe_volume(
    state: &State,
    inner: &mut Inner,
    key: &ObjectKey,
    volume: Option<&Object>,
    first: bool,
) {
    let now = now_ms();
    let (namespace, name) = (key.namespace.clone(), key.name.clone());
    let is_move = |o: &Operation| {
        o.kind == OperationKind::Move && o.target.namespace == namespace && o.target.name == name
    };
    let running = inner
        .ops
        .iter()
        .find(|o| o.running() && is_move(o))
        .map(|o| o.id.clone());
    let seen = running.as_ref().and_then(|id| inner.moves.get(id).cloned());
    let Some(volume) = volume else {
        state.change(inner, is_move, |op| {
            op.end(now, OperationState::Cancelled, "the Volume was deleted")
        });
        return;
    };
    let status: VolumeStatus = serde_json::from_value(volume.status.clone()).unwrap_or_default();
    // A transfer that ended well counts until the phase after it is seen.
    let phase = status.migration.as_ref().map(|m| m.phase);
    if seen.as_ref().is_none_or(|s| s.phase != phase)
        && inner.sending.get(key).is_some_and(|(_, done)| *done)
    {
        inner.sending.remove(key);
    }
    let sending = inner.sending.contains_key(key);
    match (&status.migration, running, seen) {
        (Some(m), Some(id), Some(seen)) if seen.to == m.to && seen.phase.is_some() => {
            inner.moves.insert(
                id.clone(),
                Seen {
                    phase: Some(m.phase),
                    ..seen
                },
            );
            state.change(
                inner,
                |o| o.id == id,
                |op| {
                    op.enter(now, Stage::of_move(m.phase, sending));
                    op.note(now, &m.message);
                },
            );
        }
        (Some(m), running, seen) => {
            if let (Some(id), Some(seen)) = (running, seen) {
                let why = match seen.phase {
                    // Its old copy is still to drop: the migrator does,
                    // whatever moves next.
                    None => (
                        OperationState::Succeeded,
                        format!("on {}; it moves on to {} now", seen.to, m.to),
                    ),
                    Some(_) => (
                        OperationState::Cancelled,
                        format!("sent to {} instead", m.to),
                    ),
                };
                state.change(inner, |o| o.id == id, |op| op.end(now, why.0, why.1));
            }
            let from = status.node.clone();
            let stage = Stage::of_move(m.phase, sending);
            let mut op = Operation::new(
                OperationKind::Move,
                target("Volume", &namespace, &name),
                format!("move Volume {namespace}/{name} from {from} to {}", m.to),
                now,
                stage,
            );
            op.part_of = inner
                .ops
                .iter()
                .find(|o| o.running() && o.kind == OperationKind::Drain && o.target.name == from)
                .map(|o| o.id.clone())
                .unwrap_or_default();
            if first {
                op.say(now, "under way when this manager began to follow it");
            }
            op.note(now, &m.message);
            inner.moves.insert(
                op.id.clone(),
                Seen {
                    from,
                    to: m.to.clone(),
                    phase: Some(m.phase),
                },
            );
            state.add(inner, op);
        }
        (None, Some(id), Some(seen)) if status.node == seen.to => {
            // Switched over: running on the new node, its old copy dropped
            // by the migrator, then forgotten.
            let left = status.leftovers.contains(&seen.from);
            inner.moves.insert(
                id.clone(),
                Seen {
                    phase: None,
                    ..seen.clone()
                },
            );
            state.change(
                inner,
                |o| o.id == id,
                |op| {
                    op.enter(now, Stage::Cleanup);
                    op.note(now, "");
                    if !left {
                        op.end(
                            now,
                            OperationState::Succeeded,
                            format!("on {}, nothing of it left on {}", seen.to, seen.from),
                        );
                    }
                },
            );
        }
        (None, Some(id), seen) => {
            let on = match status.node.is_empty() {
                true => "no node".to_owned(),
                false => status.node.clone(),
            };
            // One switched over already ended well, wherever the Volume
            // went since (handed to its replica, say).
            let (ended, why) = match seen.and_then(|s| s.phase.is_none().then_some(s.to)) {
                Some(to) => (
                    OperationState::Succeeded,
                    format!("on {to}; it is on {on} now"),
                ),
                None => (
                    OperationState::Cancelled,
                    format!("called off: its data stays on {on}"),
                ),
            };
            state.change(inner, |o| o.id == id, |op| op.end(now, ended, why));
        }
        (None, None, _) => {}
    }
}

/// Follows the replica, working out drains and moves as it changes, and
/// takes in what transfers send.
pub(crate) async fn track(
    store: Store,
    state: Arc<State>,
    mut closing: tokio::sync::watch::Receiver<bool>,
) {
    let mut first = true;
    loop {
        if *closing.borrow() {
            return;
        }
        // Listed, then watched from the listing's version, as events are.
        let watched = (|| {
            let version = store.applied_version().map_err(|e| e.to_string())?;
            let mut seen = Vec::new();
            for kind in [Kind::Node, Kind::Volume] {
                seen.extend(store.list(kind, None).map_err(|e| e.to_string())?.objects);
            }
            let watcher = store.watch(version, None).map_err(|e| e.to_string())?;
            Ok::<_, String>((seen, watcher))
        })();
        let (seen, mut watcher) = match watched {
            Ok(w) => w,
            Err(e) => {
                tracing::debug!("activity: listing: {e}");
                tokio::select! {
                    () = tokio::time::sleep(TICK) => continue,
                    _ = closing.wait_for(|c| *c) => return,
                }
            }
        };
        {
            let mut inner = state.lock();
            let present: HashSet<ObjectKey> = seen.iter().map(Object::key).collect();
            // What went while the watch was down ends.
            let gone: Vec<Target> = inner
                .ops
                .iter()
                .filter(|o| {
                    o.running() && matches!(o.kind, OperationKind::Drain | OperationKind::Move)
                })
                .filter(|o| key_of(&o.target).is_some_and(|k| !present.contains(&k)))
                .map(|o| o.target.clone())
                .collect();
            for t in gone {
                match t.kind.as_str() {
                    "Node" => observe_node(&state, &mut inner, &t.name, None, false),
                    _ => {
                        let key = ObjectKey::new(Kind::Volume, &t.namespace, &t.name);
                        observe_volume(&state, &mut inner, &key, None, false);
                    }
                }
            }
            // Nodes first, so a move finds the drain it is part of.
            for o in seen.iter().filter(|o| o.kind == Kind::Node) {
                observe_node(&state, &mut inner, &o.metadata.name, Some(o), first);
            }
            for o in seen.iter().filter(|o| o.kind == Kind::Volume) {
                observe_volume(&state, &mut inner, &o.key(), Some(o), first);
            }
        }
        first = false;
        let mut tick = tokio::time::interval(TICK);
        loop {
            let change = tokio::select! {
                c = watcher.next() => c,
                _ = tick.tick() => {
                    state.tick();
                    look_for_askers(&store, &state);
                    continue;
                }
                () = state.poke.notified() => {
                    state.tick();
                    continue;
                }
                _ = closing.wait_for(|c| *c) => return,
            };
            let mut inner = state.lock();
            match change {
                Ok(Change::Put(o)) if o.kind == Kind::Node => {
                    observe_node(&state, &mut inner, &o.metadata.name, Some(&o), false);
                }
                Ok(Change::Delete(o)) if o.kind == Kind::Node => {
                    observe_node(&state, &mut inner, &o.metadata.name, None, false);
                }
                Ok(Change::Put(o)) if o.kind == Kind::Volume => {
                    observe_volume(&state, &mut inner, &o.key(), Some(&o), false);
                }
                Ok(Change::Delete(o)) if o.kind == Kind::Volume => {
                    observe_volume(&state, &mut inner, &o.key(), None, false);
                }
                Ok(Change::Put(_) | Change::Delete(_)) => {}
                Ok(Change::Reset(_)) | Err(_) => break,
            }
        }
    }
}

/// Who asked for each drain and move begun lately, from the audit trail:
/// the last change to its object that went through up to a moment after
/// it began. A move a drain makes was asked for by whoever drained.
fn look_for_askers(store: &Store, state: &State) {
    let now = now_ms();
    let wanted: Vec<Operation> = {
        let inner = state.lock();
        inner
            .ops
            .iter()
            .filter(|o| o.by.is_empty() && !inner.looked.contains(&o.id))
            .filter(|o| matches!(o.kind, OperationKind::Drain | OperationKind::Move))
            .filter(|o| now - o.began_ms >= 1500)
            .cloned()
            .collect()
    };
    for op in wanted {
        let by = match op.part_of.is_empty() {
            false => {
                let inner = state.lock();
                inner
                    .ops
                    .iter()
                    .find(|o| o.id == op.part_of)
                    .map(|o| o.by.clone())
                    .unwrap_or_default()
            }
            true => asker(store, &op).unwrap_or_default(),
        };
        let mut inner = state.lock();
        if !by.is_empty() || now - op.began_ms > LOOK_FOR {
            inner.looked.insert(op.id.clone());
        }
        if by.is_empty() {
            continue;
        }
        let id = op.id.clone();
        // Changed even once ended: who asked is still to say.
        if let Some(o) = inner.ops.iter_mut().find(|o| o.id == id) {
            o.by = by;
            let _ = state.new.send(o.clone());
        }
    }
}

/// Whoever last changed `op`'s object, as the audit trail has it, up to a
/// moment after `op` began: a drain's Node, a move's Volume (asked to be
/// on another node).
fn asker(store: &Store, op: &Operation) -> Option<String> {
    let filter = Filter {
        since_ms: (op.began_ms - 120_000).max(0) as u64,
        action: "apply".into(),
        kind: op.target.kind.clone(),
        namespace: op.target.namespace.clone(),
        result: Some(Outcome::Ok),
        ..Default::default()
    };
    let (events, _) = crate::audit::list(store, &filter, 1000).ok()?;
    events
        .iter()
        .rev()
        .find(|e| e.target.name == op.target.name && e.time_ms as i64 <= op.began_ms + 2000)
        .map(|e| match e.actor.name.is_empty() {
            true => e.actor.kind.clone(),
            false => e.actor.name.clone(),
        })
}

/// Whether `now`, the Volume or Node an apply would write over `cur`, calls
/// off a Volume's move past its switch-over, which is refused: the tasks
/// using it have stopped for the last part, and the move goes through. The
/// Volume asked to be elsewhere than its move goes, or the node it moves
/// off made active or paused while the move was for the drain.
pub(crate) fn past_switch_over(
    volumes: impl FnOnce() -> Result<Vec<Object>, String>,
    cur: &Object,
    next: &Object,
) -> Result<Option<String>, String> {
    let finishing = |v: &Object| {
        let status: VolumeStatus = serde_json::from_value(v.status.clone()).unwrap_or_default();
        status
            .migration
            .filter(|m| m.phase == MigrationPhase::Finishing)
            .map(|m| (status.node, m.to))
    };
    match cur.kind {
        Kind::Volume => {
            let Some((from, to)) = finishing(cur) else {
                return Ok(None);
            };
            let node = |o: &Object| {
                o.spec
                    .get("node")
                    .and_then(|n| n.as_str())
                    .map(str::to_owned)
            };
            let (was, now) = (node(cur), node(next));
            if was == now || now.as_deref() == Some(to.as_str()) {
                return Ok(None);
            }
            Ok(Some(format!(
                "Volume {}/{} is past its switch-over from {from} to {to}: its tasks have \
                 stopped for the last part, and the move goes through; ask again once it is \
                 on {to}",
                cur.metadata.namespace, cur.metadata.name
            )))
        }
        Kind::Node => {
            let availability = |o: &Object| {
                legion_api::parse::<NodeSpec>(&o.spec)
                    .map(|s| s.availability)
                    .unwrap_or_default()
            };
            if availability(cur) != Availability::Drain || availability(next) == Availability::Drain
            {
                return Ok(None);
            }
            let name = &cur.metadata.name;
            let held: Vec<String> = volumes()?
                .iter()
                .filter_map(|v| {
                    let (from, to) = finishing(v)?;
                    // One asked for by its own `node` goes on regardless.
                    let asked = v.spec.get("node").and_then(|n| n.as_str()) == Some(to.as_str());
                    (from == *name && !asked)
                        .then(|| format!("{}/{} to {to}", v.metadata.namespace, v.metadata.name))
                })
                .collect();
            if held.is_empty() {
                return Ok(None);
            }
            Ok(Some(format!(
                "node {name}'s drain is past the switch-over of Volume {}: its tasks have \
                 stopped for the last part, and the move goes through; ask again once it is done",
                held.join(", ")
            )))
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use legion_api::node::{DrainItem, DrainStatus};
    use legion_api::volume::Migration;

    use super::*;

    fn node(availability: &str, drain: Option<DrainStatus>) -> Object {
        let mut o = Object::new(
            Kind::Node,
            "",
            "n2",
            serde_json::json!({ "availability": availability }),
        );
        let mut status = serde_json::json!({ "state": "ready" });
        if let Some(d) = drain {
            status["drain"] = serde_json::to_value(d).unwrap();
        }
        o.status = status;
        o
    }

    fn volume(node: &str, migration: Option<Migration>, leftovers: &[&str]) -> Object {
        let mut o = Object::new(Kind::Volume, "data", "pg-db-0", serde_json::json!({}));
        o.status = serde_json::to_value(VolumeStatus {
            node: node.into(),
            migration,
            leftovers: leftovers.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        })
        .unwrap();
        o
    }

    fn moving(phase: MigrationPhase) -> Option<Migration> {
        Some(Migration {
            to: "n3".into(),
            phase,
            message: String::new(),
        })
    }

    fn draining(phase: DrainPhase, left: usize) -> DrainStatus {
        DrainStatus {
            phase,
            reason: String::new(),
            remaining: (0..left)
                .map(|_| DrainItem {
                    kind: "Volume".into(),
                    namespace: "data".into(),
                    name: "pg-db-0".into(),
                    reason: "copying".into(),
                    message: String::new(),
                })
                .collect(),
        }
    }

    fn stages(op: &Operation) -> Vec<&'static str> {
        op.stages.iter().map(|m| m.stage.as_str()).collect()
    }

    #[test]
    fn a_drain_and_its_move_go_through_each_stage_in_turn() {
        let state = Arc::new(State::default());
        let key = ObjectKey::new(Kind::Volume, "data", "pg-db-0");
        let mut inner = state.lock();
        observe_node(&state, &mut inner, "n2", Some(&node("active", None)), true);
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", None, &[])),
            true,
        );
        assert!(inner.ops.is_empty(), "nothing under way");

        observe_node(
            &state,
            &mut inner,
            "n2",
            Some(&node("drain", Some(draining(DrainPhase::Draining, 1)))),
            false,
        );
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", moving(MigrationPhase::Copying), &[])),
            false,
        );
        drop(inner);
        let sending = state.sending(&key);
        sending.counter().fetch_add(4096, Ordering::Relaxed);
        state.tick();
        sending.done();
        state.tick();
        let mut inner = state.lock();
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", moving(MigrationPhase::Copied), &[])),
            false,
        );
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", moving(MigrationPhase::Finishing), &[])),
            false,
        );
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n3", None, &["n2"])),
            false,
        );
        observe_node(
            &state,
            &mut inner,
            "n2",
            Some(&node("drain", Some(draining(DrainPhase::Completed, 0)))),
            false,
        );
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n3", None, &[])),
            false,
        );

        let ops = inner.ops.clone();
        assert_eq!(ops.len(), 2, "{ops:#?}");
        let (drain, mv) = (&ops[0], &ops[1]);
        assert_eq!(drain.state, OperationState::Succeeded);
        assert_eq!(stages(drain), ["draining", "completed"]);
        assert_eq!(mv.part_of, drain.id);
        assert_eq!(mv.what, "move Volume data/pg-db-0 from n2 to n3");
        assert_eq!(
            stages(mv),
            [
                "preparing",
                "full-copy",
                "stopping",
                "switch-over",
                "cleanup"
            ]
        );
        assert_eq!(mv.state, OperationState::Succeeded);
        assert!(
            mv.output
                .iter()
                .any(|l| l.text == "full-copy: 4.0 KiB sent"),
            "{:?}",
            mv.output
        );
    }

    #[test]
    fn a_drain_made_active_again_is_cancelled_and_its_move_called_off() {
        let state = Arc::new(State::default());
        let key = ObjectKey::new(Kind::Volume, "data", "pg-db-0");
        let mut inner = Inner::default();
        observe_node(
            &state,
            &mut inner,
            "n2",
            Some(&node("drain", Some(draining(DrainPhase::Draining, 1)))),
            true,
        );
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", moving(MigrationPhase::Copying), &[])),
            true,
        );
        observe_node(&state, &mut inner, "n2", Some(&node("active", None)), false);
        observe_volume(
            &state,
            &mut inner,
            &key,
            Some(&volume("n2", None, &["n3"])),
            false,
        );
        let ops = &inner.ops;
        assert_eq!(ops[0].state, OperationState::Cancelled);
        assert_eq!(ops[0].message, "the node was made active again");
        assert!(
            ops[0].output[1]
                .text
                .contains("under way when this manager"),
            "{:?}",
            ops[0].output
        );
        assert_eq!(ops[1].state, OperationState::Cancelled);
        assert_eq!(ops[1].message, "called off: its data stays on n2");
    }

    #[test]
    fn a_drain_completed_before_the_first_look_is_none() {
        let state = Arc::new(State::default());
        let mut inner = Inner::default();
        observe_node(
            &state,
            &mut inner,
            "n2",
            Some(&node("drain", Some(draining(DrainPhase::Completed, 0)))),
            true,
        );
        assert!(inner.ops.is_empty());
        // Drained again later is.
        observe_node(&state, &mut inner, "n2", Some(&node("active", None)), false);
        observe_node(&state, &mut inner, "n2", Some(&node("drain", None)), false);
        assert_eq!(inner.ops.len(), 1);
        assert!(inner.ops[0].running());
    }

    #[test]
    fn a_reported_operation_counts_its_bytes_and_fails_if_dropped() {
        let state = Arc::new(State::default());
        let op = state.begin(
            OperationKind::Restore,
            target("Volume", "data", "v"),
            "restore".into(),
            Stage::Receiving,
            "alice".into(),
        );
        op.counter().fetch_add(10, Ordering::Relaxed);
        state.tick();
        assert_eq!(state.all()[0].bytes, 10);
        op.enter(Stage::Registering);
        op.finish(Ok("on n3".into()));
        let dropped = state.begin(
            OperationKind::ReplicaCopy,
            target("Volume", "data", "w"),
            "copy".into(),
            Stage::FullCopy,
            String::new(),
        );
        drop(dropped);
        let all = state.all();
        assert_eq!(all[0].state, OperationState::Succeeded);
        assert_eq!(all[0].by, "alice");
        assert_eq!(stages(&all[0]), ["receiving", "registering"]);
        assert!(
            all[0]
                .output
                .iter()
                .any(|l| l.text == "receiving: 10 B sent")
        );
        assert_eq!(all[1].state, OperationState::Failed);
    }

    #[test]
    fn a_failed_transfer_goes_back_to_preparing() {
        let state = Arc::new(State::default());
        let key = ObjectKey::new(Kind::Volume, "data", "pg-db-0");
        let mut inner = Inner::default();
        let copying = volume("n2", moving(MigrationPhase::Copying), &[]);
        observe_volume(&state, &mut inner, &key, Some(&copying), true);
        *state.lock() = inner;
        let sending = state.sending(&key);
        state.tick();
        drop(sending);
        state.tick();
        assert_eq!(
            stages(&state.all()[0]),
            ["preparing", "full-copy", "preparing"]
        );
    }

    #[test]
    fn a_cancel_past_the_switch_over_is_refused() {
        let mut cur = volume("n2", moving(MigrationPhase::Finishing), &[]);
        cur.spec = serde_json::json!({ "node": "n3" });
        let mut back = cur.clone();
        back.spec = serde_json::json!({ "node": "n2" });
        let none = || Ok(Vec::new());
        let why = past_switch_over(none, &cur, &back).unwrap().unwrap();
        assert!(why.contains("past its switch-over from n2 to n3"), "{why}");
        assert_eq!(past_switch_over(none, &cur, &cur).unwrap(), None);
        // Before the switch-over, it is called off as asked.
        let mut copying = volume("n2", moving(MigrationPhase::Copied), &[]);
        copying.spec = cur.spec.clone();
        let mut back = copying.clone();
        back.spec = serde_json::json!({ "node": "n2" });
        assert_eq!(past_switch_over(none, &copying, &back).unwrap(), None);

        // A drain made active while a move off its node switches over.
        let (draining, active) = (node("drain", None), node("active", None));
        let finishing = || Ok(vec![volume("n2", moving(MigrationPhase::Finishing), &[])]);
        let why = past_switch_over(finishing, &draining, &active)
            .unwrap()
            .unwrap();
        assert!(
            why.contains("past the switch-over of Volume data/pg-db-0 to n3"),
            "{why}"
        );
        // Not for a Volume asked to be on that node itself, nor before.
        let mut asked = volume("n2", moving(MigrationPhase::Finishing), &[]);
        asked.spec = serde_json::json!({ "node": "n3" });
        let asked = move || Ok(vec![asked]);
        assert_eq!(past_switch_over(asked, &draining, &active).unwrap(), None);
        let copied = || Ok(vec![volume("n2", moving(MigrationPhase::Copied), &[])]);
        assert_eq!(past_switch_over(copied, &draining, &active).unwrap(), None);
    }
}
