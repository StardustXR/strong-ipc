//! death detection against a peer *process*, not a cooperative drop
//!
//! [`lifecycle`](../lifecycle.rs) covers who outlives whom when everything happens inside
//! one process and every end is dropped politely. This file covers the case the design
//! actually rests on: the far side is `SIGKILL`ed, so nothing over there runs a
//! destructor, sends a goodbye, or unwinds a runtime. All that is left to notice the
//! death is the kernel closing a dead process's descriptors, which is exactly what
//! `Ref::is_dead`'s bare `ppoll`, the death watch's `EPOLLHUP`, and the node's receive
//! loop are reading. An in-process `drop` cannot tell you whether any of that works,
//! because a `drop` is cooperative and a `SIGKILL` is not.
//!
//! these live in a test binary of their own on purpose. Spawning a process duplicates the
//! whole descriptor table into the child, and every socket in it stays open over there
//! until the `exec` lands — a window of milliseconds. Any test sharing the process would
//! see its own peers' hangups delayed by every spawn happening beside it, which is a
//! property of forking and not of this crate, but is quite enough to make a neighbour's
//! "the hangup is visible the moment the node reports dead" assertion flicker.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref, RefFsBinding, TrySendError};

fn socket_path(name: &str) -> PathBuf {
	let p = std::env::temp_dir().join(format!(
		"strong-ipc-killed-{}-{name}.sock",
		std::process::id()
	));
	let _ = std::fs::remove_file(&p);
	p
}

struct Null;
impl Handler for Null {
	async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<strong_ipc::UCred>) {}
}

/// tells the child process which end of the capability it is, and where to meet
const ROLE: &str = "STRONG_IPC_KILLED_PEER_ROLE";
const MEETING_POINT: &str = "STRONG_IPC_KILLED_PEER_SOCKET";

/// the other end of the killed-peer tests, in a process of its own
///
/// re-exec rather than `fork`: by the time any of these run, this binary is a
/// multi-threaded tokio process, and the only thing a forked child of one may legally do
/// is `exec`. Spawning a copy of the test binary and filtering it down to this one
/// `#[ignore]`d test is the cheapest way to get a *real* second process — one whose death
/// is the kernel closing its descriptors, with no destructor, no runtime shutdown, and no
/// chance to say goodbye.
///
/// harmless when run on its own (`--ignored` with no role set): it returns immediately.
#[test]
#[ignore = "spawned as the peer process by the killed-peer tests; not a test on its own"]
fn peer_process() {
	let Ok(role) = std::env::var(ROLE) else {
		return;
	};
	let path = PathBuf::from(std::env::var(MEETING_POINT).expect("no meeting point"));
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.unwrap();

	runtime.block_on(async move {
		// each arm parks forever holding its end. the parent is what ends this process,
		// and it ends it with a signal, which is the entire point
		match role.as_str() {
			// owns a node and puts a capability to it on the path
			"node" => {
				let (_node, node_ref) = Node::new(Null).unwrap();
				let _binding = RefFsBinding::new(node_ref, &path).unwrap();
				std::future::pending::<()>().await
			}
			// holds a capability to the *parent's* node, and says so once, so the parent
			// knows the descriptor is really open over there before it drops its own
			"ref" => {
				let node_ref = Ref::connect(&path).await.unwrap();
				node_ref
					.send(Message::from_data(b"holding".to_vec()))
					.await
					.unwrap();
				std::future::pending::<()>().await
			}
			other => panic!("unknown role {other}"),
		}
	});
}

/// a spawned peer, killed and reaped however the test ends
///
/// a test that panics between spawn and kill would otherwise leave a process parked on
/// `pending()` for the life of the machine.
struct Peer(Child);

impl Peer {
	fn spawn(role: &str, path: &Path) -> Self {
		let child = Command::new(std::env::current_exe().expect("current_exe"))
			.args(["--exact", "--ignored", "peer_process"])
			// libtest's own chatter would land in the parent's output; a panic over there
			// still reaches stderr, which is the part worth seeing
			.stdout(Stdio::null())
			.env(ROLE, role)
			.env(MEETING_POINT, path)
			.spawn()
			.expect("failed to spawn the peer process");
		Self(child)
	}

	/// `SIGKILL`, then reap. No unwinding, no flushing, no destructors over there — the
	/// death this crate has to survive in the wild
	fn kill(&mut self) {
		self.0.kill().expect("failed to kill the peer process");
		self.0.wait().expect("failed to reap the peer process");
	}
}

impl Drop for Peer {
	fn drop(&mut self) {
		let _ = self.0.kill();
		let _ = self.0.wait();
	}
}

/// `Ref::connect` as soon as the peer has got as far as binding
async fn connect_when_ready(path: &Path) -> Ref {
	let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
	loop {
		if let Ok(node_ref) = Ref::connect(path).await {
			return node_ref;
		}
		assert!(
			tokio::time::Instant::now() < deadline,
			"the peer process never bound {}",
			path.display()
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

/// signals once the child's first message lands
struct Announcing {
	tx: tokio::sync::mpsc::UnboundedSender<()>,
}
impl Handler for Announcing {
	async fn handle(&self, _d: &mut [u8], _f: FdVec, _c: Option<strong_ipc::UCred>) {
		let _ = self.tx.send(());
	}
}

/// a `Ref` notices the *process* behind it dying, not just a cooperative drop
///
/// the killed-peer mirror of lifecycle's `a_connected_ref_notices_the_server_going_away`.
/// Nothing runs on the far side: the node is not dropped, the binding is not unlinked,
/// and the runtime is not shut down. All that happens is the kernel closing a dead
/// process's descriptors — which is what `is_dead`'s `ppoll` and the death watch's
/// `EPOLLHUP` are reading, and the only notice a real crash ever gives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ref_notices_the_process_behind_it_being_killed() {
	let path = socket_path("killed-node");
	let mut peer = Peer::spawn("node", &path);
	let node_ref = connect_when_ready(&path).await;

	assert!(
		!node_ref.is_dead(),
		"a live peer process's ref reported dead"
	);
	assert!(
		tokio::time::timeout(Duration::from_millis(100), node_ref.death_notification())
			.await
			.is_err(),
		"death fired while the peer process was still running"
	);

	// registered with the reactor *before* the kill, so this is the watch noticing a
	// hangup rather than `death_notification`'s opening `is_dead` shortcut answering
	let notified = tokio::spawn({
		let node_ref = node_ref.clone();
		async move { node_ref.death_notification().await }
	});
	tokio::time::sleep(Duration::from_millis(50)).await;

	peer.kill();

	tokio::time::timeout(Duration::from_secs(5), notified)
		.await
		.expect("death_notification never resolved after the peer process was killed")
		.unwrap();
	// `is_dead` is the bare `ppoll` and never consults the watch, so this is the
	// unregistered path agreeing with the registered one about a peer that never got the
	// chance to hang up politely
	assert!(
		node_ref.is_dead(),
		"is_dead disagrees with death_notification after a killed peer"
	);
	// and the same condition reaches a caller who never asked either question
	assert!(matches!(
		node_ref.try_send(Message::from_data(b"?".to_vec())),
		Err(TrySendError::Closed(_))
	));
}

/// a `Node` notices the *process* holding its last `Ref` dying
///
/// the killed-peer mirror of lifecycle's `a_node_notices_its_last_ref_going_away`, and the
/// half that matters most in practice: a service outlives its clients, so the death it has
/// to detect is almost always somebody else's crash rather than an orderly disconnect.
/// Here the last capability to this node lives in another process, which is killed — so
/// the only thing that can end the receive loop is the descriptor going with the process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_notices_the_process_holding_its_last_ref_being_killed() {
	let path = socket_path("killed-ref");
	let (tx, mut announced) = tokio::sync::mpsc::unbounded_channel();
	let (node, node_ref) = Node::new(Announcing { tx }).unwrap();
	// the binding hands out clones of this capability; in-process they intern back to the
	// one `Ref`, so both it and the binding have to go before the child is the only holder
	let binding = RefFsBinding::new(node_ref.clone(), &path).unwrap();

	let mut peer = Peer::spawn("ref", &path);
	tokio::time::timeout(Duration::from_secs(20), announced.recv())
		.await
		.expect("the peer process never reached us")
		.expect("channel closed");

	// every capability on this side goes; the child's descriptor is now the only one
	drop(binding);
	drop(node_ref);
	assert!(
		tokio::time::timeout(Duration::from_millis(200), node.death_notification())
			.await
			.is_err(),
		"the node gave up while another process still held a Ref to it"
	);
	assert!(!node.is_dead());

	peer.kill();

	tokio::time::timeout(Duration::from_secs(5), node.death_notification())
		.await
		.expect("the node never noticed the process holding its last Ref being killed");
	assert!(node.is_dead(), "is_dead disagrees with death_notification");
}
