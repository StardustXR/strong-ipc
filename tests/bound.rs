//! `RefFsBinding` — the path-bound door you knock on before you hold any capability
//!
//! a binding is not a node. It owns no handler and receives nothing: it holds one `Ref`
//! to a node of yours, listens on a path, and hands a clone of that capability to whoever
//! connects. So "the server" in these tests is always a pair — the `Node` doing the work
//! and the binding advertising it — and both have to go for the door to close.
//!
//! the path is the one name in this crate that is not itself a capability, which is why
//! it needs a story for two processes wanting the same one. That story is a lock file
//! beside the socket: whoever holds it owns the path, and a path whose lock nobody holds
//! is stale by definition and gets cleaned up rather than refused.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use strong_ipc::{FdVec, Handler, Message, Node, Ref, RefFsBinding};

/// a socket path unique to this process and test
fn socket_path(name: &str) -> PathBuf {
	let p = std::env::temp_dir().join(format!(
		"strong-ipc-test-{}-{name}.sock",
		std::process::id()
	));
	let _ = std::fs::remove_file(&p);
	let _ = std::fs::remove_file(lock_path(&p));
	p
}

/// the lock beside the socket, as [`RefFsBinding`] names it
fn lock_path(path: &std::path::Path) -> PathBuf {
	let mut lock = path.as_os_str().to_owned();
	lock.push(".lock");
	lock.into()
}

/// replies on the capability it was handed, tagging the reply with a per-node id
struct Echo {
	tag: &'static str,
	served: AtomicU32,
}

impl Handler for Echo {
	async fn handle(&self, data: &mut [u8], fds: FdVec, _creds: Option<strong_ipc::UCred>) {
		self.served.fetch_add(1, Ordering::Relaxed);
		let Some(fd) = fds.into_iter().next() else {
			return;
		};
		let reply = Ref::from_owned_fd(fd);
		let mut out = self.tag.as_bytes().to_vec();
		out.extend_from_slice(data);
		let _ = reply.try_send(Message::from_data(out));
	}
}

fn echo(tag: &'static str) -> Echo {
	Echo {
		tag,
		served: AtomicU32::new(0),
	}
}

/// a node behind a path: the handler that answers, and the binding that advertises it
///
/// the `Node` is handed back rather than kept inside the binding because it is a separate
/// lifetime — the binding holds a capability to it, not the node itself
fn serve(path: &std::path::Path, handler: Echo) -> (Node<Echo>, RefFsBinding) {
	let (node, node_ref) = Node::new(handler).unwrap();
	let binding = RefFsBinding::new(node_ref, path).unwrap();
	(node, binding)
}

struct Collector {
	tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
}
impl Handler for Collector {
	async fn handle(&self, data: &mut [u8], _fds: FdVec, _creds: Option<strong_ipc::UCred>) {
		let _ = self.tx.send(data.to_vec());
	}
}

fn collector() -> (
	Node<Collector>,
	Ref,
	tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
) {
	let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
	let (node, node_ref) = Node::new(Collector { tx }).unwrap();
	(node, node_ref, rx)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connect_and_echo_over_a_bound_socket() {
	let path = socket_path("echo");
	let (_server, _binding) = serve(&path, echo("S:"));

	let server_ref = Ref::connect(&path).await.unwrap();
	let (_mine, mine_ref, mut rx) = collector();

	let mut message = Message::from_data(b"knock".to_vec());
	message.add_ref(&mine_ref);
	server_ref.try_send(message).unwrap();

	let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
		.await
		.expect("timed out")
		.expect("channel closed");
	assert_eq!(got, b"S:knock");
}

/// a path somebody is still holding is not ours to take over
///
/// taking it would yank the door out from under a live server: the path would name our
/// socket while every capability already handed out still led to theirs. The lock file is
/// what makes that a refusal rather than a silent theft.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_a_held_path_fails_rather_than_clobbering() {
	let path = socket_path("occupied");
	let (_first_node, first) = serve(&path, echo("A:"));

	let (_second_node, second_ref) = Node::new(echo("B:")).unwrap();
	match RefFsBinding::new(second_ref, &path) {
		Err(e) => assert_eq!(
			e.kind(),
			std::io::ErrorKind::AddrInUse,
			"expected AddrInUse, got {e:?}"
		),
		Ok(_) => panic!("the second binding clobbered the first"),
	}

	// and the first is untouched by the attempt: same path, still its node answering
	let server_ref = Ref::connect(&path).await.unwrap();
	let (_mine, mine_ref, mut rx) = collector();
	let mut message = Message::from_data(b"knock".to_vec());
	message.add_ref(&mine_ref);
	server_ref.try_send(message).unwrap();
	let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
		.await
		.expect("timed out")
		.expect("channel closed");
	assert_eq!(got, b"A:knock", "the refused binding stole the path anyway");

	// the refusal is about the *holder*, not the path: once it lets go, the path is free
	drop(first);
	let (_third_node, third_ref) = Node::new(echo("B:")).unwrap();
	RefFsBinding::new(third_ref, &path).expect("the path stayed claimed after its holder went");
}

/// a path nobody holds is stale, and stale is ours to clean up
///
/// the counterpart to the refusal above, and the reason it is a lock file rather than a
/// mere existence check: a server that is killed leaves its socket — and possibly its
/// lock — behind, but leaves nothing holding the lock. Refusing that would mean a crash
/// locks the path out until somebody deletes a file by hand.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_path_left_behind_by_a_dead_holder_is_reclaimed() {
	let path = socket_path("stale");

	// what a killed process leaves: a real socket file, and a lock nobody is holding
	let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
	drop(stale); // std does not unlink on drop, which is exactly what we want here
	std::fs::write(lock_path(&path), b"").unwrap();
	assert!(path.exists(), "the stale socket file went away on its own");

	let (_server, _binding) = serve(&path, echo("R:"));

	// not merely bound — actually reachable, so the new socket really is the one the path
	// now names
	let server_ref = Ref::connect(&path).await.unwrap();
	let (_mine, mine_ref, mut rx) = collector();
	let mut message = Message::from_data(b"knock".to_vec());
	message.add_ref(&mine_ref);
	server_ref.try_send(message).unwrap();
	let got = tokio::time::timeout(Duration::from_secs(5), rx.recv())
		.await
		.expect("timed out")
		.expect("channel closed");
	assert_eq!(got, b"R:knock");
}

/// the binding owns the files, so dropping it takes both of them with it
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_the_binding_unlinks_the_socket_file() {
	let path = socket_path("unlink");
	{
		let (_server, _binding) = serve(&path, echo("U:"));
		assert!(path.exists(), "binding did not create the socket file");
		assert!(lock_path(&path).exists(), "binding did not create its lock");
	}
	assert!(
		!path.exists(),
		"the socket file outlived the RefFsBinding that created it"
	);
	assert!(
		!lock_path(&path).exists(),
		"the lock file outlived the RefFsBinding that created it"
	);
}

/// every connection reaches the one node, and its one handler
///
/// a binding hands out clones of a single capability rather than accepting a connection
/// per client and building something behind each, so "sharing the handler" is not a
/// coincidence of routing here — there is exactly one node, and this is what proves the
/// second connection did not quietly get a second one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_share_one_handler() {
	let path = socket_path("shared");
	let (server, _binding) = serve(&path, echo("S:"));

	let a = Ref::connect(&path).await.unwrap();
	let b = Ref::connect(&path).await.unwrap();
	let (_mine, mine_ref, mut rx) = collector();

	for (r, body) in [(&a, &b"one"[..]), (&b, &b"two"[..])] {
		let mut m = Message::from_data(body.to_vec());
		m.add_ref(&mine_ref);
		r.try_send(m).unwrap();
	}

	let mut got = Vec::new();
	for _ in 0..2 {
		got.push(
			tokio::time::timeout(Duration::from_secs(5), rx.recv())
				.await
				.expect("timed out")
				.expect("channel closed"),
		);
	}
	got.sort();
	assert_eq!(got, vec![b"S:one".to_vec(), b"S:two".to_vec()]);
	assert_eq!(
		server.handler().served.load(Ordering::Relaxed),
		2,
		"both connections should have been served by the one handler"
	);
}
