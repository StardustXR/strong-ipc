//! "the other end is gone" — one flag, one wakeup
//!
//! this is the *observed* kind of death, used by [`Node`]: something is already watching
//! the socket (the receive loop), so when it stops there is a moment to record, and the
//! answer is a plain atomic afterwards.
//!
//! [`Ref`] deliberately does **not** use this. Nothing watches a `Ref`'s socket — that is
//! the whole point of the unregistered fast path — so there is no loop to notice a
//! hangup and set a flag. It asks the kernel instead, in [`crate::wire::is_hung_up`] and
//! [`crate::wire::Reactive::hangup`].
//!
//! [`Node`]: crate::Node
//! [`Ref`]: crate::Ref

use crate::Ref;
use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;

/// an owned handle that resolves when a node dies, from either side of it
///
/// the two sides watch by different means — a node has this module's latch, which its
/// receive loop sets on the way out, while a [`Ref`] registers its fd with the reactor
/// and parks on hangup readiness. Neither borrows what it watches, so both can be handed
/// over and awaited in a spawned task; this is the one type that covers both, so a caller
/// can wait on "whatever this is" without knowing which it got.
///
/// what holding one says about lifetime depends on where it came from, and that
/// difference is worth knowing even though it is not visible from out here:
///
/// - from [`crate::Node::death_notifier`] it shares the death and nothing else, so it
///   decides nothing about how long the node lives. Hold it beside a `Weak` when you want
///   the node to die on its own terms and to hear about it when it does.
/// - from [`Ref::death_notifier`] it *is* a ref, and a ref keeps the node it names
///   reachable. Across a process boundary that costs nothing you wanted: the peer can
///   still crash, exit, or drop its node, and this fires. In-process it pins the node
///   open, which is the ordinary meaning of holding a ref.
#[derive(Clone, Debug)]
pub struct DeathNotifier(Watched);

/// which of the two watches this is — private, so [`Death`] need never be public
#[derive(Clone, Debug)]
enum Watched {
	/// the death of a node in this process, held without holding the node
	Node(Arc<Death>),
	/// the death of whatever a ref names, watched by holding the ref
	Ref(Ref),
}

impl DeathNotifier {
	pub(crate) fn node(death: Arc<Death>) -> Self {
		Self(Watched::Node(death))
	}

	pub(crate) fn node_ref(node_ref: Ref) -> Self {
		Self(Watched::Ref(node_ref))
	}

	/// has it died? never false again once true
	pub fn is_dead(&self) -> bool {
		match &self.0 {
			Watched::Node(death) => death.is_dead(),
			Watched::Ref(node_ref) => node_ref.is_dead(),
		}
	}

	/// parks until it dies, returning straight away if it already has
	pub async fn wait(&self) {
		match &self.0 {
			Watched::Node(death) => death.wait().await,
			Watched::Ref(node_ref) => node_ref.death_notification().await,
		}
	}
}

/// a one-way latch: not dead, then dead, never back again
///
/// reached only through a [`DeathNotifier`], which is what a caller outside this crate
/// waits on: the latch itself has no public surface of its own.
pub(crate) struct Death {
	dead: AtomicBool,
	notify: Notify,
}

/// `dead` is the only thing about a death worth printing, and it is an atomic load
impl std::fmt::Debug for Death {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Death")
			.field("dead", &self.is_dead())
			.finish()
	}
}

impl Death {
	pub(crate) fn new() -> Self {
		Self {
			dead: AtomicBool::new(false),
			notify: Notify::new(),
		}
	}

	/// has the node this belongs to stopped receiving? see [`crate::Node::is_dead`]
	pub(crate) fn is_dead(&self) -> bool {
		self.dead.load(Ordering::Relaxed)
	}

	/// parks until [`Death::declare`], or returns straight away if it already happened
	///
	/// the ordering here is load-bearing. `Notified` does not register with the `Notify`
	/// until it is first polled, so checking the flag and *then* awaiting would lose a
	/// `declare` landing in between and park forever. `enable()` registers up front, so
	/// the check happens with the waiter already in place.
	pub(crate) async fn wait(&self) {
		let mut notified = std::pin::pin!(self.notify.notified());
		notified.as_mut().enable();
		if self.is_dead() {
			return;
		}
		notified.await;
	}

	fn declare(&self) {
		self.dead.store(true, Ordering::Relaxed);
		self.notify.notify_waiters();
	}

	/// a guard that declares this death when it drops
	///
	/// held *inside* the task rather than called at the end of it, so the death is
	/// recorded however the task ends: a clean hangup, an io error, a panic, or the
	/// abort that comes with dropping the node
	pub(crate) fn tombstone(self: &Arc<Self>) -> Tombstone {
		Tombstone(self.clone())
	}
}

pub(crate) struct Tombstone(Arc<Death>);

impl Drop for Tombstone {
	fn drop(&mut self) {
		self.0.declare();
	}
}
