//! a stalled receiver must not let a sender pile descriptors up in the kernel
//!
//! the kernel's in-flight count is per uid, so whatever these tests queue is counted
//! against dbus-broker and every wayland client of the person running them. the process
//! starts at a soft limit of 128, which makes the budget 16, and no test puts more than
//! 64 on the wire even if the budget is broken.
//!
//! every test holds `serial()` for its whole run: they share one process-wide budget, and
//! the probe lowers this process's fd limit, which a descriptor allocated on another
//! thread would trip over

use rustix::{
	io::Errno,
	net::{
		AddressFamily, RecvAncillaryBuffer, RecvFlags, SendAncillaryBuffer, SendAncillaryMessage,
		SendFlags, SocketFlags, SocketType, recvmsg, sendmsg, socketpair,
	},
};
use std::{
	io::{IoSlice, IoSliceMut},
	mem::MaybeUninit,
	os::fd::{AsFd, OwnedFd},
	sync::{Arc, Mutex as StdMutex, Once},
	time::Duration,
};
use strong_ipc::{FdVec, Handler, Message, Node, Ref, TrySendError};
use tokio::sync::{Mutex, MutexGuard, Semaphore};

const BUDGET: usize = 16;

async fn serial() -> MutexGuard<'static, ()> {
	static SERIAL: Mutex<()> = Mutex::const_new(());
	static LIMIT: Once = Once::new();
	let guard = SERIAL.lock().await;
	LIMIT.call_once(|| {
		strong_ipc::set_fd_limit(BUDGET * 8).unwrap();
	});
	guard
}

/// handles nothing until given permits, recording each message's sequence number and
/// how many fds rode along
#[derive(Clone)]
struct Stalled {
	permits: Arc<Semaphore>,
	seen: Arc<StdMutex<Vec<(u8, usize)>>>,
}
impl Stalled {
	fn new() -> Self {
		Self {
			permits: Arc::new(Semaphore::new(0)),
			seen: Arc::default(),
		}
	}
	fn seen(&self) -> Vec<(u8, usize)> {
		self.seen.lock().unwrap().clone()
	}
	async fn until_seen(&self, n: usize) {
		until(&format!("{n} messages to land"), || self.seen().len() >= n).await;
	}
}
impl Handler for Stalled {
	async fn handle(self: &Arc<Self>, data: &mut [u8], fds: FdVec, _: Option<strong_ipc::UCred>) {
		self.permits.acquire().await.unwrap().forget();
		self.seen.lock().unwrap().push((data[0], fds.len()));
	}
}

fn stalled() -> (Node<Stalled>, Ref, Stalled) {
	let s = Stalled::new();
	let (node, r) = Node::new(s.clone()).unwrap();
	(node, r, s)
}

fn carrying(seq: u8, fds: usize, cargo: &Ref) -> Message {
	let mut m = Message::from_data(vec![seq]);
	for _ in 0..fds {
		m.add_ref(cargo);
	}
	m
}

/// the same search as `examples/inflight.rs`
fn kernel_in_flight(pair: &(OwnedFd, OwnedFd)) -> usize {
	let hard = strong_ipc::fd_limit().unwrap();
	let refused = |limit| {
		strong_ipc::set_fd_limit(limit).unwrap();
		let fds = [pair.0.as_fd()];
		let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
		let mut control = SendAncillaryBuffer::new(&mut space);
		control.push(SendAncillaryMessage::ScmRights(&fds));
		let sent = sendmsg(
			&pair.0,
			&[IoSlice::new(&[0])],
			&mut control,
			SendFlags::DONTWAIT,
		);
		strong_ipc::set_fd_limit(hard).unwrap();
		match sent {
			Ok(_) => {
				let mut space = [MaybeUninit::uninit(); rustix::cmsg_space!(ScmRights(1))];
				let mut control = RecvAncillaryBuffer::new(&mut space);
				let _ = recvmsg(
					&pair.1,
					&mut [IoSliceMut::new(&mut [0])],
					&mut control,
					RecvFlags::CMSG_CLOEXEC,
				);
				drop(control.drain().collect::<Vec<_>>());
				false
			}
			Err(Errno::TOOMANYREFS) => true,
			Err(e) => panic!("probe failed: {e}"),
		}
	};
	if !refused(0) {
		return 0;
	}
	let (mut lo, mut hi) = (0, 1);
	while refused(hi) {
		lo = hi;
		hi *= 2;
	}
	while hi - lo > 1 {
		let mid = (lo + hi) / 2;
		if refused(mid) { lo = mid } else { hi = mid }
	}
	hi
}

fn probe() -> (OwnedFd, OwnedFd) {
	socketpair(
		AddressFamily::UNIX,
		SocketType::SEQPACKET,
		SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
		None,
	)
	.unwrap()
}

/// other processes of this uid move the count a little, so this leaves some slack
fn assert_within_budget(queued: usize) {
	assert!(
		(BUDGET / 2..=BUDGET + 8).contains(&queued),
		"{queued} descriptors in flight against a budget of {BUDGET}"
	);
}

/// a budget that is never paid back shows up as a wait that never ends, so every wait
/// in here is bounded to fail rather than hang
async fn within<T>(what: &str, f: impl Future<Output = T>) -> T {
	tokio::time::timeout(Duration::from_secs(5), f)
		.await
		.unwrap_or_else(|_| panic!("timed out waiting for {what}"))
}

async fn until(what: &str, done: impl Fn() -> bool) {
	within(what, async {
		while !done() {
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	})
	.await;
}

/// bigger than the whole budget, so it only goes once every debt before it is paid off
async fn assert_budget_free(cargo: &Ref) {
	let (_node, r, s) = stalled();
	s.permits.add_permits(1);
	within(
		"the budget to be paid back",
		r.send(carrying(0, BUDGET + 4, cargo)),
	)
	.await
	.ok()
	.unwrap();
	s.until_seen(1).await;
}

#[tokio::test]
async fn a_stalled_receiver_holds_the_sender_to_its_budget() {
	let _serial = serial().await;
	let probe = probe();
	let (_node, target, s) = stalled();
	let (_cargo, cargo, _) = stalled();
	let baseline = kernel_in_flight(&probe);

	let sender = tokio::spawn({
		let (target, cargo) = (target.clone(), cargo.clone());
		async move {
			for seq in 0..64 {
				assert!(target.send(carrying(seq, 1, &cargo)).await.is_ok());
			}
		}
	});
	tokio::time::sleep(Duration::from_millis(100)).await;

	// without the budget all 63 unread messages would be sitting here
	assert_within_budget(kernel_in_flight(&probe).saturating_sub(baseline));
	assert!(
		!sender.is_finished(),
		"the sender should be waiting for room"
	);

	s.permits.add_permits(1000);
	s.until_seen(64).await;
	within("the sender", sender).await.unwrap();
	assert_budget_free(&cargo).await;
}

#[tokio::test]
async fn the_budget_is_shared_by_every_ref_in_the_process() {
	let _serial = serial().await;
	let probe = probe();
	let (_cargo, cargo, _) = stalled();
	let receivers: Vec<_> = (0..4).map(|_| stalled()).collect();
	let baseline = kernel_in_flight(&probe);

	let senders: Vec<_> = receivers
		.iter()
		.map(|(_, r, _)| {
			let (r, cargo) = (r.clone(), cargo.clone());
			tokio::spawn(async move {
				for seq in 0..16 {
					assert!(r.send(carrying(seq, 1, &cargo)).await.is_ok());
				}
			})
		})
		.collect();
	tokio::time::sleep(Duration::from_millis(100)).await;

	// four receivers at 16 each would be 60 unread without a shared budget
	assert_within_budget(kernel_in_flight(&probe).saturating_sub(baseline));

	for (_, _, s) in &receivers {
		s.permits.add_permits(1000);
	}
	for sender in senders {
		within("the sender", sender).await.unwrap();
	}
	for (_, _, s) in &receivers {
		s.until_seen(16).await;
	}
	assert_budget_free(&cargo).await;
}

#[tokio::test]
async fn try_send_queues_past_the_budget_instead_of_failing() {
	let _serial = serial().await;
	let (_node, target, s) = stalled();
	let (_cargo, cargo, _) = stalled();

	// the first fills the budget by itself, so everything after it has to wait
	let mut sent = vec![(0, BUDGET)];
	target.try_send(carrying(0, BUDGET, &cargo)).ok().unwrap();
	for seq in 1..8 {
		let fds = (seq % 2) as usize;
		match target.try_send(carrying(seq, fds, &cargo)) {
			Ok(()) => sent.push((seq, fds)),
			Err(TrySendError::Full(_)) => panic!("an over-budget send was refused at {seq}"),
			Err(TrySendError::Closed(_)) => panic!("an over-budget send looked dead at {seq}"),
			Err(TrySendError::TooLarge(_)) => unreachable!(),
		}
	}

	s.permits.add_permits(1000);
	s.until_seen(sent.len()).await;
	assert_eq!(
		s.seen(),
		sent,
		"messages without fds overtook ones waiting on the budget"
	);
	assert_budget_free(&cargo).await;
}

#[tokio::test]
async fn a_dropped_ref_still_pays_its_debt() {
	let _serial = serial().await;
	let (cargo_node, cargo, _) = stalled();
	let (node, target, s) = stalled();
	for seq in 0..BUDGET as u8 {
		within("room", target.send(carrying(seq, 1, &cargo)))
			.await
			.ok()
			.unwrap();
	}
	drop(target);

	// the ledger is still watching the socket, and the node hasn't read its queue yet
	tokio::time::sleep(Duration::from_millis(50)).await;
	assert!(
		!node.is_dead(),
		"the node hung up with messages still unread"
	);

	s.permits.add_permits(1000);
	s.until_seen(BUDGET).await;
	until("the node to hang up once drained", || node.is_dead()).await;
	assert_budget_free(&cargo).await;
	drop(cargo_node);
}

#[tokio::test]
async fn a_receiver_that_dies_unread_frees_the_budget() {
	let _serial = serial().await;
	let (_cargo, cargo, _) = stalled();
	let (node, target, _) = stalled();
	for seq in 0..BUDGET as u8 {
		within("room", target.send(carrying(seq, 1, &cargo)))
			.await
			.ok()
			.unwrap();
	}

	// the kernel discards a closed socket's queue, and the descriptors in it with it
	drop(node);
	assert_budget_free(&cargo).await;
	drop(target);
}

#[tokio::test]
async fn a_message_bigger_than_the_budget_still_goes() {
	let _serial = serial().await;
	let (_cargo, cargo, _) = stalled();
	assert_budget_free(&cargo).await;
}
