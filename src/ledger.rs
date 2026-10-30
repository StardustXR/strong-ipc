//! the budget on descriptors sent but not yet received
//!
//! the kernel counts those per uid, across every process, and refuses a sender's
//! SCM_RIGHTS with ETOOMANYREFS once the count passes that sender's soft RLIMIT_NOFILE.
//! strong-ipc raises its own limit far past the 1024 the rest of the session runs at, so
//! left alone it can fill the count without ever feeling it, and dbus-broker and every
//! wayland client fall over instead.
//!
//! so each process keeps what it has on the wire under an eighth of the limit it
//! inherited. descriptors the far end has received live in its own fd table and cost
//! nothing here, this only caps how many are queued at once.
//!
//! a socket's debt is only known to be paid once `SIOCOUTQ` reads empty, since that is
//! the moment the receiver has dequeued everything it sent. a sender settles its own debt
//! as it goes. when the budget runs out every outstanding debt goes to the ledger thread,
//! which watches those sockets with an epoll of its own and pays each one off as it drains.

use crate::{capability, wire};
use rustix::{
	buffer::spare_capacity,
	event::epoll::{self, CreateFlags, EventData, EventFlags},
};
use std::{
	collections::HashMap,
	os::fd::{AsFd, BorrowedFd, OwnedFd},
	pin::pin,
	sync::{
		LazyLock, Mutex, PoisonError,
		atomic::{AtomicUsize, Ordering},
	},
};
use tokio::sync::Notify;

static IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
static WAITING: AtomicUsize = AtomicUsize::new(0);
static RELEASED: Notify = Notify::const_new();

fn budget() -> usize {
	static BUDGET: LazyLock<usize> = LazyLock::new(|| (wire::inherited_fd_limit() / 8).max(1));
	*BUDGET
}

/// a message bigger than the whole budget still goes once nothing else is in flight,
/// otherwise a 253 fd message could never be sent at all
fn take(n: usize) -> bool {
	n == 0
		|| IN_FLIGHT
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |f| {
				(f == 0 || f + n <= budget()).then_some(f + n)
			})
			.is_ok()
}

pub(crate) fn release(n: usize) {
	if n == 0 {
		return;
	}
	IN_FLIGHT.fetch_sub(n, Ordering::AcqRel);
	if WAITING.load(Ordering::Acquire) != 0 {
		RELEASED.notify_waiters();
	}
}

pub(crate) fn try_claim(n: usize) -> bool {
	take(n) || {
		capability::sweep();
		take(n)
	}
}

pub(crate) async fn claim(n: usize) {
	if try_claim(n) {
		return;
	}
	struct Waiting;
	impl Drop for Waiting {
		fn drop(&mut self) {
			WAITING.fetch_sub(1, Ordering::AcqRel);
		}
	}
	WAITING.fetch_add(1, Ordering::AcqRel);
	let _waiting = Waiting;
	loop {
		let mut released = pin!(RELEASED.notified());
		released.as_mut().enable();
		if try_claim(n) {
			return;
		}
		released.await;
	}
}

/// sock_wfree wakes writers before it drops the last byte of an skb's charge, so a socket
/// woken by its final read can still report 1, and no real message is that small
fn drained(fd: BorrowedFd<'_>) -> bool {
	wire::unsent(fd).map_or(true, |n| n <= 1)
}

/// descriptors one socket has sent that may still be queued at the far end
#[derive(Default)]
pub(crate) struct Debt(AtomicUsize);
impl Debt {
	pub(crate) fn incur(&self, n: usize) {
		self.0.fetch_add(n, Ordering::AcqRel);
	}

	/// only what was owed before the check is paid, anything sent since may still be queued
	pub(crate) fn settle(&self, fd: BorrowedFd<'_>) -> bool {
		let owed = self.0.load(Ordering::Acquire);
		if owed == 0 {
			return true;
		}
		if !drained(fd) {
			return false;
		}
		let paid = self
			.0
			.compare_exchange(owed, 0, Ordering::AcqRel, Ordering::Acquire)
			.is_ok();
		if paid {
			release(owed);
		}
		paid
	}

	pub(crate) fn hand_over(&self, fd: BorrowedFd<'_>) {
		if self.settle(fd) {
			return;
		}
		let owed = self.0.swap(0, Ordering::AcqRel);
		if owed != 0 {
			adopt(fd, owed);
		}
	}
}

struct Ledger {
	epoll: OwnedFd,
	/// a dup of every socket still owing, keyed by inode, held so a dropped `Ref`'s debt
	/// can still be watched, and so the far end only sees the hangup after it has read
	/// everything anyway
	watched: Mutex<HashMap<u64, (OwnedFd, usize)>>,
}

static LEDGER: LazyLock<Option<Ledger>> = LazyLock::new(|| {
	let ledger = Ledger {
		epoll: epoll::create(CreateFlags::CLOEXEC).ok()?,
		watched: Mutex::default(),
	};
	std::thread::Builder::new()
		.name("strong-ipc-ledger".into())
		.spawn(|| {
			if let Some(ledger) = LEDGER.as_ref() {
				ledger.run();
			}
		})
		.ok()?;
	Some(ledger)
});

/// with no ledger to watch it, a debt is forgiven rather than held against the budget forever
fn adopt(fd: BorrowedFd<'_>, n: usize) {
	let Some(ledger) = LEDGER.as_ref() else {
		return release(n);
	};
	let Ok(ino) = rustix::fs::fstat(fd).map(|s| s.st_ino) else {
		return release(n);
	};
	let mut watched = ledger
		.watched
		.lock()
		.unwrap_or_else(PoisonError::into_inner);
	if let Some((_, owed)) = watched.get_mut(&ino) {
		*owed += n;
		return;
	}
	let Ok(dup) = fd.try_clone_to_owned() else {
		return release(n);
	};
	// edge triggered, since a unix socket wakes its writers on every message the far end
	// reads, and adding one that has already drained reports it straight away
	let flags = EventFlags::OUT | EventFlags::ET;
	if epoll::add(&ledger.epoll, &dup, EventData::new_u64(ino), flags).is_err() {
		return release(n);
	}
	watched.insert(ino, (dup, n));
}

impl Ledger {
	fn run(&self) {
		let mut events = Vec::with_capacity(64);
		loop {
			events.clear();
			if epoll::wait(&self.epoll, spare_capacity(&mut events), None).is_err() {
				continue;
			}
			let mut watched = self.watched.lock().unwrap_or_else(PoisonError::into_inner);
			for event in &events {
				let ino = event.data.u64();
				if !watched.get(&ino).is_some_and(|(fd, _)| drained(fd.as_fd())) {
					continue;
				}
				let Some((fd, owed)) = watched.remove(&ino) else {
					continue;
				};
				// closing the dup alone wouldn't unregister it while the `Ref` still holds
				// the same socket open
				let _ = epoll::delete(&self.epoll, &fd);
				release(owed);
			}
		}
	}
}
