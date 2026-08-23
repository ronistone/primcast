use std::pin::Pin;
use std::task::Poll;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use futures::prelude::*;
use futures::ready;

use tokio::sync::oneshot;

use pin_project_lite::pin_project;

pub trait StreamExt2: Stream {
    /// Stream that yields (i.e., returns Poll::Pending) after n consecutive items that return without waiting.
    fn yield_after(self, n: usize) -> Yield<Self>
    where
        Self: Sized;
    /// Read the next (up to `max_items`) ready items into buf.
    /// It will block util at least one item is returned by the stream.
    /// Returns the number of items read (0 means the stream returned None).
    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<Self::Item>) -> NextReadyChunk<'a, Self>
    where
        Self: Sized + Unpin;
}

impl<S: Stream> StreamExt2 for S {
    fn yield_after(self, n: usize) -> Yield<Self>
    where
        Self: Sized,
    {
        assert!(n > 0);
        Yield {
            inner: self,
            count: 0,
            yield_after: n,
            next_item: None,
        }
    }

    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<S::Item>) -> NextReadyChunk<'a, Self>
    where
        Self: Sized + Unpin,
    {
        assert!(max_items > 0);
        NextReadyChunk {
            inner: self,
            buf,
            max_items,
        }
    }
}

pin_project! {
    /// Stream for the [`yield_after`](StreamExt2::yield_after) method.
    pub struct Yield<S: Stream> {
        #[pin]
        inner: S,
        count: usize,
        yield_after: usize,
        next_item: Option<S::Item>,
    }
}

impl<S: Stream> Stream for Yield<S> {
    type Item = S::Item;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if let Some(it) = this.next_item.take() {
            // return cached item from previous yield
            *this.count += 1;
            return Poll::Ready(Some(it));
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(it)) if this.count >= this.yield_after => {
                // forced yield
                *this.count = 0;
                *this.next_item = Some(it);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(it)) => {
                *this.count += 1;
                Poll::Ready(Some(it))
            }
            poll => {
                // end or natural yield
                *this.count = 0;
                poll
            }
        }
    }
}

pub struct NextReadyChunk<'a, S: Stream> {
    inner: &'a mut S,
    buf: &'a mut Vec<S::Item>,
    max_items: usize,
}

impl<'a, S: Stream + Unpin> Future for NextReadyChunk<'a, S> {
    type Output = usize;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        debug_assert!(self.max_items > 0);
        let this = self.get_mut();

        // read at least one element
        match ready!(this.inner.poll_next_unpin(cx)) {
            Some(it) => {
                this.buf.push(it);
            }
            None => return Poll::Ready(0),
        }
        let mut count = 1;

        // read at most max_items ready elements
        while count < this.max_items {
            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(it)) => {
                    this.buf.push(it);
                    count += 1;
                }
                _ => break,
            }
        }

        return Poll::Ready(count);
    }
}

/// When dropped, makes the related Shutdown futures complete.
pub struct ShutdownHandle(oneshot::Sender<()>);

impl ShutdownHandle {
    /// Manually trigger shutdown without waiting for this handle to be dropped
    pub fn shutdown(self) {
        let _ = self.0.send(());
    }
}

#[derive(Clone)]

/// Future that can be cloned to be waited on by multiple tasks. All copies complete when
/// the related ShutdownHandle is dropped.
pub struct Shutdown(future::Shared<oneshot::Receiver<()>>);

impl Future for Shutdown {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx).map(|_| ())
    }
}

impl Shutdown {
    /// Create a new Shutdown/ShutdownHandle pair.
    pub fn new() -> (Self, ShutdownHandle) {
        let (tx, rx) = oneshot::channel();
        (Shutdown(rx.shared()), ShutdownHandle(tx))
    }
}

// DEBUG: correlates AbortHandle::abort()/Drop (the *request* to cancel) with
// whether the spawned future's own Drop glue (the *actual* teardown, which
// releases any guard/permit it's holding) ever runs. tokio::task::JoinHandle::
// abort() only schedules a final poll to tear the task down; it is not
// synchronous. If we see "abort requested" for an id but never see the
// matching "future dropped", the final poll-and-drop never happened —
// confirming a missed-wakeup/stuck-executor rather than a leaked guard in
// application code. See project_leader_fail_stall investigation.
static ABORT_HANDLE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pin_project! {
    struct DropCanary<F> {
        #[pin]
        inner: F,
        id: u64,
        caller: &'static std::panic::Location<'static>,
    }

    impl<F> PinnedDrop for DropCanary<F> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            primcast_core::timed_print!(
                "[ABORTHANDLE] task {} (spawned @ {}) future DROPPED (final teardown ran)",
                this.id, this.caller
            );
        }
    }
}

impl<F: Future> Future for DropCanary<F> {
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// Wrapper over async task JoinHandle that aborts the task if it is dropped.
/// Can also be awaited (as a JoinHandle) to wait for task completion and
/// result. Note that just dropping the AbortHandle or calling abort() does not
/// mean the task is finished immediately.
pub struct AbortHandle<T> {
    jh: tokio::task::JoinHandle<T>,
    id: u64,
    caller: &'static std::panic::Location<'static>,
}

impl<T> AbortHandle<T> {
    /// wrap an existing join handle
    #[track_caller]
    pub fn new(join_handle: tokio::task::JoinHandle<T>) -> Self {
        Self {
            jh: join_handle,
            id: ABORT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            caller: std::panic::Location::caller(),
        }
    }

    /// tokio::spawn the future, returning its AbortHandle
    #[track_caller]
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let id = ABORT_HANDLE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let caller = std::panic::Location::caller();
        primcast_core::timed_print!("[ABORTHANDLE] task {} spawned @ {}", id, caller);
        let jh = tokio::spawn(DropCanary { inner: future, id, caller });
        Self { jh, id, caller }
    }

    /// abort the task
    pub fn abort(&self) {
        primcast_core::timed_print!(
            "[ABORTHANDLE] task {} (spawned @ {}) abort() requested",
            self.id, self.caller
        );
        self.jh.abort()
    }
}

impl<T> Future for AbortHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.jh.poll_unpin(cx)
    }
}

impl<T> Drop for AbortHandle<T> {
    fn drop(&mut self) {
        primcast_core::timed_print!(
            "[ABORTHANDLE] task {} (spawned @ {}) AbortHandle dropped -> abort() requested",
            self.id, self.caller
        );
        self.jh.abort()
    }
}

pub struct RoundRobinStreams<S> {
    next: usize,
    inner: Vec<S>,
}

impl<S> RoundRobinStreams<S>
where
    S: Stream + Unpin,
{
    pub fn new() -> Self {
        Self { next: 0, inner: vec![] }
    }

    pub fn push(&mut self, stream: S) {
        self.inner.push(stream);
    }
}

impl<S> Stream for RoundRobinStreams<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.len() == 0 {
            return Poll::Pending;
        }
        let this = self.get_mut();
        this.next %= this.inner.len();
        let mut done = vec![];
        for _ in 0..this.inner.len() {
            let current = this.next;
            this.next += 1;
            this.next %= this.inner.len();
            match this.inner.get_mut(current).unwrap().poll_next_unpin(cx) {
                r @ Poll::Ready(Some(_)) => return r,
                Poll::Ready(None) => {
                    done.push(current);
                }
                Poll::Pending => {}
            }
        }

        done.sort_unstable();
        while let Some(idx) = done.pop() {
            this.inner.remove(idx);
        }

        if this.inner.is_empty() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
/// Wrapper over tokio RwLock instrumented to track *who* holds/waits for the
/// lock, so a stall can be attributed to a specific call site.
///
/// Every acquisition records `file:line` + a timestamp in a side table; the
/// `[LOCKPROBE]` ticker dumps that table when the lock looks stuck.
pub struct RwLock<T> {
    inner: tokio::sync::RwLock<T>,
    track: std::sync::Mutex<Track>,
}

type TaskId = Option<tokio::task::Id>;

#[derive(Default)]
struct Track {
    next_id: u64,
    writer: Option<(u64, &'static str, Instant, TaskId)>,
    readers: std::collections::HashMap<u64, (&'static str, Instant, TaskId)>,
    waiting: std::collections::HashMap<u64, (&'static str, bool, Instant, TaskId, std::sync::Arc<WakeProbe>)>,
    /// Last guard release seen: (loc, when). If the lock looks stuck with no
    /// holder, this says when it was last actually free.
    last_release: Option<(&'static str, Instant)>,
    acquires: u64,
    releases: u64,
    /// Cumulative hold time and count per call site — who actually owns the
    /// lock's time budget under load.
    hold_stats: std::collections::HashMap<&'static str, (std::time::Duration, u64)>,
}

/// Counts how many times a pending lock-acquire future was woken, so a stall
/// can be classified: `wakes=0` means the semaphore never handed the permit
/// over (missing permits / never released), `wakes>0` with the future still
/// pending means the wake happened but the task was not re-polled.
#[derive(Default)]
pub struct WakeProbe {
    count: std::sync::atomic::AtomicU64,
    polls: std::sync::atomic::AtomicU64,
    waker: std::sync::Mutex<Option<std::task::Waker>>,
}

impl futures::task::ArcWake for WakeProbe {
    fn wake_by_ref(arc_self: &std::sync::Arc<Self>) {
        arc_self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let w = arc_self.waker.lock().unwrap().clone();
        if let Some(w) = w {
            w.wake();
        }
    }
}

pin_project! {
    /// Wraps a lock-acquire future: counts polls/wakes and removes the waiter
    /// bookkeeping entry if the future is dropped before completing (a select!
    /// branch losing the race), so the waiter list stays truthful.
    struct TrackedAcquire<'a, F, T> {
        #[pin]
        inner: F,
        lock: &'a RwLock<T>,
        id: u64,
        probe: std::sync::Arc<WakeProbe>,
        done: bool,
    }

    impl<'a, F, T> PinnedDrop for TrackedAcquire<'a, F, T> {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if !*this.done {
                this.lock.track.lock().unwrap().waiting.remove(this.id);
            }
        }
    }
}

impl<'a, F: Future, T> Future for TrackedAcquire<'a, F, T> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<F::Output> {
        let this = self.project();
        this.probe.polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        *this.probe.waker.lock().unwrap() = Some(cx.waker().clone());
        let w = futures::task::waker_ref(this.probe);
        let mut cx2 = std::task::Context::from_waker(&w);
        let res = this.inner.poll(&mut cx2);
        if res.is_ready() {
            *this.done = true;
        }
        res
    }
}

impl Track {
    /// Does `task` already hold this lock? Acquiring again from the same task
    /// deadlocks (tokio RwLock is neither reentrant nor reader-preferring: a
    /// second `read()` queues behind any waiting writer).
    fn held_by(&self, task: TaskId) -> Option<&'static str> {
        if task.is_none() {
            return None;
        }
        if let Some((_, loc, _, w)) = &self.writer {
            if *w == task {
                return Some(loc);
            }
        }
        self.readers
            .values()
            .find(|(_, _, t)| *t == task)
            .map(|(loc, _, _)| *loc)
    }
}

pub struct RwLockWriteGuard<'a, T> {
    id: u64,
    lock: &'a RwLock<T>,
    guard: tokio::sync::RwLockWriteGuard<'a, T>,
}

pub struct RwLockReadGuard<'a, T> {
    id: u64,
    lock: &'a RwLock<T>,
    guard: tokio::sync::RwLockReadGuard<'a, T>,
}

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        let mut t = self.lock.track.lock().unwrap();
        if let Some((loc, since, _)) = t.readers.remove(&self.id) {
            let now = Instant::now();
            t.releases += 1;
            t.last_release = Some((loc, now));
            let e = t.hold_stats.entry(loc).or_insert((std::time::Duration::ZERO, 0));
            e.0 += now.duration_since(since);
            e.1 += 1;
        }
    }
}

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        let mut t = self.lock.track.lock().unwrap();
        if t.writer.map(|(id, _, _, _)| id) == Some(self.id) {
            let (_, loc, since, _) = t.writer.unwrap();
            let now = Instant::now();
            t.writer = None;
            t.releases += 1;
            t.last_release = Some((loc, now));
            let e = t.hold_stats.entry(loc).or_insert((std::time::Duration::ZERO, 0));
            e.0 += now.duration_since(since);
            e.1 += 1;
        }
    }
}

impl<'a, T> std::ops::Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> std::ops::Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, T> std::ops::DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl<T> RwLock<T> {
    pub fn new(inner: T) -> Self {
        RwLock {
            inner: tokio::sync::RwLock::new(inner),
            track: std::sync::Mutex::new(Track::default()),
        }
    }

    fn begin_wait(&self, loc: &'static str, write: bool) -> (u64, std::sync::Arc<WakeProbe>) {
        let task = tokio::task::try_id();
        let mut t = self.track.lock().unwrap();
        if let Some(held_at) = t.held_by(task) {
            // Self-deadlock: this task is about to block on a lock it already
            // holds. Report it instead of hanging silently.
            eprintln!(
                "[LOCKDEADLOCK] task {:?} acquiring {} ({}) while already holding it from {held_at}",
                task,
                loc,
                if write { "write" } else { "read" },
            );
        }
        t.next_id += 1;
        let id = t.next_id;
        let probe = std::sync::Arc::new(WakeProbe::default());
        t.waiting.insert(id, (loc, write, Instant::now(), task, probe.clone()));
        (id, probe)
    }

    pub async fn write(&self, loc: &'static str) -> RwLockWriteGuard<'_, T> {
        let (id, probe) = self.begin_wait(loc, true);
        let guard = TrackedAcquire {
            inner: self.inner.write(),
            lock: self,
            id,
            probe,
            done: false,
        }
        .await;
        {
            let mut t = self.track.lock().unwrap();
            t.waiting.remove(&id);
            t.acquires += 1;
            t.writer = Some((id, loc, Instant::now(), tokio::task::try_id()));
        }
        RwLockWriteGuard { id, lock: self, guard }
    }

    pub async fn read(&self, loc: &'static str) -> RwLockReadGuard<'_, T> {
        let (id, probe) = self.begin_wait(loc, false);
        let guard = TrackedAcquire {
            inner: self.inner.read(),
            lock: self,
            id,
            probe,
            done: false,
        }
        .await;
        {
            let mut t = self.track.lock().unwrap();
            t.waiting.remove(&id);
            t.acquires += 1;
            t.readers.insert(id, (loc, Instant::now(), tokio::task::try_id()));
        }
        RwLockReadGuard { id, lock: self, guard }
    }

    pub fn try_write(&self, loc: &'static str) -> Result<RwLockWriteGuard<'_, T>, tokio::sync::TryLockError> {
        let guard = self.inner.try_write()?;
        let mut t = self.track.lock().unwrap();
        t.next_id += 1;
        let id = t.next_id;
        t.writer = Some((id, loc, Instant::now(), tokio::task::try_id()));
        t.acquires += 1;
        drop(t);
        Ok(RwLockWriteGuard { id, lock: self, guard })
    }

    pub fn try_read(&self, loc: &'static str) -> Result<RwLockReadGuard<'_, T>, tokio::sync::TryLockError> {
        let guard = self.inner.try_read()?;
        let mut t = self.track.lock().unwrap();
        t.next_id += 1;
        let id = t.next_id;
        t.readers.insert(id, (loc, Instant::now(), tokio::task::try_id()));
        t.acquires += 1;
        drop(t);
        Ok(RwLockReadGuard { id, lock: self, guard })
    }

    /// Cumulative hold time per call site, biggest first: where the lock's time
    /// actually goes under load (contention, not deadlock).
    pub fn hold_report(&self, top: usize) -> String {
        let t = self.track.lock().unwrap();
        let mut v: Vec<_> = t.hold_stats.iter().collect();
        v.sort_by_key(|(_, (d, _))| std::cmp::Reverse(*d));
        let total: std::time::Duration = t.hold_stats.values().map(|(d, _)| *d).sum();
        let mut out = format!("lock time total={:?}", total);
        for (loc, (d, n)) in v.into_iter().take(top) {
            out.push_str(&format!(
                "\n    {loc} held {:?} over {n} acquires (avg {:?})",
                d,
                d.checked_div(*n as u32).unwrap_or_default()
            ));
        }
        out
    }

    /// Human-readable snapshot: current writer, readers and waiters with ages.
    pub fn dump(&self) -> String {
        let t = self.track.lock().unwrap();
        let now = Instant::now();
        let mut out = String::new();
        match &t.writer {
            Some((_, loc, since, task)) => {
                out.push_str(&format!(
                    "writer={loc} task={task:?} held_for={:?}",
                    now.duration_since(*since)
                ));
            }
            None => out.push_str("writer=none"),
        }
        let mut readers: Vec<_> = t.readers.values().collect();
        readers.sort_by_key(|(_, since, _)| *since);
        out.push_str(&format!(" readers={}", readers.len()));
        for (loc, since, task) in readers.iter().take(8) {
            out.push_str(&format!(
                "\n    reader {loc} task={task:?} held_for={:?}",
                now.duration_since(*since)
            ));
        }
        match &t.last_release {
            Some((loc, when)) => out.push_str(&format!(
                "\n  acquires={} releases={} last_release={loc} {:?} ago",
                t.acquires,
                t.releases,
                now.duration_since(*when)
            )),
            None => out.push_str(&format!("\n  acquires={} releases={} last_release=never", t.acquires, t.releases)),
        }
        let mut waiting: Vec<_> = t.waiting.values().collect();
        waiting.sort_by_key(|(_, _, since, _, _)| *since);
        out.push_str(&format!("\n  waiting={}", waiting.len()));
        for (loc, write, since, task, probe) in waiting.iter().take(20) {
            out.push_str(&format!(
                "\n    waiter {loc} kind={} task={task:?} waiting_for={:?} polls={} wakes={}",
                if *write { "write" } else { "read" },
                now.duration_since(*since),
                probe.polls.load(std::sync::atomic::Ordering::Relaxed),
                probe.count.load(std::sync::atomic::Ordering::Relaxed),
            ));
        }
        out
    }
}

/// `file:line` of the call site, used to label lock acquisitions.
#[macro_export]
macro_rules! loc {
    () => {
        concat!(file!(), ":", line!())
    };
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundrobin_stream() {
        let mut rr = RoundRobinStreams::<Box<dyn Stream<Item = usize> + Unpin>>::new();
        rr.push(Box::new(futures::stream::repeat(1)));
        rr.push(Box::new(futures::stream::repeat(2)));
        rr.push(Box::new(futures::stream::repeat(3)));
        rr.push(Box::new(futures::stream::repeat(4)));
        rr.push(Box::new(futures::stream::repeat(5).take(3)));
        futures::executor::block_on(async {
            for _ in 0..3 {
                for i in 1..6 {
                    assert_eq!(Some(i), rr.next().await);
                }
            }
            // here, stream returning 5 was dropped
            for i in 1..5 {
                assert_eq!(Some(i), rr.next().await);
            }
            assert_eq!(Some(1), rr.next().await);
        });
    }

    #[test]
    fn test_yield_chunks() {
        let mut s = futures::stream::iter(0..10).yield_after(3);
        let mut buf = vec![];
        futures::executor::block_on(async {
            // chunk of size 1
            assert_eq!(1, s.next_ready_chunk(1, &mut buf).await);
            assert_eq!(buf.len(), 1);
            // will return only 2 since stream yields after 3 items
            assert_eq!(2, s.next_ready_chunk(3, &mut buf).await);
            assert_eq!(buf.len(), 3);
            // should return full chunk
            assert_eq!(3, s.next_ready_chunk(3, &mut buf).await);
            assert_eq!(buf.len(), 6);
            // will return 3 after stream yields
            assert_eq!(3, s.next_ready_chunk(4, &mut buf).await);
            assert_eq!(buf.len(), 9);
            // return last element
            assert_eq!(1, s.next_ready_chunk(4, &mut buf).await);
            assert_eq!(buf.len(), 10);
            // stream done
            assert_eq!(0, s.next_ready_chunk(4, &mut buf).await);
        });
    }
}
