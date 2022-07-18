use std::pin::Pin;
use std::task::Poll;

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
    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<Self::Item>) -> NextReadyChunk<Self>
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

    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<S::Item>) -> NextReadyChunk<Self>
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

/// Wrapper over async task JoinHandle that aborts the task if it is dropped.
/// Can also be awaited (as a JoinHandle) to wait for task completion and
/// result. Note that just dropping the AbortHandle or calling abort() does not
/// mean the task is finished immediately.
pub struct AbortHandle<T>(tokio::task::JoinHandle<T>);

impl<T> AbortHandle<T> {
    /// wrap an existing join handle
    pub fn new(join_handle: tokio::task::JoinHandle<T>) -> Self {
        Self(join_handle)
    }

    /// tokio::spawn the future, returning its AbortHandle
    pub fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let jh = tokio::spawn(future);
        Self(jh)
    }

    /// abort the task
    pub fn abort(&self) {
        self.0.abort()
    }
}

impl<T> Future for AbortHandle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.0.poll_unpin(cx)
    }
}

impl<T> Drop for AbortHandle<T> {
    fn drop(&mut self) {
        self.0.abort()
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

/// Wrapper over tokio RwLock, instrumented with printing for debugging.
pub struct RwLock<T>(tokio::sync::RwLock<T>);
pub struct RwLockWriteGuard<'a, T>(u32, tokio::sync::RwLockWriteGuard<'a, T>);
pub struct RwLockReadGuard<'a, T>(u32, tokio::sync::RwLockReadGuard<'a, T>);

impl<'a, T> Drop for RwLockReadGuard<'a, T> {
    fn drop(&mut self) {
        println!("=> unlocking read {}", self.0);
    }
}

impl<'a, T> Drop for RwLockWriteGuard<'a, T> {
    fn drop(&mut self) {
        println!("=> unlocking write {}", self.0);
    }
}

impl<'a, T> std::ops::Deref for RwLockReadGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.1.deref()
    }
}

impl<'a, T> std::ops::Deref for RwLockWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.1.deref()
    }
}

impl<'a, T> std::ops::DerefMut for RwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.1.deref_mut()
    }
}

impl<T> RwLock<T> {
    pub fn new(inner: T) -> Self {
        RwLock(tokio::sync::RwLock::new(inner))
    }

    pub async fn write(&self, line: u32) -> RwLockWriteGuard<'_, T> {
        println!("=> locking write {}", line);
        let g = self.0.write().await;
        println!("=> ok write {}", line);
        RwLockWriteGuard(line, g)
    }

    pub async fn read(&self, line: u32) -> RwLockReadGuard<'_, T> {
        println!("=> locking read {}", line);
        let g = self.0.read().await;
        println!("=> ok read {}", line);
        RwLockReadGuard(line, g)
    }
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
