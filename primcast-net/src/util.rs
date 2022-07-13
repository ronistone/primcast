use std::pin::Pin;
use std::task::Poll;

use futures::Future;
use futures::Stream;

use pin_project_lite::pin_project;

pub trait StreamExt2: Stream {
    /// Stream that yields (i.e., returns Poll::Pending) after n consecutive items that return without waiting.
    fn yield_after(self, n: usize) -> Yield<Self>
    where Self: Sized;
    /// Read the next (up to `max_items`) ready items into buf.
    /// It will block util at least one item is returned by the stream.
    /// Returns the number of items read (0 means the stream returned None).
    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<Self::Item>) -> NextReadyChunk<Self>
    where Self: Sized + Unpin;
}

impl<S: Stream> StreamExt2 for S {
    fn yield_after(self, n: usize) -> Yield<Self> where Self: Sized {
        assert!(n > 0);
        Yield {
            inner: self,
            count: 0,
            yield_after: n,
            next_item: None,
        }
    }

    fn next_ready_chunk<'a>(&'a mut self, max_items: usize, buf: &'a mut Vec<S::Item>) -> NextReadyChunk<Self>
    where Self: Sized + Unpin
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

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        debug_assert!(self.max_items > 0);

        let s = Pin::new(&mut self.inner);
        match s.poll_next(cx) {
            Poll::Ready(Some(it)) => {
                self.buf.push(it);
            },
            Poll::Ready(None) => return Poll::Ready(0),
            Poll::Pending => return Poll::Pending,
        }

        let mut count = 1;

        while count < self.max_items {
            let s = Pin::new(&mut self.inner);
            if let Poll::Ready(Some(it)) = s.poll_next(cx) {
                self.buf.push(it);
                count += 1;
            } else {
                break;
            }
        }

        return Poll::Ready(count);
    }
}

#[cfg(test)]
mod tests {
    use super::StreamExt2;

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
