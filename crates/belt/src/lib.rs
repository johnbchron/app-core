//! Provides [`Belt`], a byte streaming container.

mod bottleneck;
mod comp;
mod counter;
mod source;

/// A good default chunk size for [`Belt`]s. Not used anywhere in the library,
/// just for reference.
pub const DEFAULT_CHUNK_SIZE: NonZeroUsize =
  NonZeroUsize::new(32 * 1024).expect("DEFAULT_CHUNK_SIZE is zero");

use std::{
  io::{Cursor, Result},
  num::NonZeroUsize,
  pin::Pin,
  sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
  },
  task::{Context, Poll},
};

use bytes::Bytes;
pub use futures::{Stream, StreamExt};
use tokio::{io::AsyncBufRead, sync::mpsc};
use tokio_util::io::ReaderStream;

use self::{bottleneck::Bottleneck, source::BytesSource};
pub use self::{comp::CompressionAlgorithm, counter::Counter};

#[derive(Debug)]
enum MaybeBottleneckSource {
  Unmodified(BytesSource),
  Bottlenecked(Bottleneck<BytesSource>),
}

impl Stream for MaybeBottleneckSource {
  type Item = Result<Bytes>;

  fn poll_next(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    match &mut *self {
      Self::Unmodified(source) => source.poll_next_unpin(cx),
      Self::Bottlenecked(bottleneck) => bottleneck.poll_next_unpin(cx),
    }
  }
}

/// A byte stream container.
#[derive(Debug)]
pub struct Belt {
  inner:         MaybeBottleneckSource,
  count:         Arc<AtomicU64>,
  declared_comp: Option<CompressionAlgorithm>,
}

impl Belt {
  /// Create a new [`Belt`] from a single [`Bytes`] chunk.
  pub fn from_bytes(data: Bytes, max_chunk_size: Option<NonZeroUsize>) -> Self {
    Self::from_async_buf_read(Cursor::new(data), max_chunk_size)
  }

  /// Create a new [`Belt`] from an existing [`mpsc::Receiver`]`<Bytes>`.
  pub fn from_channel(
    receiver: mpsc::Receiver<Result<Bytes>>,
    max_chunk_size: Option<NonZeroUsize>,
  ) -> Self {
    Self {
      inner:         match max_chunk_size {
        Some(max_chunk_size) => MaybeBottleneckSource::Bottlenecked(
          Bottleneck::new(max_chunk_size, BytesSource::Channel(receiver)),
        ),
        None => {
          MaybeBottleneckSource::Unmodified(BytesSource::Channel(receiver))
        }
      },
      count:         Arc::new(AtomicU64::new(0)),
      declared_comp: None,
    }
  }

  /// Create a new [`Belt`] from an existing `impl `[`Stream`]`<Item = Bytes>`.
  pub fn from_stream(
    stream: impl Stream<Item = Result<Bytes>> + Send + Unpin + 'static,
    max_chunk_size: Option<NonZeroUsize>,
  ) -> Self {
    Self {
      inner:         match max_chunk_size {
        Some(max_chunk_size) => {
          MaybeBottleneckSource::Bottlenecked(Bottleneck::new(
            max_chunk_size,
            BytesSource::Erased(Box::new(stream)),
          ))
        }
        None => MaybeBottleneckSource::Unmodified(BytesSource::Erased(
          Box::new(stream),
        )),
      },
      count:         Arc::new(AtomicU64::new(0)),
      declared_comp: None,
    }
  }

  /// Create a new [`Belt`] from an existing `impl `[`AsyncBufRead`].
  pub fn from_async_buf_read(
    reader: impl AsyncBufRead + Send + Unpin + 'static,
    max_chunk_size: Option<NonZeroUsize>,
  ) -> Self {
    Self {
      inner:         match max_chunk_size {
        Some(max_chunk_size) => {
          MaybeBottleneckSource::Bottlenecked(Bottleneck::new(
            max_chunk_size,
            BytesSource::AsyncBufRead(tokio_util::io::ReaderStream::new(
              Box::new(reader),
            )),
          ))
        }
        None => MaybeBottleneckSource::Unmodified(BytesSource::AsyncBufRead(
          tokio_util::io::ReaderStream::new(Box::new(reader)),
        )),
      },
      count:         Arc::new(AtomicU64::new(0)),
      declared_comp: None,
    }
  }

  /// Create a new [`Belt`] from an existing `impl `[`tokio::io::AsyncRead`].
  pub fn from_async_read(
    reader: impl tokio::io::AsyncRead + Send + Unpin + 'static,
    max_chunk_size: Option<NonZeroUsize>,
  ) -> Self {
    Self::from_async_buf_read(tokio::io::BufReader::new(reader), max_chunk_size)
  }

  /// Create a new [`Belt`] from a new channel pair with a default buffer size.
  pub fn new_channel(
    buffer_size: usize,
    max_chunk_size: Option<NonZeroUsize>,
  ) -> (mpsc::Sender<Result<Bytes>>, Self) {
    let (tx, rx) = mpsc::channel(buffer_size);
    (tx, Self::from_channel(rx, max_chunk_size))
  }

  /// Adapt this [`Belt`] to compress with the given algorithm.
  pub fn adapt_to_comp(self, algo: CompressionAlgorithm) -> Self {
    Self {
      inner:         MaybeBottleneckSource::Unmodified(
        BytesSource::CompressionAdapter(Box::new(ReaderStream::new(
          match algo {
            CompressionAlgorithm::Zstd => {
              comp::CompressionAdapter::to_zstd(self)
            }
          },
        ))),
      ),
      count:         Arc::new(AtomicU64::new(0)),
      declared_comp: Some(algo),
    }
  }

  /// Adapt this [`Belt`] to decompress with the given algorithm.
  pub fn adapt_from_comp(self, algo: CompressionAlgorithm) -> Self {
    Self {
      inner:         MaybeBottleneckSource::Unmodified(
        BytesSource::CompressionAdapter(Box::new(ReaderStream::new(
          match algo {
            CompressionAlgorithm::Zstd => {
              comp::CompressionAdapter::from_zstd(self)
            }
          },
        ))),
      ),
      count:         Arc::new(AtomicU64::new(0)),
      declared_comp: None,
    }
  }

  /// Adapt this [`Belt`] output uncompressed data.
  pub fn adapt_to_no_comp(self) -> Self {
    match self.declared_comp {
      Some(algo) => self.adapt_from_comp(algo),
      None => self,
    }
  }

  /// Set the declared compression algorithm for this [`Belt`].
  pub fn set_declared_comp(self, algo: Option<CompressionAlgorithm>) -> Self {
    Self {
      declared_comp: algo,
      ..self
    }
  }

  /// Get the current compression algorithm of the streamed data.
  pub fn comp(&self) -> Option<CompressionAlgorithm> { self.declared_comp }

  /// Get a tracking counter for the total number of bytes read from this
  /// [`Belt`].
  pub fn counter(&self) -> Counter { Counter::new(self.count.clone()) }

  /// Convert this Belt into an [`AsyncBufRead`] implementer.
  pub fn to_async_buf_read(self) -> tokio_util::io::StreamReader<Self, Bytes> {
    tokio_util::io::StreamReader::new(self)
  }

  /// Consume and collect into a [`Bytes`]
  pub async fn collect(self) -> Result<Bytes> {
    use bytes::BytesMut;
    use futures::TryStreamExt;

    // Collect all bytes from the stream
    let mut buffer = BytesMut::new();
    let mut stream = self;

    while let Some(chunk) = stream.try_next().await? {
      buffer.extend_from_slice(&chunk);
    }

    Ok(buffer.freeze())
  }
}

impl Stream for Belt {
  type Item = Result<Bytes>;

  fn poll_next(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<Option<Self::Item>> {
    let poll_result = self.inner.poll_next_unpin(cx);

    if let Poll::Ready(Some(Ok(bytes))) = &poll_result {
      self.count.fetch_add(bytes.len() as u64, Ordering::Release);
    }

    poll_result
  }
}

#[cfg(test)]
mod tests {
  use futures::StreamExt;
  use tokio::io::AsyncReadExt;

  use super::*;

  #[tokio::test]
  async fn test_belt_from_bytes() {
    let bytes = Bytes::from("hello");
    let mut belt = Belt::from_bytes(bytes, Some(3.try_into().unwrap()));

    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("hel"))
    );
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("lo"))
    );
  }

  #[tokio::test]
  async fn test_belt_from_channel() {
    let (tx, mut belt) = Belt::new_channel(10, None);
    let counter = belt.counter();

    tx.send(Ok(Bytes::from("hello"))).await.unwrap();
    tx.send(Ok(Bytes::from(" world"))).await.unwrap();

    drop(tx); // Close the channel

    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("hello"))
    );
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from(" world"))
    );
    assert_eq!(counter.current(), 11);
  }

  #[tokio::test]
  async fn test_belt_from_stream() {
    let stream = futures::stream::iter(vec![
      Ok(Bytes::from("hello")),
      Ok(Bytes::from(" world")),
    ]);
    let belt = Belt::from_stream(stream, None);
    let counter = belt.counter();

    let mut belt = belt;
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("hello"))
    );
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from(" world"))
    );
    assert_eq!(counter.current(), 11);
  }

  #[tokio::test]
  async fn test_belt_from_async_read() {
    let reader = std::io::Cursor::new(b"hello world");
    let mut belt =
      Belt::from_async_buf_read(reader, Some(5.try_into().unwrap()));
    let counter = belt.counter();

    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("hello"))
    );
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from(" worl"))
    );
    assert_eq!(
      belt.next().await.transpose().unwrap(),
      Some(Bytes::from("d"))
    );
    assert_eq!(counter.current(), 11);
  }

  #[tokio::test]
  async fn test_belt_to_async_read() {
    let stream = futures::stream::iter(vec![
      Ok(Bytes::from("hello")),
      Ok(Bytes::from(" world")),
    ]);
    let belt = Belt::from_stream(stream, None);
    let counter = belt.counter();

    let mut reader = belt.to_async_buf_read();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();

    assert_eq!(buf, b"hello world");
    assert_eq!(counter.current(), 11);
  }

  #[tokio::test]
  async fn test_belt_to_async_read_error() {
    let (tx, belt) = Belt::new_channel(10, None);
    let counter = belt.counter();

    tx.send(Ok(Bytes::from("hello"))).await.unwrap();
    tx.send(Err(std::io::Error::other("oh no"))).await.unwrap();

    drop(tx); // Close the channel

    let mut reader = belt.to_async_buf_read();
    let mut buf = Vec::new();
    let err = reader.read_to_end(&mut buf).await.unwrap_err();

    assert_eq!(buf, b"hello");
    assert_eq!(err.to_string(), "oh no");
    assert_eq!(counter.current(), 5);
  }

  #[tokio::test]
  async fn test_belt_to_async_read_channel_partial() {
    let (tx, belt) = Belt::new_channel(10, None);
    let counter = belt.counter();

    tx.send(Ok(Bytes::from("hello world"))).await.unwrap();

    drop(tx); // Close the channel

    let mut reader = belt.to_async_buf_read();
    let mut buf = [0; 5];
    reader.read_exact(&mut buf).await.unwrap();

    assert_eq!(&buf, b"hello");
    // the whole bytes object was consumed
    assert_eq!(counter.current(), 11);

    let (mut belt, buf) = reader.into_inner_with_chunk();
    // there are no chunks left
    assert_eq!(belt.next().await.transpose().unwrap(), None);
    // the bytes that weren't read from the StreamReader
    assert_eq!(buf, Some(Bytes::from_static(b" world")));
  }
}
