use std::{
  collections::HashMap,
  path::{Path, PathBuf},
  sync::{Arc, LazyLock},
};

use belt::Belt;
use bytes::Bytes;
use hex::health;
use tokio::sync::RwLock;

use super::{ReadError, StorageClientLike};
use crate::WriteError;

pub struct StaticMemoryStorageClient(
  pub(crate) &'static LazyLock<MemoryStorageClient>,
);

#[async_trait::async_trait]
impl StorageClientLike for StaticMemoryStorageClient {
  async fn read(&self, input_path: &Path) -> Result<Belt, ReadError> {
    self.0.read(input_path).await
  }
  async fn write(
    &self,
    path: &Path,
    data: Belt,
  ) -> Result<dvf::FileSize, WriteError> {
    self.0.write(path, data).await
  }
}
#[async_trait::async_trait]
impl health::HealthReporter for StaticMemoryStorageClient {
  fn name(&self) -> &'static str { self.0.name() }
  async fn health_check(&self) -> health::ComponentHealth {
    self.0.health_check().await
  }
}

/// In-memory storage client using a hashmap with `Bytes` for efficient storage
pub struct MemoryStorageClient {
  storage: Arc<RwLock<HashMap<PathBuf, Bytes>>>,
}

#[allow(dead_code)]
impl MemoryStorageClient {
  /// Create a new empty memory storage client
  pub fn new() -> Self {
    Self {
      storage: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  /// Create a new memory storage client with pre-populated data
  pub fn with_data(data: HashMap<PathBuf, Bytes>) -> Self {
    Self {
      storage: Arc::new(RwLock::new(data)),
    }
  }

  /// Get the number of stored items
  pub async fn len(&self) -> usize { self.storage.read().await.len() }

  /// Check if the storage is empty
  pub async fn is_empty(&self) -> bool { self.storage.read().await.is_empty() }

  /// List all stored paths
  pub async fn list_paths(&self) -> Vec<PathBuf> {
    self.storage.read().await.keys().cloned().collect()
  }

  /// Clear all stored data
  pub async fn clear(&self) { self.storage.write().await.clear(); }

  /// Remove a specific path from storage
  pub async fn remove(&self, path: &Path) -> Option<Bytes> {
    self.storage.write().await.remove(path)
  }
}

impl Default for MemoryStorageClient {
  fn default() -> Self { Self::new() }
}

#[async_trait::async_trait]
impl health::HealthReporter for MemoryStorageClient {
  fn name(&self) -> &'static str { stringify!(MemoryStorageClient) }

  async fn health_check(&self) -> health::ComponentHealth {
    health::IntrensicallyUp.into()
  }
}

#[async_trait::async_trait]
impl StorageClientLike for MemoryStorageClient {
  #[tracing::instrument(skip(self))]
  async fn read(&self, input_path: &Path) -> Result<Belt, ReadError> {
    let storage = self.storage.read().await;

    match storage.get(input_path) {
      Some(bytes) => {
        // Convert Bytes to Belt by creating a cursor from the bytes
        Ok(Belt::from_bytes(
          bytes.clone(),
          Some(belt::DEFAULT_CHUNK_SIZE),
        ))
      }
      None => Err(ReadError::NotFound(input_path.to_path_buf())),
    }
  }

  #[tracing::instrument(skip(self, data))]
  async fn write(
    &self,
    path: &Path,
    data: Belt,
  ) -> Result<dvf::FileSize, WriteError> {
    // Convert Belt to bytes by reading all data
    let mut buf = Vec::new();
    let counter = data.counter();
    let mut reader = data.to_async_buf_read();

    tokio::io::copy(&mut reader, &mut buf).await?;

    let bytes = Bytes::from(buf);
    let file_size = dvf::FileSize::new(counter.current());

    // Store in hashmap
    let mut storage = self.storage.write().await;
    storage.insert(path.to_path_buf(), bytes);

    Ok(file_size)
  }
}

#[cfg(test)]
mod tests {
  use std::str::FromStr;

  use tokio::io::AsyncReadExt;

  use super::*;

  #[tokio::test]
  async fn read_works() {
    let client = MemoryStorageClient::new();

    // Pre-populate with test data
    {
      let mut storage = client.storage.write().await;
      storage.insert(PathBuf::from_str("file1").unwrap(), Bytes::from("abc"));
    }

    let data = client
      .read(&PathBuf::from_str("file1").unwrap())
      .await
      .unwrap();

    let mut result = String::new();
    data
      .to_async_buf_read()
      .read_to_string(&mut result)
      .await
      .unwrap();

    assert_eq!(&result, "abc");
  }

  #[tokio::test]
  async fn write_and_read_works() {
    let client = MemoryStorageClient::new();
    let test_data = "hello world";
    let path = PathBuf::from_str("test_file").unwrap();

    // Write data
    let belt =
      Belt::from_bytes(test_data.into(), Some(belt::DEFAULT_CHUNK_SIZE));

    let file_size = client.write(&path, belt).await.unwrap();
    assert_eq!(file_size.into_inner(), test_data.len() as u64);

    // Read it back
    let data = client.read(&path).await.unwrap();
    let mut result = String::new();
    data
      .to_async_buf_read()
      .read_to_string(&mut result)
      .await
      .unwrap();

    assert_eq!(&result, test_data);
  }

  #[tokio::test]
  async fn read_nonexistent_returns_not_found() {
    let client = MemoryStorageClient::new();

    let result = client
      .read(&PathBuf::from_str("nonexistent").unwrap())
      .await;

    assert!(matches!(result, Err(ReadError::NotFound(_))));
  }

  #[tokio::test]
  async fn utility_methods_work() {
    let client = MemoryStorageClient::new();

    // Initially empty
    assert!(client.is_empty().await);
    assert_eq!(client.len().await, 0);

    // Add some data
    {
      let mut storage = client.storage.write().await;
      storage.insert(PathBuf::from("file1"), Bytes::from("data1"));
      storage.insert(PathBuf::from("file2"), Bytes::from("data2"));
    }

    // Check size and contents
    assert!(!client.is_empty().await);
    assert_eq!(client.len().await, 2);

    let paths = client.list_paths().await;
    assert_eq!(paths.len(), 2);
    assert!(paths.contains(&PathBuf::from("file1")));
    assert!(paths.contains(&PathBuf::from("file2")));

    // Remove one item
    let removed = client.remove(&PathBuf::from("file1")).await;
    assert!(removed.is_some());
    assert_eq!(client.len().await, 1);

    // Clear all
    client.clear().await;
    assert!(client.is_empty().await);
  }

  #[tokio::test]
  async fn with_data_constructor_works() {
    let mut initial_data = HashMap::new();
    initial_data.insert(PathBuf::from("preset"), Bytes::from("preset_data"));

    let client = MemoryStorageClient::with_data(initial_data);

    assert_eq!(client.len().await, 1);

    let data = client.read(&PathBuf::from("preset")).await.unwrap();
    let mut result = String::new();
    data
      .to_async_buf_read()
      .read_to_string(&mut result)
      .await
      .unwrap();

    assert_eq!(&result, "preset_data");
  }
}
