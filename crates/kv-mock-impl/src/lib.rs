//! A mock implementation of [`KvTransactional`]. Follows TiKV semantics around
//! transactions.

#[cfg(test)]
mod tests;

use std::{
  collections::{HashMap, HashSet},
  ops::Bound,
  sync::Arc,
};

use kv::{
  DynTransaction, Key, KvError, KvPrimitive, KvResult, KvTransaction,
  KvTransactional, Value,
};
use tokio::sync::{Mutex, RwLock};

/// A mock key-value store.
#[derive(Clone)]
pub struct MockStore {
  data:  Arc<RwLock<HashMap<Key, Value>>>,
  locks: Arc<Mutex<HashSet<Key>>>, // Set of keys currently locked
}

impl MockStore {
  /// Create a new mock store.
  pub fn new() -> Arc<Self> {
    Arc::new(Self {
      data:  Arc::new(RwLock::new(HashMap::new())),
      locks: Arc::new(Mutex::new(HashSet::new())),
    })
  }

  /// Screw with the internal data of the store. This is useful for testing.
  #[allow(dead_code)]
  pub fn screw_with_internal_data(&self) -> &RwLock<HashMap<Key, Value>> {
    &self.data
  }
}

#[async_trait::async_trait]
impl KvTransactional for MockStore {
  async fn begin_optimistic_transaction(&self) -> KvResult<DynTransaction> {
    Ok(DynTransaction::new(OptimisticTransaction {
      store:     self.clone(),
      read_set:  HashMap::new(),
      write_set: HashMap::new(),
    }))
  }

  async fn begin_pessimistic_transaction(&self) -> KvResult<DynTransaction> {
    Ok(DynTransaction::new(PessimisticTransaction {
      store:       self.clone(),
      locked_keys: HashSet::new(),
      write_set:   HashMap::new(),
    }))
  }
}

/// A transaction error.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum TransactionError {
  /// The key was locked.
  #[error("key locked: `{0}`")]
  KeyLocked(Key),
  /// There was a key conflict.
  #[error("key conflict: `{0}`")]
  KeyConflict(Key),
  /// Some other error occurred.
  #[error("other: {0:?}")]
  Other(String),
}

impl From<TransactionError> for KvError {
  fn from(error: TransactionError) -> Self {
    KvError::PlatformError(error.into())
  }
}

/// An optimistic transaction.
pub struct OptimisticTransaction {
  store:     MockStore,
  read_set:  HashMap<Key, Option<Value>>,
  write_set: HashMap<Key, Option<Value>>, // None means delete
}

impl Drop for OptimisticTransaction {
  fn drop(&mut self) {
    if !self.read_set.is_empty() || !self.write_set.is_empty() {
      panic!(
        "WARNING: Optimistic transaction dropped without commit or rollback. \
         This may indicate a logic error. Read Set: {:?}, Write Set: {:?}",
        self.read_set.keys().collect::<Vec<_>>(),
        self.write_set.keys().collect::<Vec<_>>()
      );
    }
  }
}

impl OptimisticTransaction {
  async fn check_conflicts(&self) -> Result<(), TransactionError> {
    let data = self.store.data.read().await;
    for (key, expected_value) in &self.read_set {
      let current_value = data.get(key).cloned();
      if current_value != *expected_value {
        return Err(TransactionError::KeyConflict(key.clone()));
      }
    }
    Ok(())
  }
}

#[async_trait::async_trait]
impl KvPrimitive for OptimisticTransaction {
  async fn get(&mut self, key: &Key) -> KvResult<Option<Value>> {
    let data = self.store.data.read().await;
    let value = data.get(key).cloned();
    self.read_set.insert(key.clone(), value.clone());
    Ok(value)
  }

  async fn put(&mut self, key: &Key, value: Value) -> KvResult<()> {
    // Record the current value before modifying write_set
    if !self.read_set.contains_key(key) {
      let data = self.store.data.read().await;
      self.read_set.insert(key.clone(), data.get(key).cloned());
    }
    self.write_set.insert(key.clone(), Some(value));
    Ok(())
  }

  async fn insert(&mut self, key: &Key, value: Value) -> KvResult<()> {
    // For optimistic transactions, defer existence check to commit time
    if !self.read_set.contains_key(key) {
      let data = self.store.data.read().await;
      let existing_value = data.get(key).cloned();
      self.read_set.insert(key.clone(), existing_value.clone());

      // Check if key exists now for immediate feedback
      if existing_value.is_some() {
        return Err(KvError::PlatformError(miette::miette!(
          "Key already exists"
        )));
      }
    } else if let Some(existing_value) = self.read_set.get(key)
      && existing_value.is_some()
    {
      return Err(KvError::PlatformError(miette::miette!(
        "Key already exists"
      )));
    }

    self.write_set.insert(key.clone(), Some(value));
    Ok(())
  }

  async fn scan(
    &mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> KvResult<Vec<(Key, Value)>> {
    let data = self.store.data.read().await;
    let mut result = Vec::new();

    // Collect all matching keys first
    let mut matching_keys: Vec<_> = data.keys().cloned().collect();
    matching_keys.sort(); // Ensure consistent ordering

    for key in matching_keys {
      let in_range = match &start {
        Bound::Included(start) => key >= *start,
        Bound::Excluded(start) => key > *start,
        Bound::Unbounded => true,
      } && match &end {
        Bound::Included(end) => key <= *end,
        Bound::Excluded(end) => key < *end,
        Bound::Unbounded => true,
      };

      if in_range && let Some(value) = data.get(&key) {
        self.read_set.insert(key.clone(), Some(value.clone()));
        result.push((key.clone(), value.clone()));

        if let Some(limit) = limit
          && result.len() == limit as usize
        {
          break;
        }
      }
    }

    Ok(result)
  }

  async fn delete(&mut self, key: &Key) -> KvResult<bool> {
    // Record the current value before modifying
    if !self.read_set.contains_key(key) {
      let data = self.store.data.read().await;
      self.read_set.insert(key.clone(), data.get(key).cloned());
    }

    let existed = self.read_set.get(key).unwrap().is_some();
    self.write_set.insert(key.clone(), None); // None means delete
    Ok(existed)
  }
}

#[async_trait::async_trait]
impl KvTransaction for OptimisticTransaction {
  async fn commit(&mut self) -> KvResult<()> {
    self.check_conflicts().await?;
    let mut data = self.store.data.write().await;

    for (key, value_opt) in self.write_set.drain() {
      match value_opt {
        Some(value) => {
          data.insert(key, value);
        }
        None => {
          data.remove(&key); // Actually delete the key
        }
      }
    }

    self.read_set.clear();
    Ok(())
  }

  async fn rollback(&mut self) -> KvResult<()> {
    self.read_set.clear();
    self.write_set.clear();
    Ok(())
  }
}

/// A pessimistic transaction.
pub struct PessimisticTransaction {
  store:       MockStore,
  locked_keys: HashSet<Key>,
  write_set:   HashMap<Key, Option<Value>>, // None means delete
}

impl Drop for PessimisticTransaction {
  fn drop(&mut self) {
    if !self.locked_keys.is_empty() || !self.write_set.is_empty() {
      panic!(
        "WARNING: Pessimistic transaction dropped without commit or rollback. \
         Cleaning up {} locked keys. This may indicate a logic error.",
        self.locked_keys.len()
      );
    }
  }
}

impl PessimisticTransaction {
  async fn lock_key(&mut self, key: &Key) -> Result<(), TransactionError> {
    if self.locked_keys.contains(key) {
      return Ok(());
    }
    let mut locks = self.store.locks.lock().await;
    if locks.contains(key) {
      return Err(TransactionError::KeyLocked(key.clone()));
    }
    locks.insert(key.clone());
    self.locked_keys.insert(key.clone());
    Ok(())
  }

  async fn lock_keys(&mut self, keys: &[Key]) -> Result<(), TransactionError> {
    // Sort keys to prevent deadlocks when multiple transactions try to lock
    // overlapping sets
    let mut sorted_keys = keys.to_vec();
    sorted_keys.sort();
    sorted_keys.dedup();

    for key in sorted_keys {
      self.lock_key(&key).await?;
    }
    Ok(())
  }

  async fn unlock_keys(&self) {
    if !self.locked_keys.is_empty() {
      let mut locks = self.store.locks.lock().await;
      for key in &self.locked_keys {
        locks.remove(key);
      }
    }
  }
}

#[async_trait::async_trait]
impl KvPrimitive for PessimisticTransaction {
  async fn get(&mut self, key: &Key) -> KvResult<Option<Value>> {
    self.lock_key(key).await?;
    let data = self.store.data.read().await;
    Ok(data.get(key).cloned())
  }

  async fn put(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self.lock_key(key).await?;
    self.write_set.insert(key.clone(), Some(value));
    Ok(())
  }

  async fn insert(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self.lock_key(key).await?;
    if self.store.data.read().await.contains_key(key) {
      return Err(KvError::PlatformError(miette::miette!(
        "Key already exists"
      )));
    }
    self.write_set.insert(key.clone(), Some(value));
    Ok(())
  }

  async fn scan(
    &mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> KvResult<Vec<(Key, Value)>> {
    // First, identify all keys in the range and lock them
    let data = self.store.data.read().await;
    let mut keys_in_range = Vec::new();

    for key in data.keys() {
      let in_range = match &start {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
      } && match &end {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
      };

      if in_range {
        keys_in_range.push(key.clone());
      }
    }

    // Sort keys for consistent ordering and deadlock prevention
    keys_in_range.sort();

    // Apply limit to keys we need to lock
    if let Some(limit) = limit {
      keys_in_range.truncate(limit as usize);
    }

    // Lock all keys we're about to read
    drop(data); // Release read lock before acquiring individual key locks
    self.lock_keys(&keys_in_range).await?;

    // Now read the data
    let data = self.store.data.read().await;
    let mut result = Vec::new();

    for key in keys_in_range {
      if let Some(value) = data.get(&key) {
        result.push((key, value.clone()));
      }
    }

    Ok(result)
  }

  async fn delete(&mut self, key: &Key) -> KvResult<bool> {
    self.lock_key(key).await?;
    let existed = self.store.data.read().await.contains_key(key);
    self.write_set.insert(key.clone(), None); // None means delete
    Ok(existed)
  }
}

#[async_trait::async_trait]
impl KvTransaction for PessimisticTransaction {
  async fn commit(&mut self) -> KvResult<()> {
    let mut data = self.store.data.write().await;

    for (key, value_opt) in self.write_set.drain() {
      match value_opt {
        Some(value) => {
          data.insert(key, value);
        }
        None => {
          data.remove(&key); // Actually delete the key
        }
      }
    }

    drop(data); // Release write lock before unlocking individual keys
    self.unlock_keys().await;
    self.locked_keys.clear();
    Ok(())
  }

  async fn rollback(&mut self) -> KvResult<()> {
    self.unlock_keys().await;
    self.locked_keys.clear();
    self.write_set.clear();
    Ok(())
  }
}
