//! TiKV key-value store implementation of [`KvTransactional`].

use std::ops::Bound;

use kv::{
  DynTransaction, Key, KvError, KvPrimitive, KvResult, KvTransaction,
  KvTransactional, Value,
};
use miette::{Context, IntoDiagnostic};

fn tikv_key_from_key(key: Key) -> tikv_client::Key { key.to_string().into() }

trait IntoKvError<T> {
  fn to_kv_err(self) -> KvResult<T>;
}

impl<T: Sized> IntoKvError<T> for Result<T, tikv_client::Error> {
  fn to_kv_err(self) -> KvResult<T> {
    self.into_diagnostic().map_err(KvError::PlatformError)
  }
}

/// TiKV client.
pub struct TikvClient(tikv_client::TransactionClient);

impl TikvClient {
  /// Create a new TiKV client.
  pub async fn new(endpoints: Vec<&str>) -> KvResult<Self> {
    Ok(TikvClient(
      tikv_client::TransactionClient::new(endpoints)
        .await
        .to_kv_err()?,
    ))
  }

  /// Create a new TiKV client from environment variables.
  pub async fn new_from_env() -> miette::Result<Self> {
    let urls = std::env::var("TIKV_URLS")
      .into_diagnostic()
      .wrap_err("missing TIKV_URLS")?;
    let urls = urls.split(',').collect();
    let client = TikvClient::new(urls)
      .await
      .into_diagnostic()
      .context("failed to create tikv client")?;
    Ok(client)
  }
}

#[async_trait::async_trait]
impl KvTransactional for TikvClient {
  async fn begin_optimistic_transaction(&self) -> KvResult<DynTransaction> {
    Ok(DynTransaction::new(TikvTransaction(
      self.0.begin_optimistic().await.to_kv_err()?,
    )))
  }
  async fn begin_pessimistic_transaction(&self) -> KvResult<DynTransaction> {
    Ok(DynTransaction::new(TikvTransaction(
      self.0.begin_pessimistic().await.to_kv_err()?,
    )))
  }
}

/// TiKV transaction.
#[must_use]
pub struct TikvTransaction(tikv_client::Transaction);

#[async_trait::async_trait]
impl KvPrimitive for TikvTransaction {
  async fn get(&mut self, key: &Key) -> KvResult<Option<Value>> {
    Ok(
      self
        .0
        .get(tikv_key_from_key(key.clone()))
        .await
        .to_kv_err()?
        .map(Value::from),
    )
  }

  async fn put(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self
      .0
      .put(tikv_key_from_key(key.clone()), value)
      .await
      .to_kv_err()?;
    Ok(())
  }

  async fn insert(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self
      .0
      .insert(tikv_key_from_key(key.clone()), value)
      .await
      .to_kv_err()?;
    Ok(())
  }

  async fn scan(
    &mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> KvResult<Vec<(Key, Value)>> {
    match limit {
      Some(limit) => scan(self, start, end, limit).await,
      None => scan_unlimited(self, start, end).await,
    }
  }

  async fn delete(&mut self, key: &Key) -> KvResult<bool> {
    let key = tikv_key_from_key(key.clone());
    let exists = self.0.key_exists(key.clone()).await.to_kv_err()?;
    if exists {
      self.0.delete(key).await.to_kv_err()?;
      return Ok(true);
    }
    Ok(false)
  }
}

#[async_trait::async_trait]
impl KvTransaction for TikvTransaction {
  async fn commit(&mut self) -> KvResult<()> {
    self.0.commit().await.to_kv_err()?;
    Ok(())
  }

  async fn rollback(&mut self) -> KvResult<()> {
    self.0.rollback().await.to_kv_err()?;
    Ok(())
  }
}

async fn scan(
  txn: &mut TikvTransaction,
  start: Bound<Key>,
  end: Bound<Key>,
  limit: u32,
) -> KvResult<Vec<(Key, Value)>> {
  let start_bound: Bound<tikv_client::Key> = match start {
    Bound::Included(k) => Bound::Included(tikv_key_from_key(k)),
    Bound::Excluded(k) => Bound::Excluded(tikv_key_from_key(k)),
    Bound::Unbounded => Bound::Unbounded,
  };
  let end_bound: Bound<tikv_client::Key> = match end {
    Bound::Included(k) => Bound::Included(tikv_key_from_key(k)),
    Bound::Excluded(k) => Bound::Excluded(tikv_key_from_key(k)),
    Bound::Unbounded => Bound::Unbounded,
  };
  let range = tikv_client::BoundRange {
    from: start_bound,
    to:   end_bound,
  };

  Ok(
    txn
      .0
      .scan(range, limit)
      .await
      .to_kv_err()?
      .filter_map(|kp| match Key::try_from(Vec::<u8>::from(kp.0)) {
        Ok(key) => Some((key, Value::from(kp.1))),
        Err(e) => {
          tracing::error!("failed to parse key from kv store: {}", e);
          None
        }
      })
      .collect(),
  )
}

async fn scan_unlimited(
  txn: &mut TikvTransaction,
  start: Bound<Key>,
  end: Bound<Key>,
) -> KvResult<Vec<(Key, Value)>> {
  let mut start_bound: Bound<tikv_client::Key> = match start {
    Bound::Included(k) => Bound::Included(tikv_key_from_key(k)),
    Bound::Excluded(k) => Bound::Excluded(tikv_key_from_key(k)),
    Bound::Unbounded => Bound::Unbounded,
  };
  let end_bound: Bound<tikv_client::Key> = match end {
    Bound::Included(k) => Bound::Included(tikv_key_from_key(k)),
    Bound::Excluded(k) => Bound::Excluded(tikv_key_from_key(k)),
    Bound::Unbounded => Bound::Unbounded,
  };

  let mut results = Vec::new();
  loop {
    let range = tikv_client::BoundRange {
      from: start_bound,
      to:   end_bound.clone(),
    };
    let scan_result = txn.0.scan(range.clone(), 1000).await.to_kv_err()?;
    let scan = scan_result
      .filter_map(|kp| match Key::try_from(Vec::<u8>::from(kp.0)) {
        Ok(key) => Some((key, Value::from(kp.1))),
        Err(e) => {
          tracing::error!("failed to parse key from kv store: {}", e);
          None
        }
      })
      .collect::<Vec<_>>();

    if scan.is_empty() {
      break;
    }

    start_bound =
      Bound::Excluded(tikv_key_from_key(scan.last().unwrap().0.clone()));
    results.extend(scan);
  }

  Ok(results)
}
