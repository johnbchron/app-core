use std::ops::Bound;

use kv::*;
use miette::Result;

pub trait ConsumptiveTransaction:
  KvPrimitive + KvTransaction + Send + Sync + 'static + Sized
{
  async fn csm_exists(self, key: &Key) -> Result<(Self, bool)> {
    let (txn, value) = self.csm_get(key).await?;
    Ok((txn, value.is_some()))
  }

  async fn csm_insert(mut self, key: &Key, value: Value) -> Result<Self> {
    if let Err(e) = self.insert(key, value).await {
      return Err(
        self
          .to_rollback_with_error(e.into(), "failed to insert value")
          .await,
      );
    }

    Ok(self)
  }

  async fn csm_get(mut self, key: &Key) -> Result<(Self, Option<Value>)> {
    let value = match self.get(key).await {
      Ok(v) => v,
      Err(e) => {
        return Err(
          self
            .to_rollback_with_error(e.into(), "failed to get value")
            .await,
        );
      }
    };

    Ok((self, value))
  }

  async fn csm_scan(
    mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> Result<(Self, Vec<(Key, Value)>)> {
    let scan = match self.scan(start, end, limit).await {
      Ok(s) => s,
      Err(e) => {
        return Err(
          self
            .to_rollback_with_error(e.into(), "failed to scan values")
            .await,
        );
      }
    };

    Ok((self, scan))
  }

  async fn csm_put(mut self, key: &Key, value: Value) -> Result<Self> {
    if let Err(e) = self.put(key, value).await {
      return Err(
        self
          .to_rollback_with_error(e.into(), "failed to put value")
          .await,
      );
    }

    Ok(self)
  }

  async fn csm_delete(mut self, key: &Key) -> Result<(Self, bool)> {
    let existed = match self.delete(key).await {
      Ok(v) => v,
      Err(e) => {
        return Err(
          self
            .to_rollback_with_error(e.into(), "failed to get value")
            .await,
        );
      }
    };

    Ok((self, existed))
  }
}

impl<T> ConsumptiveTransaction for T where
  T: KvPrimitive + KvTransaction + Send + Sync + 'static
{
}
