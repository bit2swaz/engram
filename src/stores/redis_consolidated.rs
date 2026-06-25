use std::error::Error as StdError;

use async_trait::async_trait;
use futures::StreamExt;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

use crate::consolidation::store::{ConsolidatedMemoryStore, Summary};
use crate::core::MemoryError;

#[derive(Debug, Clone)]
pub struct RedisConsolidatedStore {
    connection: MultiplexedConnection,
}

impl RedisConsolidatedStore {
    pub fn new(connection: MultiplexedConnection) -> Self {
        Self { connection }
    }

    pub async fn connect(redis_url: &str) -> Result<Self, MemoryError> {
        let client = Client::open(redis_url).map_err(memory_error)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(memory_error)?;
        Ok(Self::new(connection))
    }

    fn session_key(session_id: &str) -> String {
        format!("consolidated:{session_id}")
    }
}

#[async_trait]
impl ConsolidatedMemoryStore for RedisConsolidatedStore {
    async fn add_summary(&self, session_id: &str, summary: Summary) -> Result<(), MemoryError> {
        let key = Self::session_key(session_id);
        let mut conn = self.connection.clone();

        // Idempotent: skip if this summary id is already in the list.
        let existing: Vec<String> = conn.lrange(&key, 0, -1).await.map_err(memory_error)?;
        for raw in &existing {
            let s: Summary = serde_json::from_str(raw).map_err(memory_error)?;
            if s.id == summary.id {
                return Ok(());
            }
        }

        let payload = serde_json::to_string(&summary).map_err(memory_error)?;
        let _: usize = conn.rpush(&key, payload).await.map_err(memory_error)?;
        Ok(())
    }

    async fn get_summaries(&self, session_id: &str) -> Result<Vec<Summary>, MemoryError> {
        let mut conn = self.connection.clone();
        let raw: Vec<String> = conn
            .lrange(Self::session_key(session_id), 0, -1)
            .await
            .map_err(memory_error)?;
        raw.into_iter()
            .map(|r| serde_json::from_str(&r).map_err(memory_error))
            .collect()
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut conn = self.connection.clone();
        let _: usize = conn
            .del(Self::session_key(session_id))
            .await
            .map_err(memory_error)?;
        Ok(())
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<Summary>)>, MemoryError> {
        let mut conn = self.connection.clone();
        let keys: Vec<String> = {
            let mut iter = conn
                .scan_match::<_, String>("consolidated:*")
                .await
                .map_err(memory_error)?;
            let mut collected = Vec::new();
            while let Some(key) = iter.next().await {
                collected.push(key);
            }
            collected
        };
        let mut out = Vec::new();
        for key in keys {
            let session_id = session_id_from_key(&key);
            let summaries = self.get_summaries(&session_id).await?;
            out.push((session_id, summaries));
        }
        Ok(out)
    }

    async fn restore_all(&self, sessions: Vec<(String, Vec<Summary>)>) -> Result<(), MemoryError> {
        let mut conn = self.connection.clone();
        // Wipe existing data before restoring snapshot.
        let existing: Vec<String> = {
            let mut iter = conn
                .scan_match::<_, String>("consolidated:*")
                .await
                .map_err(memory_error)?;
            let mut collected = Vec::new();
            while let Some(key) = iter.next().await {
                collected.push(key);
            }
            collected
        };
        for key in existing {
            let _: usize = conn.del(key.as_str()).await.map_err(memory_error)?;
        }
        for (session_id, summaries) in sessions {
            for summary in summaries {
                self.add_summary(&session_id, summary).await?;
            }
        }
        Ok(())
    }
}

fn session_id_from_key(key: &str) -> String {
    key.strip_prefix("consolidated:")
        .unwrap_or(key)
        .to_string()
}

fn memory_error(error: impl StdError + Send + Sync + 'static) -> MemoryError {
    MemoryError::Other(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consolidation::store::{ConsolidatedMemoryStore, Summary};
    use testcontainers::{
        GenericImage,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };

    const REDIS_PORT: u16 = 6379;

    async fn test_store() -> (RedisConsolidatedStore, testcontainers::ContainerAsync<GenericImage>) {
        let node = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(REDIS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .unwrap();
        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(REDIS_PORT.tcp()).await.unwrap();
        let url = format!("redis://{host}:{port}/");
        let store = RedisConsolidatedStore::connect(&url).await.unwrap();
        (store, node)
    }

    fn summary(id: &str) -> Summary {
        Summary {
            id: id.into(),
            text: "t".into(),
            created_at_index: 1,
            consumed_message_ids: vec!["m1".into()],
            consumed_count: 1,
            model: "mock".into(),
            prompt_version: "summarize_v1".into(),
        }
    }

    #[tokio::test]
    async fn redis_consolidated_round_trip_and_idempotent() {
        let (store, _c) = test_store().await;
        store.add_summary("s1", summary("a")).await.unwrap();
        store.add_summary("s1", summary("a")).await.unwrap(); // idempotent
        store.add_summary("s1", summary("b")).await.unwrap();
        assert_eq!(store.get_summaries("s1").await.unwrap().len(), 2);

        let dump = store.dump_all().await.unwrap();
        let (fresh, _c2) = test_store().await;
        fresh.restore_all(dump).await.unwrap();
        assert_eq!(fresh.get_summaries("s1").await.unwrap().len(), 2);
    }
}
