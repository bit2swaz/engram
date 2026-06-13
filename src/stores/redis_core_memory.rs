use std::error::Error as StdError;

use async_trait::async_trait;
use futures::StreamExt;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

use crate::core::{CoreMemoryStore, MemoryError};

#[derive(Debug, Clone)]
pub struct RedisCoreMemoryStore {
    connection: MultiplexedConnection,
}

impl RedisCoreMemoryStore {
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
        format!("session:{session_id}:core_memory")
    }
}

#[async_trait]
impl CoreMemoryStore for RedisCoreMemoryStore {
    async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError> {
        let mut connection = self.connection.clone();
        let _: usize = connection
            .sadd(Self::session_key(session_id), fact)
            .await
            .map_err(memory_error)?;
        Ok(())
    }

    async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError> {
        let mut connection = self.connection.clone();
        connection
            .smembers(Self::session_key(session_id))
            .await
            .map_err(memory_error)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut connection = self.connection.clone();
        let _: usize = connection
            .del(Self::session_key(session_id))
            .await
            .map_err(memory_error)?;
        Ok(())
    }

    async fn dump_all(&self) -> Result<Vec<(String, Vec<String>)>, MemoryError> {
        let mut connection = self.connection.clone();
        let keys: Vec<String> = {
            let mut iter = connection
                .scan_match::<_, String>("session:*:core_memory")
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
            let session_id = core_session_id_from_key(&key);
            let facts: Vec<String> = connection.smembers(key.as_str()).await.map_err(memory_error)?;
            out.push((session_id, facts));
        }
        Ok(out)
    }

    async fn restore_all(&self, sessions: Vec<(String, Vec<String>)>) -> Result<(), MemoryError> {
        let mut connection = self.connection.clone();
        let existing: Vec<String> = {
            let mut iter = connection
                .scan_match::<_, String>("session:*:core_memory")
                .await
                .map_err(memory_error)?;
            let mut collected = Vec::new();
            while let Some(key) = iter.next().await {
                collected.push(key);
            }
            collected
        };
        for key in existing {
            let _: usize = connection.del(key.as_str()).await.map_err(memory_error)?;
        }
        for (session_id, facts) in sessions {
            for fact in facts {
                self.add_fact(&session_id, &fact).await?;
            }
        }
        Ok(())
    }
}

fn core_session_id_from_key(key: &str) -> String {
    key.strip_prefix("session:")
        .and_then(|s| s.strip_suffix(":core_memory"))
        .unwrap_or(key)
        .to_string()
}

fn memory_error(error: impl StdError + Send + Sync + 'static) -> MemoryError {
    MemoryError::Other(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::CoreMemoryStore;
    use testcontainers::{
        GenericImage,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };

    const REDIS_PORT: u16 = 6379;

    async fn test_store() -> (RedisCoreMemoryStore, testcontainers::ContainerAsync<GenericImage>) {
        let node = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(REDIS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .unwrap();
        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(REDIS_PORT.tcp()).await.unwrap();
        let url = format!("redis://{host}:{port}/");
        let store = RedisCoreMemoryStore::connect(&url).await.unwrap();
        (store, node)
    }

    #[tokio::test]
    async fn dump_all_and_restore_all_round_trip() {
        let (store, _node) = test_store().await;
        store.add_fact("s1", "fact-a").await.unwrap();
        store.add_fact("s1", "fact-b").await.unwrap();
        store.add_fact("s2", "fact-c").await.unwrap();

        let dump = store.dump_all().await.unwrap();
        assert_eq!(dump.len(), 2);

        store.add_fact("stale", "old").await.unwrap();
        store.restore_all(dump).await.unwrap();

        assert!(store.get_facts("stale").await.unwrap().is_empty());
        let mut s1_facts = store.get_facts("s1").await.unwrap();
        s1_facts.sort();
        assert_eq!(s1_facts, vec!["fact-a".to_string(), "fact-b".to_string()]);
        assert_eq!(store.get_facts("s2").await.unwrap(), vec!["fact-c".to_string()]);
    }
}
