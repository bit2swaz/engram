use std::error::Error as StdError;

use async_trait::async_trait;
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
}

fn memory_error(error: impl StdError + Send + Sync + 'static) -> MemoryError {
    MemoryError::Other(Box::new(error))
}
