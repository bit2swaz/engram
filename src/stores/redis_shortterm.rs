use std::error::Error as StdError;

use async_trait::async_trait;
use redis::{AsyncCommands, Client, aio::MultiplexedConnection};

use crate::core::{MemoryError, ShortTermMemory, TokenCounter, trim_messages_to_token_budget};
use crate::models::{EmbeddingStatus, Message};

const SESSION_TTL_SECONDS: i64 = 7 * 24 * 60 * 60;

#[derive(Debug, Clone)]
pub struct RedisShortTermMemory {
    connection: MultiplexedConnection,
    ttl_seconds: i64,
}

impl RedisShortTermMemory {
    pub fn new(connection: MultiplexedConnection) -> Self {
        Self {
            connection,
            ttl_seconds: SESSION_TTL_SECONDS,
        }
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
        format!("session:{session_id}:messages")
    }

    async fn read_messages(&self, session_id: &str) -> Result<Vec<Message>, MemoryError> {
        let mut connection = self.connection.clone();
        let raw_messages: Vec<String> = connection
            .lrange(Self::session_key(session_id), 0, -1)
            .await
            .map_err(memory_error)?;

        raw_messages
            .into_iter()
            .map(|raw| serde_json::from_str(&raw).map_err(memory_error))
            .collect()
    }

    async fn write_messages(
        &self,
        session_id: &str,
        messages: &[Message],
    ) -> Result<(), MemoryError> {
        let key = Self::session_key(session_id);
        let mut connection = self.connection.clone();

        let _: usize = connection.del(&key).await.map_err(memory_error)?;

        if !messages.is_empty() {
            let payloads = messages
                .iter()
                .map(|message| serde_json::to_string(message).map_err(memory_error))
                .collect::<Result<Vec<_>, _>>()?;

            let _: usize = connection
                .rpush(&key, payloads)
                .await
                .map_err(memory_error)?;
            let _: bool = connection
                .expire(&key, self.ttl_seconds)
                .await
                .map_err(memory_error)?;
        }

        Ok(())
    }
}

#[async_trait]
impl ShortTermMemory for RedisShortTermMemory {
    async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError> {
        let key = Self::session_key(session_id);
        let mut connection = self.connection.clone();
        let payload = serde_json::to_string(&msg).map_err(memory_error)?;

        let _: usize = connection
            .rpush(&key, payload)
            .await
            .map_err(memory_error)?;
        let _: bool = connection
            .expire(&key, self.ttl_seconds)
            .await
            .map_err(memory_error)?;

        Ok(())
    }

    async fn get_recent(
        &self,
        session_id: &str,
        count: usize,
    ) -> Result<Vec<Message>, MemoryError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut connection = self.connection.clone();
        let start = -(count as isize);
        let raw_messages: Vec<String> = connection
            .lrange(Self::session_key(session_id), start, -1)
            .await
            .map_err(memory_error)?;

        raw_messages
            .into_iter()
            .map(|raw| serde_json::from_str(&raw).map_err(memory_error))
            .collect()
    }

    async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError> {
        let key = Self::session_key(session_id);
        let mut connection = self.connection.clone();

        if max_count == 0 {
            let _: usize = connection.del(&key).await.map_err(memory_error)?;
            return Ok(());
        }

        let len: usize = connection.llen(&key).await.map_err(memory_error)?;
        if len > max_count {
            let start = -(max_count as isize);
            let _: () = connection
                .ltrim(&key, start, -1)
                .await
                .map_err(memory_error)?;
            let _: bool = connection
                .expire(&key, self.ttl_seconds)
                .await
                .map_err(memory_error)?;
        }

        Ok(())
    }

    async fn trim_to_token_budget(
        &self,
        session_id: &str,
        max_tokens: usize,
        token_counter: &dyn TokenCounter,
    ) -> Result<Vec<Message>, MemoryError> {
        let messages = self.read_messages(session_id).await?;
        let trimmed = trim_messages_to_token_budget(messages, max_tokens, token_counter);
        self.write_messages(session_id, &trimmed).await?;

        Ok(trimmed)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut connection = self.connection.clone();
        let _: usize = connection
            .del(Self::session_key(session_id))
            .await
            .map_err(memory_error)?;
        Ok(())
    }

    async fn update_message_status(
        &self,
        session_id: &str,
        message_id: &str,
        status: EmbeddingStatus,
    ) -> Result<(), MemoryError> {
        let mut messages = self.read_messages(session_id).await?;

        if let Some(message) = messages
            .iter_mut()
            .find(|message| message.id.as_deref() == Some(message_id))
        {
            message.embedding_status = Some(status);
            self.write_messages(session_id, &messages).await?;
        }

        Ok(())
    }

    async fn get_message_by_id(
        &self,
        session_id: &str,
        message_id: &str,
    ) -> Result<Option<Message>, MemoryError> {
        let messages = self.read_messages(session_id).await?;

        Ok(messages
            .into_iter()
            .find(|message| message.id.as_deref() == Some(message_id)))
    }
}

fn memory_error(error: impl StdError + Send + Sync + 'static) -> MemoryError {
    MemoryError::Other(Box::new(error))
}
