use std::error::Error as StdError;

use async_trait::async_trait;
use futures::StreamExt;
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
        // LRANGE start = -(count): clamp to 0 (fetch all) if count overflows isize.
        let start: isize = if count > isize::MAX as usize { 0 } else { -(count as isize) };
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

    async fn dump_all(&self) -> Result<Vec<(String, Vec<Message>)>, MemoryError> {
        let mut connection = self.connection.clone();
        let keys: Vec<String> = {
            let mut iter = connection
                .scan_match::<_, String>("session:*:messages")
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
            let raw: Vec<String> = connection.lrange(key.as_str(), 0, -1).await.map_err(memory_error)?;
            let messages = raw
                .into_iter()
                .map(|r| serde_json::from_str(&r).map_err(memory_error))
                .collect::<Result<Vec<Message>, _>>()?;
            out.push((session_id, messages));
        }
        Ok(out)
    }

    async fn restore_all(&self, sessions: Vec<(String, Vec<Message>)>) -> Result<(), MemoryError> {
        let mut connection = self.connection.clone();
        let existing: Vec<String> = {
            let mut iter = connection
                .scan_match::<_, String>("session:*:messages")
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
        for (session_id, messages) in sessions {
            self.write_messages(&session_id, &messages).await?;
        }
        Ok(())
    }

    async fn remove_messages(&self, session_id: &str, ids: &[String]) -> Result<(), MemoryError> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut messages = self.read_messages(session_id).await?;
        messages.retain(|m| m.id.as_deref().map_or(true, |id| !ids.contains(&id.to_string())));
        self.write_messages(session_id, &messages).await
    }
}

fn session_id_from_key(key: &str) -> String {
    key.strip_prefix("session:")
        .and_then(|s| s.strip_suffix(":messages"))
        .unwrap_or(key)
        .to_string()
}

fn memory_error(error: impl StdError + Send + Sync + 'static) -> MemoryError {
    MemoryError::Other(Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ShortTermMemory;
    use crate::models::Message;
    use testcontainers::{
        GenericImage,
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
    };

    const REDIS_PORT: u16 = 6379;

    async fn test_store() -> (RedisShortTermMemory, testcontainers::ContainerAsync<GenericImage>) {
        let node = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(REDIS_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Ready to accept connections"))
            .start()
            .await
            .unwrap();
        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(REDIS_PORT.tcp()).await.unwrap();
        let url = format!("redis://{host}:{port}/");
        let store = RedisShortTermMemory::connect(&url).await.unwrap();
        (store, node)
    }

    fn sample_message(content: &str) -> Message {
        Message {
            id: None,
            role: "user".to_string(),
            content: content.to_string(),
            timestamp: None,
            embedding_status: None,
        }
    }

    fn message_with_id(id: &str, content: &str) -> Message {
        Message {
            id: Some(id.to_string()),
            role: "user".to_string(),
            content: content.to_string(),
            timestamp: None,
            embedding_status: None,
        }
    }

    #[tokio::test]
    async fn dump_all_and_restore_all_round_trip() {
        let (store, _node) = test_store().await;
        store.add_message("s1", sample_message("hi")).await.unwrap();
        store.add_message("s2", sample_message("yo")).await.unwrap();

        let dump = store.dump_all().await.unwrap();
        assert_eq!(dump.len(), 2);

        // Wipe and restore into the same Redis.
        store.add_message("stale", sample_message("old")).await.unwrap();
        store.restore_all(dump).await.unwrap();

        assert!(store.get_recent("stale", 10).await.unwrap().is_empty());
        assert_eq!(store.get_recent("s1", 10).await.unwrap().len(), 1);
        assert_eq!(store.get_recent("s2", 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn remove_messages_by_id() {
        let (store, _node) = test_store().await;
        store.add_message("s1", message_with_id("m1", "first")).await.unwrap();
        store.add_message("s1", message_with_id("m2", "second")).await.unwrap();
        store.add_message("s1", message_with_id("m3", "third")).await.unwrap();

        store.remove_messages("s1", &["m1".into(), "m3".into()]).await.unwrap();

        let remaining = store.get_recent("s1", 10).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].id.as_deref(), Some("m2"));
    }
}
