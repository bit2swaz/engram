use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Mutex;

use async_trait::async_trait;
use thiserror::Error;

type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub text: String,
    pub score: f32,
}

#[derive(Debug, Error)]
pub enum EmbedError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum MemoryError {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Other(#[from] BoxError),
}

#[derive(Debug, Error)]
pub enum MemoryServerError {
    #[error(transparent)]
    Embed(#[from] EmbedError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Memory(#[from] MemoryError),
}

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError>;
}

#[async_trait]
pub trait VectorStore: Send + Sync {
    async fn insert(
        &self,
        session_id: &str,
        text: &str,
        embedding: Vec<f32>,
        message_id: &str,
    ) -> Result<(), StoreError>;

    async fn search(
        &self,
        session_id: &str,
        query_embedding: &[f32],
        top_k: usize,
    ) -> Result<Vec<SearchResult>, StoreError>;

    async fn delete_session(&self, session_id: &str) -> Result<(), StoreError>;
}

#[async_trait]
pub trait ShortTermMemory: Send + Sync {
    async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError>;

    async fn get_recent(&self, session_id: &str, count: usize)
    -> Result<Vec<Message>, MemoryError>;

    async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError>;

    async fn trim_to_token_budget(
        &self,
        session_id: &str,
        max_tokens: usize,
        token_counter: &dyn TokenCounter,
    ) -> Result<Vec<Message>, MemoryError>;

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError>;
}

pub trait TokenCounter: Send + Sync {
    fn count_tokens(&self, text: &str) -> usize;
}

#[async_trait]
pub trait CoreMemoryStore: Send + Sync {
    async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError>;

    async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError>;
}

#[derive(Debug, Default)]
pub struct DummyEmbeddingProvider;

#[async_trait]
impl EmbeddingProvider for DummyEmbeddingProvider {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        Ok(texts.iter().map(|_| vec![0.0; 1536]).collect())
    }
}

#[derive(Debug, Default)]
pub struct DummyVectorStore;

#[async_trait]
impl VectorStore for DummyVectorStore {
    async fn insert(
        &self,
        _session_id: &str,
        _text: &str,
        _embedding: Vec<f32>,
        _message_id: &str,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn search(
        &self,
        _session_id: &str,
        _query_embedding: &[f32],
        _top_k: usize,
    ) -> Result<Vec<SearchResult>, StoreError> {
        Ok(Vec::new())
    }

    async fn delete_session(&self, _session_id: &str) -> Result<(), StoreError> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DummyShortTermMemory {
    messages: Mutex<HashMap<String, Vec<Message>>>,
}

#[async_trait]
impl ShortTermMemory for DummyShortTermMemory {
    async fn add_message(&self, session_id: &str, msg: Message) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        messages
            .entry(session_id.to_string())
            .or_default()
            .push(msg);
        Ok(())
    }

    async fn get_recent(
        &self,
        session_id: &str,
        count: usize,
    ) -> Result<Vec<Message>, MemoryError> {
        let messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        let session_messages = messages.get(session_id).cloned().unwrap_or_default();
        let start = session_messages.len().saturating_sub(count);

        Ok(session_messages[start..].to_vec())
    }

    async fn trim(&self, session_id: &str, max_count: usize) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        if let Some(session_messages) = messages.get_mut(session_id) {
            if session_messages.len() > max_count {
                let start = session_messages.len() - max_count;
                session_messages.drain(0..start);
            }
        }

        Ok(())
    }

    async fn trim_to_token_budget(
        &self,
        session_id: &str,
        max_tokens: usize,
        token_counter: &dyn TokenCounter,
    ) -> Result<Vec<Message>, MemoryError> {
        let recent = self.get_recent(session_id, usize::MAX).await?;
        let mut total_tokens = 0;
        let mut trimmed = Vec::new();

        for message in recent.into_iter().rev() {
            let message_tokens = token_counter.count_tokens(&message.content);
            if total_tokens + message_tokens > max_tokens {
                break;
            }

            total_tokens += message_tokens;
            trimmed.push(message);
        }

        trimmed.reverse();
        Ok(trimmed)
    }

    async fn delete_session(&self, session_id: &str) -> Result<(), MemoryError> {
        let mut messages = self
            .messages
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        messages.remove(session_id);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DummyTokenCounter;

impl TokenCounter for DummyTokenCounter {
    fn count_tokens(&self, text: &str) -> usize {
        text.chars().count()
    }
}

#[derive(Debug, Default)]
pub struct DummyCoreMemoryStore {
    facts: Mutex<HashMap<String, Vec<String>>>,
}

#[async_trait]
impl CoreMemoryStore for DummyCoreMemoryStore {
    async fn add_fact(&self, session_id: &str, fact: &str) -> Result<(), MemoryError> {
        let mut facts = self
            .facts
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        facts
            .entry(session_id.to_string())
            .or_default()
            .push(fact.to_string());
        Ok(())
    }

    async fn get_facts(&self, session_id: &str) -> Result<Vec<String>, MemoryError> {
        let facts = self
            .facts
            .lock()
            .map_err(|error| MemoryError::Message(error.to_string()))?;

        Ok(facts.get(session_id).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dummy_embedding_provider_returns_zero_vectors() {
        let provider = DummyEmbeddingProvider::default();
        let inputs = vec!["hello".to_string(), "world".to_string()];

        let embeddings = provider.embed(&inputs).await.unwrap();

        assert_eq!(embeddings.len(), 2);
        assert!(embeddings.iter().all(|embedding| embedding.len() == 1536));
        assert!(
            embeddings
                .iter()
                .all(|embedding| embedding.iter().all(|value| *value == 0.0))
        );
    }

    #[tokio::test]
    async fn dummy_vector_store_supports_insert_search_and_delete() {
        let store = DummyVectorStore::default();

        store
            .insert("session-1", "hello", vec![0.0; 1536], "message-1")
            .await
            .unwrap();

        let results = store.search("session-1", &[0.0; 1536], 5).await.unwrap();
        assert!(results.is_empty());

        store.delete_session("session-1").await.unwrap();
    }

    #[tokio::test]
    async fn dummy_short_term_memory_supports_basic_operations() {
        let store = DummyShortTermMemory::default();
        let token_counter = DummyTokenCounter;
        let message = Message {
            role: "user".to_string(),
            content: "hello".to_string(),
        };

        store
            .add_message("session-1", message.clone())
            .await
            .unwrap();

        let recent = store.get_recent("session-1", 10).await.unwrap();
        assert_eq!(recent, vec![message.clone()]);

        let trimmed = store
            .trim_to_token_budget("session-1", 5, &token_counter)
            .await
            .unwrap();
        assert_eq!(trimmed, vec![message.clone()]);

        store.trim("session-1", 0).await.unwrap();
        let recent = store.get_recent("session-1", 10).await.unwrap();
        assert!(recent.is_empty());

        store.delete_session("session-1").await.unwrap();
    }

    #[test]
    fn dummy_token_counter_counts_characters() {
        let counter = DummyTokenCounter;

        assert_eq!(counter.count_tokens("hello"), 5);
        assert_eq!(counter.count_tokens(""), 0);
    }

    #[tokio::test]
    async fn dummy_core_memory_store_adds_and_reads_facts() {
        let store = DummyCoreMemoryStore::default();

        store
            .add_fact("session-1", "User name is Alex")
            .await
            .unwrap();

        let facts = store.get_facts("session-1").await.unwrap();
        assert_eq!(facts, vec!["User name is Alex".to_string()]);
    }

    #[test]
    fn memory_server_error_wraps_each_trait_error_type() {
        let embed_error = MemoryServerError::from(EmbedError::Message("embed".to_string()));
        let store_error = MemoryServerError::from(StoreError::Message("store".to_string()));
        let memory_error = MemoryServerError::from(MemoryError::Message("memory".to_string()));

        assert_eq!(embed_error.to_string(), "embed");
        assert_eq!(store_error.to_string(), "store");
        assert_eq!(memory_error.to_string(), "memory");
    }
}
