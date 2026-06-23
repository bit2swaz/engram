use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::models::Message;

const SUMMARIZE_SYSTEM_PROMPT: &str =
    "You are a memory consolidation system. Given a conversation transcript, write a concise \
     third-person summary that preserves the durable facts, decisions, and entities. Be faithful \
     and compact. Respond with only the summary text, no preamble.";

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: AssistantMessage,
}

#[derive(Deserialize)]
struct AssistantMessage {
    content: String,
}

pub struct OpenAISummarizer {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
    max_retries: u32,
}

impl OpenAISummarizer {
    pub fn new(api_key: String) -> Self {
        Self::new_with_base_url(api_key, "https://api.openai.com".to_string())
    }

    pub fn new_with_base_url(api_key: String, base_url: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
            base_url,
            model: "gpt-4o-mini".to_string(),
            max_retries: 3,
        }
    }

    fn transcript(messages: &[Message]) -> String {
        let mut lines = vec!["Conversation:".to_string()];
        lines.extend(messages.iter().map(|m| format!("{}: {}", m.role, m.content)));
        lines.join("\n")
    }
}

#[async_trait]
impl Summarizer for OpenAISummarizer {
    fn model(&self) -> &str {
        &self.model
    }

    async fn summarize(&self, messages: &[Message]) -> Result<String, SummarizeError> {
        let url = format!("{}/v1/chat/completions", self.base_url);
        let transcript = Self::transcript(messages);
        let req = ChatRequest {
            model: &self.model,
            messages: vec![
                ChatMessage { role: "system", content: SUMMARIZE_SYSTEM_PROMPT },
                ChatMessage { role: "user", content: &transcript },
            ],
            temperature: 0.0,
        };

        let mut attempt = 0u32;
        loop {
            let resp = self
                .client
                .post(&url)
                .bearer_auth(&self.api_key)
                .json(&req)
                .send()
                .await
                .map_err(|e| SummarizeError::Api(e.to_string()))?;

            match resp.status().as_u16() {
                200..=299 => {
                    let chat: ChatResponse = resp
                        .json()
                        .await
                        .map_err(|e| SummarizeError::Parse(e.to_string()))?;
                    let content = chat
                        .choices
                        .into_iter()
                        .next()
                        .ok_or_else(|| SummarizeError::Parse("empty choices array".into()))?
                        .message
                        .content;
                    return Ok(content.trim().to_string());
                }
                429 => {
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(SummarizeError::RateLimitExceeded {
                            retries: self.max_retries,
                        });
                    }
                    // Exponential backoff, same pattern as extractor.rs.
                    let backoff_ms = std::cmp::min(1000u64 << attempt.saturating_sub(1), 30_000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                }
                status => {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(SummarizeError::Api(format!("HTTP {status}: {body}")));
                }
            }
        }
    }
}

pub const SUMMARIZE_PROMPT_VERSION: &str = "summarize_v1";

#[derive(Debug, Error)]
pub enum SummarizeError {
    #[error("summarize API error: {0}")]
    Api(String),
    #[error("summarize parse error: {0}")]
    Parse(String),
    #[error("rate limit exceeded after {retries} retries")]
    RateLimitExceeded { retries: u32 },
}

#[async_trait]
pub trait Summarizer: Send + Sync {
    async fn summarize(&self, messages: &[Message]) -> Result<String, SummarizeError>;
    fn model(&self) -> &str;
}

pub struct MockSummarizer;

#[async_trait]
impl Summarizer for MockSummarizer {
    async fn summarize(&self, messages: &[Message]) -> Result<String, SummarizeError> {
        let body = messages
            .iter()
            .map(|m| {
                let snippet: String = m.content.chars().take(80).collect();
                format!("{}: {}", m.role, snippet)
            })
            .collect::<Vec<_>>()
            .join("; ");
        Ok(format!("Summary of {} messages: {}", messages.len(), body))
    }

    fn model(&self) -> &str {
        "mock"
    }
}

#[cfg(test)]
mod mock_tests {
    use super::*;
    use crate::models::Message;

    fn msg(role: &str, content: &str) -> Message {
        Message {
            id: Some(format!("{role}-{content}")),
            role: role.into(),
            content: content.into(),
            timestamp: None,
            embedding_status: None,
        }
    }

    #[tokio::test]
    async fn mock_is_deterministic_and_nonempty() {
        let msgs = vec![msg("user", "Alice works at OpenAI"), msg("assistant", "Noted")];
        let a = MockSummarizer.summarize(&msgs).await.unwrap();
        let b = MockSummarizer.summarize(&msgs).await.unwrap();
        assert_eq!(a, b, "mock summarizer must be deterministic");
        assert!(!a.is_empty());
        assert!(a.contains("Alice works at OpenAI"));
    }

    #[tokio::test]
    async fn mock_model_label_is_mock() {
        assert_eq!(MockSummarizer.model(), "mock");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::Message;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn msg(role: &str, content: &str) -> Message {
        Message {
            id: None,
            role: role.into(),
            content: content.into(),
            timestamp: None,
            embedding_status: None,
        }
    }

    fn chat_response(content: &str) -> serde_json::Value {
        json!({ "choices": [{ "message": { "content": content } }] })
    }

    #[tokio::test]
    async fn openai_summarizer_returns_summary_text() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(chat_response("Alice works at OpenAI.")),
            )
            .mount(&server)
            .await;

        let s = OpenAISummarizer::new_with_base_url("sk-test".into(), server.uri());
        let out = s.summarize(&[msg("user", "Alice works at OpenAI")]).await.unwrap();
        assert_eq!(out, "Alice works at OpenAI.");
        assert_eq!(s.model(), "gpt-4o-mini");
    }

    #[tokio::test]
    async fn openai_summarizer_retries_on_rate_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_response("ok")),
            )
            .mount(&server)
            .await;

        let s = OpenAISummarizer::new_with_base_url("sk-test".into(), server.uri());
        assert_eq!(s.summarize(&[msg("user", "hi")]).await.unwrap(), "ok");
    }

    #[tokio::test]
    async fn openai_summarizer_exhausts_retries() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let s = OpenAISummarizer::new_with_base_url("sk-test".into(), server.uri());
        let err = s.summarize(&[msg("user", "hi")]).await.unwrap_err();
        assert!(matches!(err, SummarizeError::RateLimitExceeded { .. }));
    }
}
