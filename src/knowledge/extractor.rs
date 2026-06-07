use std::collections::HashMap;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::knowledge::types::{Entity, ExtractionResult, Relationship};

#[derive(Debug, Error)]
pub enum ExtractError {
    #[error("extraction API error: {0}")]
    Api(String),
    #[error("extraction parse error: {0}")]
    Parse(String),
    #[error("rate limit exceeded after {retries} retries")]
    RateLimitExceeded { retries: u32 },
}

#[async_trait]
pub trait KnowledgeExtractor: Send + Sync {
    async fn extract(&self, text: &str) -> Result<ExtractionResult, ExtractError>;
}

const SYSTEM_PROMPT: &str = r#"You are a knowledge extraction system. Extract named entities and relationships from the given text.

Respond with valid JSON in exactly this format:
{"entities": [{"name": "string", "type": "Person|Organization|Place|Concept|Event|Other"}], "relationships": [{"from": "entity_name", "to": "entity_name", "type": "relationship_type"}]}

Rules:
- Keep entity names as they appear in the text.
- Relationship types must be snake_case (e.g. works_at, knows, located_in, created_by, part_of).
- Only include relationships between entities you extracted.
- If no entities or relationships are found, return empty arrays.
- Respond with only the JSON object, no surrounding text."#;

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    response_format: ResponseFormat,
    temperature: f32,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    format_type: &'static str,
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

#[derive(Deserialize)]
struct RawExtractionResult {
    entities: Vec<RawEntity>,
    #[serde(default)]
    relationships: Vec<RawRelationship>,
}

#[derive(Deserialize)]
struct RawEntity {
    name: String,
    #[serde(rename = "type")]
    entity_type: String,
}

#[derive(Deserialize)]
struct RawRelationship {
    from: String,
    to: String,
    #[serde(rename = "type")]
    relationship_type: String,
}

pub struct OpenAIKnowledgeExtractor {
    client: Client,
    api_key: String,
    base_url: String,
    model: String,
    max_retries: u32,
}

impl OpenAIKnowledgeExtractor {
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
}

#[async_trait]
impl KnowledgeExtractor for OpenAIKnowledgeExtractor {
    async fn extract(&self, text: &str) -> Result<ExtractionResult, ExtractError> {
        let url = format!("{}/v1/chat/completions", self.base_url);
        let req = ChatRequest {
            model: &self.model,
            messages: vec![
                ChatMessage { role: "system", content: SYSTEM_PROMPT },
                ChatMessage { role: "user", content: text },
            ],
            response_format: ResponseFormat { format_type: "json_object" },
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
                .map_err(|e| ExtractError::Api(e.to_string()))?;

            match resp.status().as_u16() {
                200..=299 => {
                    let chat: ChatResponse = resp
                        .json()
                        .await
                        .map_err(|e| ExtractError::Parse(e.to_string()))?;
                    let content = chat
                        .choices
                        .into_iter()
                        .next()
                        .ok_or_else(|| ExtractError::Parse("empty choices array".to_string()))?
                        .message
                        .content;
                    let raw: RawExtractionResult = serde_json::from_str(&content)
                        .map_err(|e| ExtractError::Parse(format!("{e}: {content}")))?;
                    let entities = raw
                        .entities
                        .into_iter()
                        .map(|e| Entity {
                            name: e.name,
                            entity_type: e.entity_type,
                            attributes: HashMap::new(),
                        })
                        .collect();
                    let relationships = raw
                        .relationships
                        .into_iter()
                        .map(|r| Relationship {
                            from: r.from,
                            to: r.to,
                            relationship_type: r.relationship_type,
                        })
                        .collect();
                    return Ok(ExtractionResult { entities, relationships });
                }
                429 => {
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(ExtractError::RateLimitExceeded { retries: self.max_retries });
                    }
                    let backoff_ms = std::cmp::min(1000u64 << attempt.saturating_sub(1), 30_000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                }
                status => {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(ExtractError::Api(format!("HTTP {status}: {body}")));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn chat_response(content: &str) -> serde_json::Value {
        json!({ "choices": [{ "message": { "content": content } }] })
    }

    #[tokio::test]
    async fn extracts_entities_and_relationships() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_response(
                r#"{"entities":[{"name":"Alice","type":"Person"},{"name":"OpenAI","type":"Organization"}],"relationships":[{"from":"Alice","to":"OpenAI","type":"works_at"}]}"#,
            )))
            .mount(&server)
            .await;

        let extractor = OpenAIKnowledgeExtractor::new_with_base_url("sk-test".into(), server.uri());
        let result = extractor.extract("Alice works at OpenAI").await.unwrap();

        assert_eq!(result.entities.len(), 2);
        assert_eq!(result.entities[0].name, "Alice");
        assert_eq!(result.entities[0].entity_type, "Person");
        assert_eq!(result.relationships.len(), 1);
        assert_eq!(result.relationships[0].relationship_type, "works_at");
    }

    #[tokio::test]
    async fn returns_empty_when_no_entities_found() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_response(
                r#"{"entities":[],"relationships":[]}"#,
            )))
            .mount(&server)
            .await;

        let extractor = OpenAIKnowledgeExtractor::new_with_base_url("sk-test".into(), server.uri());
        let result = extractor.extract("the sky is blue").await.unwrap();
        assert!(result.entities.is_empty());
        assert!(result.relationships.is_empty());
    }

    #[tokio::test]
    async fn retries_on_rate_limit_and_succeeds() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_response(
                r#"{"entities":[],"relationships":[]}"#,
            )))
            .mount(&server)
            .await;

        let extractor = OpenAIKnowledgeExtractor::new_with_base_url("sk-test".into(), server.uri());
        let result = extractor.extract("test").await.unwrap();
        assert!(result.entities.is_empty());
    }

    #[tokio::test]
    async fn exhausted_retries_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let extractor = OpenAIKnowledgeExtractor::new_with_base_url("sk-test".into(), server.uri());
        let err = extractor.extract("test").await.unwrap_err();
        assert!(matches!(err, ExtractError::RateLimitExceeded { .. }));
    }
}
