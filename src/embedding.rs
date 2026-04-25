use std::env;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::core::{EmbedError, EmbeddingProvider};

const DEFAULT_BASE_URL: &str = "https://api.openai.com";
const DEFAULT_MODEL: &str = "text-embedding-3-small";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_BASE_BACKOFF_MS: u64 = 1_000;
const MAX_BACKOFF_MS: u64 = 30_000;

pub struct OpenAIEmbedder {
    http_client: reqwest::Client,
    api_key: String,
    base_url: String,
    model: String,
    max_retries: u32,
    base_backoff_ms: u64,
}

#[derive(Debug, Serialize)]
struct EmbeddingsRequest {
    input: Vec<String>,
    model: String,
}

#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

impl OpenAIEmbedder {
    pub fn new() -> Result<Self, EmbedError> {
        let api_key = env::var("OPENAI_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or(EmbedError::MissingApiKey)?;

        Self::new_with_config(
            api_key,
            DEFAULT_BASE_URL.to_string(),
            DEFAULT_MODEL.to_string(),
            DEFAULT_MAX_RETRIES,
            DEFAULT_BASE_BACKOFF_MS,
        )
    }

    fn new_with_config(
        api_key: String,
        base_url: String,
        model: String,
        max_retries: u32,
        base_backoff_ms: u64,
    ) -> Result<Self, EmbedError> {
        if api_key.trim().is_empty() {
            return Err(EmbedError::MissingApiKey);
        }

        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| EmbedError::Other(error.into()))?;

        Ok(Self {
            http_client,
            api_key,
            base_url: base_url.trim_end_matches('/').to_string(),
            model,
            max_retries,
            base_backoff_ms,
        })
    }

    fn endpoint(&self) -> String {
        format!("{}/v1/embeddings", self.base_url)
    }

    fn request_body(&self, texts: &[String]) -> EmbeddingsRequest {
        EmbeddingsRequest {
            input: texts.to_vec(),
            model: self.model.clone(),
        }
    }

    fn next_backoff_ms(&self, attempt: u32) -> u64 {
        let multiplier = 2_u64.saturating_pow(attempt);
        self.base_backoff_ms
            .saturating_mul(multiplier)
            .min(MAX_BACKOFF_MS)
    }
}

#[async_trait]
impl EmbeddingProvider for OpenAIEmbedder {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let request_body = self.request_body(texts);

        for attempt in 0..=self.max_retries {
            let response = self
                .http_client
                .post(self.endpoint())
                .bearer_auth(&self.api_key)
                .json(&request_body)
                .send()
                .await
                .map_err(|error| EmbedError::Other(error.into()))?;

            match response.status() {
                StatusCode::OK => {
                    let payload: EmbeddingsResponse = response
                        .json()
                        .await
                        .map_err(|error| EmbedError::Other(error.into()))?;

                    if payload.data.is_empty() {
                        return Err(EmbedError::InvalidResponse(
                            "response contained no embeddings".to_string(),
                        ));
                    }

                    if payload.data.len() != texts.len() {
                        return Err(EmbedError::InvalidResponse(format!(
                            "expected {} embeddings, received {}",
                            texts.len(),
                            payload.data.len()
                        )));
                    }

                    if payload.data.iter().any(|item| item.embedding.is_empty()) {
                        return Err(EmbedError::InvalidResponse(
                            "response contained an empty embedding".to_string(),
                        ));
                    }

                    return Ok(payload
                        .data
                        .into_iter()
                        .map(|item| item.embedding)
                        .collect());
                }
                StatusCode::TOO_MANY_REQUESTS => {
                    if attempt == self.max_retries {
                        return Err(EmbedError::RateLimitExceeded);
                    }

                    sleep(Duration::from_millis(self.next_backoff_ms(attempt))).await;
                }
                status => {
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "<unreadable response body>".to_string());
                    return Err(EmbedError::HttpStatus {
                        status: status.as_u16(),
                        body,
                    });
                }
            }
        }

        Err(EmbedError::RateLimitExceeded)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, Request, ResponseTemplate};

    use super::OpenAIEmbedder;
    use crate::core::{EmbedError, EmbeddingProvider};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    fn embedding_payload(length: usize) -> serde_json::Value {
        json!({
            "data": [
                {
                    "embedding": vec![0.123_f32; length],
                    "index": 0,
                    "object": "embedding"
                }
            ],
            "model": "text-embedding-3-small",
            "object": "list",
            "usage": {
                "prompt_tokens": 8,
                "total_tokens": 8
            }
        })
    }

    #[tokio::test]
    async fn successful_request_returns_embedding_with_expected_length() {
        let mock_server = MockServer::start().await;
        let request_body = json!({
            "input": ["hello"],
            "model": "text-embedding-3-small"
        });

        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .and(header("Authorization", "Bearer test-key"))
            .and(body_json(&request_body))
            .respond_with(ResponseTemplate::new(200).set_body_json(embedding_payload(1536)))
            .expect(1)
            .mount(&mock_server)
            .await;

        let embedder = OpenAIEmbedder::new_with_config(
            "test-key".to_string(),
            mock_server.uri(),
            "text-embedding-3-small".to_string(),
            3,
            1,
        )
        .unwrap();

        let embeddings = embedder.embed(&["hello".to_string()]).await.unwrap();

        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), 1536);
    }

    #[tokio::test]
    async fn rate_limit_with_eventual_success_retries_until_success() {
        let mock_server = MockServer::start().await;
        let request_body = json!({
            "input": ["hello"],
            "model": "text-embedding-3-small"
        });
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_responder = attempts.clone();

        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .and(header("Authorization", "Bearer test-key"))
            .and(body_json(&request_body))
            .respond_with(move |_request: &Request| {
                let attempt = attempts_for_responder.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    ResponseTemplate::new(429).insert_header("Retry-After", "1")
                } else {
                    ResponseTemplate::new(200).set_body_json(embedding_payload(1536))
                }
            })
            .expect(3)
            .mount(&mock_server)
            .await;

        let embedder = OpenAIEmbedder::new_with_config(
            "test-key".to_string(),
            mock_server.uri(),
            "text-embedding-3-small".to_string(),
            3,
            1,
        )
        .unwrap();

        let embeddings = embedder.embed(&["hello".to_string()]).await.unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].len(), 1536);
    }

    #[tokio::test]
    async fn rate_limit_exhausted_returns_error_after_max_retries() {
        let mock_server = MockServer::start().await;
        let request_body = json!({
            "input": ["hello"],
            "model": "text-embedding-3-small"
        });
        let attempts = Arc::new(AtomicUsize::new(0));
        let attempts_for_responder = attempts.clone();

        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .and(header("Authorization", "Bearer test-key"))
            .and(body_json(&request_body))
            .respond_with(move |_request: &Request| {
                attempts_for_responder.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(429)
            })
            .expect(4)
            .mount(&mock_server)
            .await;

        let embedder = OpenAIEmbedder::new_with_config(
            "test-key".to_string(),
            mock_server.uri(),
            "text-embedding-3-small".to_string(),
            3,
            1,
        )
        .unwrap();

        let error = embedder.embed(&["hello".to_string()]).await.unwrap_err();

        assert_eq!(attempts.load(Ordering::SeqCst), 4);
        assert!(matches!(error, EmbedError::RateLimitExceeded));
    }

    #[test]
    fn constructor_returns_error_when_api_key_is_missing() {
        let _guard = env_lock().lock().unwrap();
        let previous_value = env::var("OPENAI_API_KEY").ok();
        unsafe { env::remove_var("OPENAI_API_KEY") };

        let result = OpenAIEmbedder::new();

        match previous_value {
            Some(value) => unsafe { env::set_var("OPENAI_API_KEY", value) },
            None => unsafe { env::remove_var("OPENAI_API_KEY") },
        }

        assert!(matches!(result, Err(EmbedError::MissingApiKey)));
    }
}
