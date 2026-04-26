use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub struct Message {
    pub id: Option<String>,
    pub role: String,
    pub content: String,
    pub timestamp: Option<DateTime<Utc>>,
    pub embedding_status: Option<EmbeddingStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum EmbeddingStatus {
    Pending,
    Processing,
    Completed,
    Failed(String),
}
