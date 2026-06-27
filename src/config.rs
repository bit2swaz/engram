use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use thiserror::Error;

/// A single Raft peer's node ID and gRPC address.
///
/// NOTE: IPv6 addresses are not supported. The `host:port` format relies on
/// splitting on `:` and would mis-parse `[::1]:9001`. This is acceptable for
/// Stage 1 (Docker Compose assigns IPv4 names like `node-1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerConfig {
    pub id: u64,
    pub addr: String, // "host:grpc_port"
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KnowledgeExtractorType {
    OpenAI,
    Mock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummarizerType {
    Mock,
    OpenAI,
}

const DEFAULT_REDIS_URL: &str = "redis://localhost:6379";
const DEFAULT_LANCE_DB_PATH: &str = "./data/lancedb";
const DEFAULT_EMBEDDING_DIMENSION: usize = 1536;
const DEFAULT_EMBEDDING_MAX_CONCURRENCY: usize = 10;
const DEFAULT_MPSC_CHANNEL_SIZE: usize = 1_000;
const DEFAULT_SHORT_TERM_COUNT: usize = 20;
const DEFAULT_RAFT_DB_PATH: &str = "./data/raft/engram.redb";
const DEFAULT_SNAPSHOT_LOG_THRESHOLD: u64 = 1000;
const DEFAULT_CONSOLIDATION_THRESHOLD: usize = 50;
const DEFAULT_CONSOLIDATION_TARGET_WINDOW: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub redis_url: String,
    pub openai_api_key: String,
    pub openai_base_url: Option<String>,
    pub lance_db_path: PathBuf,
    pub embedding_dimension: usize,
    pub embedding_max_concurrency: usize,
    pub mpsc_channel_size: usize,
    pub short_term_count: usize,
    /// Set via NODE_ID env var. None means standalone (non-cluster) mode.
    pub node_id: Option<u64>,
    /// gRPC listen address for this node's Raft server, e.g. "0.0.0.0:9001".
    /// Set via RAFT_ADDR env var.
    pub raft_addr: Option<String>,
    /// gRPC address advertised to peers and stored in cluster membership.
    /// Must be a routable hostname/IP, e.g. "node-1:9001".
    /// Defaults to raft_addr when not set which is only correct if raft_addr is already a routable address.
    /// Set via RAFT_ADVERTISE_ADDR env var.
    pub raft_advertise_addr: Option<String>,
    /// Other Raft peers in the cluster, parsed from CLUSTER_PEERS.
    /// Format: "id:host:grpc_port,id:host:grpc_port"
    pub cluster_peers: Vec<PeerConfig>,
    /// HTTP addresses of peer nodes keyed by node ID, parsed from CLUSTER_HTTP_PEERS.
    /// Format: "id:host:http_port,id:host:http_port"
    pub cluster_http_peers: HashMap<u64, String>,
    pub knowledge_max_workers: usize,
    pub knowledge_channel_size: usize,
    pub knowledge_extractor: KnowledgeExtractorType,
    /// Path to the redb file backing the persistent Raft log + snapshot store.
    /// Set via RAFT_DB_PATH. Each node needs its own path/volume.
    pub raft_db_path: std::path::PathBuf,
    /// Build a snapshot every N committed log entries (openraft SnapshotPolicy::LogsSinceLast).
    /// Set via SNAPSHOT_LOG_THRESHOLD.
    pub snapshot_log_threshold: u64,
    /// Which summarizer the consolidation scheduler calls. Mock is offline/deterministic.
    pub summarizer: SummarizerType,
    /// A session crossing this many short-term messages becomes a consolidation candidate.
    pub consolidation_threshold: usize,
    /// Consolidation drives a session back down to exactly this many raw messages.
    pub consolidation_target_window: usize,
    pub consolidation_max_workers: usize,
    pub consolidation_channel_size: usize,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("environment variable {name} must not be empty")]
    EmptyValue { name: &'static str },
    #[error("environment variable {name} is required")]
    MissingRequired { name: &'static str },
    #[error("environment variable {name} must be a positive integer")]
    InvalidPositiveInteger { name: &'static str },
}

impl Default for Config {
    fn default() -> Self {
        Self {
            redis_url: DEFAULT_REDIS_URL.to_string(),
            openai_api_key: String::new(),
            openai_base_url: None,
            lance_db_path: std::path::PathBuf::from(DEFAULT_LANCE_DB_PATH),
            embedding_dimension: DEFAULT_EMBEDDING_DIMENSION,
            embedding_max_concurrency: DEFAULT_EMBEDDING_MAX_CONCURRENCY,
            mpsc_channel_size: DEFAULT_MPSC_CHANNEL_SIZE,
            short_term_count: DEFAULT_SHORT_TERM_COUNT,
            node_id: None,
            raft_addr: None,
            raft_advertise_addr: None,
            cluster_peers: vec![],
            cluster_http_peers: HashMap::new(),
            knowledge_max_workers: 4,
            knowledge_channel_size: 500,
            knowledge_extractor: KnowledgeExtractorType::OpenAI,
            raft_db_path: std::path::PathBuf::from(DEFAULT_RAFT_DB_PATH),
            snapshot_log_threshold: DEFAULT_SNAPSHOT_LOG_THRESHOLD,
            summarizer: SummarizerType::OpenAI,
            consolidation_threshold: DEFAULT_CONSOLIDATION_THRESHOLD,
            consolidation_target_window: DEFAULT_CONSOLIDATION_TARGET_WINDOW,
            consolidation_max_workers: 2,
            consolidation_channel_size: 100,
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let knowledge_extractor = match env::var("KNOWLEDGE_EXTRACTOR").as_deref() {
            Ok("mock") => KnowledgeExtractorType::Mock,
            _ => KnowledgeExtractorType::OpenAI,
        };
        Ok(Self {
            redis_url: env::var("REDIS_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string()),
            openai_api_key: if knowledge_extractor == KnowledgeExtractorType::Mock {
                env::var("OPENAI_API_KEY").unwrap_or_default()
            } else {
                required_env("OPENAI_API_KEY")?
            },
            openai_base_url: optional_env("OPENAI_BASE_URL")?,
            lance_db_path: PathBuf::from(optional_lance_db_path()?),
            embedding_dimension: positive_usize_env(
                "EMBEDDING_DIMENSION",
                DEFAULT_EMBEDDING_DIMENSION,
            )?,
            embedding_max_concurrency: positive_usize_env(
                "EMBEDDING_MAX_CONCURRENCY",
                DEFAULT_EMBEDDING_MAX_CONCURRENCY,
            )?,
            mpsc_channel_size: positive_usize_env(
                "MPSC_CHANNEL_SIZE",
                DEFAULT_MPSC_CHANNEL_SIZE,
            )?,
            short_term_count: positive_usize_env(
                "SHORT_TERM_COUNT",
                DEFAULT_SHORT_TERM_COUNT,
            )?,
            node_id: env::var("NODE_ID").ok().and_then(|s| s.trim().parse().ok()),
            raft_addr: optional_env("RAFT_ADDR")?,
            raft_advertise_addr: optional_env("RAFT_ADVERTISE_ADDR")?,
            cluster_peers: Self::parse_cluster_peers(
                &env::var("CLUSTER_PEERS").unwrap_or_default(),
            ),
            cluster_http_peers: Self::parse_http_peers(
                &env::var("CLUSTER_HTTP_PEERS").unwrap_or_default(),
            ),
            knowledge_max_workers:  positive_usize_env("KNOWLEDGE_MAX_WORKERS",  4)?,
            knowledge_channel_size: positive_usize_env("KNOWLEDGE_CHANNEL_SIZE", 500)?,
            knowledge_extractor,
            raft_db_path: PathBuf::from(
                optional_env("RAFT_DB_PATH")?.unwrap_or_else(|| DEFAULT_RAFT_DB_PATH.to_string()),
            ),
            snapshot_log_threshold: positive_u64_env(
                "SNAPSHOT_LOG_THRESHOLD",
                DEFAULT_SNAPSHOT_LOG_THRESHOLD,
            )?,
            summarizer: match env::var("SUMMARIZER").as_deref() {
                Ok("mock") => SummarizerType::Mock,
                _ => SummarizerType::OpenAI,
            },
            consolidation_threshold: positive_usize_env(
                "CONSOLIDATION_THRESHOLD",
                DEFAULT_CONSOLIDATION_THRESHOLD,
            )?,
            consolidation_target_window: positive_usize_env(
                "CONSOLIDATION_TARGET_WINDOW",
                DEFAULT_CONSOLIDATION_TARGET_WINDOW,
            )?,
            consolidation_max_workers: positive_usize_env("CONSOLIDATION_MAX_WORKERS", 2)?,
            consolidation_channel_size: positive_usize_env("CONSOLIDATION_CHANNEL_SIZE", 100)?,
        })
    }

    pub fn parse_cluster_peers(raw: &str) -> Vec<PeerConfig> {
        raw.split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| parse_peer_entry(s).map(|(id, addr)| PeerConfig { id, addr }))
            .collect()
    }

    pub fn parse_http_peers(raw: &str) -> HashMap<u64, String> {
        raw.split(',')
            .filter(|s| !s.is_empty())
            .filter_map(parse_peer_entry)
            .collect()
    }
}

/// Parses a single "id:host:port" entry into (node_id, "host:port").
/// Returns None if the entry is malformed or the id is not a valid u64.
fn parse_peer_entry(s: &str) -> Option<(u64, String)> {
    let parts: Vec<&str> = s.splitn(3, ':').collect();
    if parts.len() == 3 {
        parts[0]
            .trim()
            .parse::<u64>()
            .ok()
            .map(|id| (id, format!("{}:{}", parts[1].trim(), parts[2].trim())))
    } else {
        None
    }
}

fn required_env(name: &'static str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::MissingRequired { name })?;
    let trimmed = value.trim();

    if trimmed.is_empty() {
        return Err(ConfigError::EmptyValue { name });
    }

    Ok(trimmed.to_string())
}

fn optional_env(name: &'static str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }

            Ok(Some(trimmed.to_string()))
        }
        Err(_) => Ok(None),
    }
}

fn optional_lance_db_path() -> Result<String, ConfigError> {
    if let Ok(value) = env::var("LANCE_DB_PATH") {
        if value.trim().is_empty() {
            return Err(ConfigError::EmptyValue {
                name: "LANCE_DB_PATH",
            });
        }

        return Ok(value);
    }

    if let Ok(value) = env::var("LANCEDB_PATH") {
        if value.trim().is_empty() {
            return Err(ConfigError::EmptyValue {
                name: "LANCEDB_PATH",
            });
        }

        return Ok(value);
    }

    Ok(DEFAULT_LANCE_DB_PATH.to_string())
}

fn positive_u64_env(name: &'static str, default: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ConfigError::InvalidPositiveInteger { name }),
        Err(_) => Ok(default),
    }
}

fn positive_usize_env(name: &'static str, default: usize) -> Result<usize, ConfigError> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ConfigError::InvalidPositiveInteger { name }),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::{Mutex, OnceLock};

    use super::{Config, ConfigError, KnowledgeExtractorType, SummarizerType};

    fn env_lock() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn from_env_uses_defaults_for_optional_settings() {
        let _guard = env_lock().lock().unwrap();
        let old_redis = env::var("REDIS_URL").ok();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_base_url = env::var("OPENAI_BASE_URL").ok();
        let old_lance = env::var("LANCE_DB_PATH").ok();
        let old_lancedb = env::var("LANCEDB_PATH").ok();
        let old_embedding_dimension = env::var("EMBEDDING_DIMENSION").ok();
        let old_concurrency = env::var("EMBEDDING_MAX_CONCURRENCY").ok();
        let old_channel = env::var("MPSC_CHANNEL_SIZE").ok();
        let old_short_term_count = env::var("SHORT_TERM_COUNT").ok();

        unsafe {
            env::remove_var("REDIS_URL");
            env::set_var("OPENAI_API_KEY", "test-key");
            env::remove_var("OPENAI_BASE_URL");
            env::remove_var("LANCE_DB_PATH");
            env::remove_var("LANCEDB_PATH");
            env::remove_var("EMBEDDING_DIMENSION");
            env::remove_var("EMBEDDING_MAX_CONCURRENCY");
            env::remove_var("MPSC_CHANNEL_SIZE");
            env::remove_var("SHORT_TERM_COUNT");
        }

        let config = Config::from_env().unwrap();

        assert_eq!(config.redis_url, "redis://localhost:6379");
        assert_eq!(config.openai_api_key, "test-key");
        assert_eq!(config.openai_base_url, None);
        assert_eq!(config.lance_db_path.to_string_lossy(), "./data/lancedb");
        assert_eq!(config.embedding_dimension, 1536);
        assert_eq!(config.embedding_max_concurrency, 10);
        assert_eq!(config.mpsc_channel_size, 1_000);
        assert_eq!(config.short_term_count, 20);

        restore_env("REDIS_URL", old_redis);
        restore_env("OPENAI_API_KEY", old_key);
        restore_env("OPENAI_BASE_URL", old_base_url);
        restore_env("LANCE_DB_PATH", old_lance);
        restore_env("LANCEDB_PATH", old_lancedb);
        restore_env("EMBEDDING_DIMENSION", old_embedding_dimension);
        restore_env("EMBEDDING_MAX_CONCURRENCY", old_concurrency);
        restore_env("MPSC_CHANNEL_SIZE", old_channel);
        restore_env("SHORT_TERM_COUNT", old_short_term_count);
    }

    #[test]
    fn from_env_reads_openai_base_url_when_present() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_base_url = env::var("OPENAI_BASE_URL").ok();

        unsafe {
            env::set_var("OPENAI_API_KEY", "test-key");
            env::set_var("OPENAI_BASE_URL", "http://127.0.0.1:4010");
        }

        let config = Config::from_env().unwrap();

        assert_eq!(config.openai_base_url.as_deref(), Some("http://127.0.0.1:4010"));

        restore_env("OPENAI_API_KEY", old_key);
        restore_env("OPENAI_BASE_URL", old_base_url);
    }

    #[test]
    fn from_env_treats_blank_openai_base_url_as_missing() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_base_url = env::var("OPENAI_BASE_URL").ok();

        unsafe {
            env::set_var("OPENAI_API_KEY", "test-key");
            env::set_var("OPENAI_BASE_URL", "   ");
        }

        let config = Config::from_env().unwrap();

        assert_eq!(config.openai_base_url, None);

        restore_env("OPENAI_API_KEY", old_key);
        restore_env("OPENAI_BASE_URL", old_base_url);
    }

    #[test]
    fn from_env_reads_embedding_dimension_when_present() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_embedding_dimension = env::var("EMBEDDING_DIMENSION").ok();

        unsafe {
            env::set_var("OPENAI_API_KEY", "test-key");
            env::set_var("EMBEDDING_DIMENSION", "384");
        }

        let config = Config::from_env().unwrap();

        assert_eq!(config.embedding_dimension, 384);

        restore_env("OPENAI_API_KEY", old_key);
        restore_env("EMBEDDING_DIMENSION", old_embedding_dimension);
    }

    #[test]
    fn from_env_requires_openai_api_key() {
        let _guard = env_lock().lock().unwrap();
        let previous_value = env::var("OPENAI_API_KEY").ok();
        unsafe { env::remove_var("OPENAI_API_KEY") };

        let error = Config::from_env().unwrap_err();

        restore_env("OPENAI_API_KEY", previous_value);
        assert!(matches!(
            error,
            ConfigError::MissingRequired {
                name: "OPENAI_API_KEY"
            }
        ));
    }

    #[test]
    fn knowledge_extractor_defaults_to_openai() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_extractor = env::var("KNOWLEDGE_EXTRACTOR").ok();
        unsafe {
            env::set_var("OPENAI_API_KEY", "test-key");
            env::remove_var("KNOWLEDGE_EXTRACTOR");
        }
        let config = Config::from_env().unwrap();
        assert_eq!(config.knowledge_extractor, KnowledgeExtractorType::OpenAI);
        restore_env("OPENAI_API_KEY", old_key);
        restore_env("KNOWLEDGE_EXTRACTOR", old_extractor);
    }

    #[test]
    fn knowledge_extractor_mock_parsed_from_env() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_extractor = env::var("KNOWLEDGE_EXTRACTOR").ok();
        unsafe {
            env::remove_var("OPENAI_API_KEY");
            env::set_var("KNOWLEDGE_EXTRACTOR", "mock");
        }
        let config = Config::from_env().unwrap();
        assert_eq!(config.knowledge_extractor, KnowledgeExtractorType::Mock);
        restore_env("OPENAI_API_KEY", old_key);
        restore_env("KNOWLEDGE_EXTRACTOR", old_extractor);
    }

    #[test]
    fn mock_mode_does_not_require_openai_api_key() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_extractor = env::var("KNOWLEDGE_EXTRACTOR").ok();
        unsafe {
            env::remove_var("OPENAI_API_KEY");
            env::set_var("KNOWLEDGE_EXTRACTOR", "mock");
        }
        let result = Config::from_env();
        assert!(result.is_ok(), "mock mode should not require OPENAI_API_KEY");
        restore_env("OPENAI_API_KEY", old_key);
        restore_env("KNOWLEDGE_EXTRACTOR", old_extractor);
    }

    #[test]
    fn consolidation_defaults_and_overrides() {
        // Defaults when unset.
        let cfg = Config::default();
        assert_eq!(cfg.consolidation_threshold, 50);
        assert_eq!(cfg.consolidation_target_window, 20);
        assert!(matches!(cfg.summarizer, SummarizerType::OpenAI));
    }

    #[test]
    fn summarizer_mock_parsed_from_env() {
        let _guard = env_lock().lock().unwrap();
        let old_key = env::var("OPENAI_API_KEY").ok();
        let old_summarizer = env::var("SUMMARIZER").ok();
        unsafe {
            env::set_var("OPENAI_API_KEY", "test-key");
            env::set_var("SUMMARIZER", "mock");
        }
        let config = Config::from_env().unwrap();
        assert_eq!(config.summarizer, SummarizerType::Mock);
        restore_env("OPENAI_API_KEY", old_key);
        restore_env("SUMMARIZER", old_summarizer);
    }

    fn restore_env(name: &str, value: Option<String>) {
        match value {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }
    }
}

#[cfg(test)]
mod cluster_config_tests {
    use super::*;

    #[test]
    fn parses_cluster_peers() {
        let peers = Config::parse_cluster_peers("1:node1:9001,2:node2:9001");
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].id, 1);
        assert_eq!(peers[0].addr, "node1:9001");
        assert_eq!(peers[1].id, 2);
        assert_eq!(peers[1].addr, "node2:9001");
    }

    #[test]
    fn parses_http_peers() {
        let m = Config::parse_http_peers("1:node1:3000,2:node2:3001");
        assert_eq!(m.get(&1u64).unwrap(), "node1:3000");
        assert_eq!(m.get(&2u64).unwrap(), "node2:3001");
    }

    #[test]
    fn empty_peers_returns_empty_collections() {
        assert!(Config::parse_cluster_peers("").is_empty());
        assert!(Config::parse_http_peers("").is_empty());
    }

    #[test]
    fn malformed_peer_entry_is_skipped() {
        let peers = Config::parse_cluster_peers("bad-entry,1:node1:9001");
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id, 1);
    }

    #[test]
    fn defaults_persistence_paths_and_threshold() {
        let cfg = Config::default();
        assert_eq!(cfg.raft_db_path.to_string_lossy(), "./data/raft/engram.redb");
        assert_eq!(cfg.snapshot_log_threshold, 1000);
    }
}