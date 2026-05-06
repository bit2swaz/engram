use std::env;
use std::path::PathBuf;

use thiserror::Error;

const DEFAULT_REDIS_URL: &str = "redis://localhost:6379";
const DEFAULT_LANCE_DB_PATH: &str = "./data/lancedb";
const DEFAULT_EMBEDDING_DIMENSION: usize = 1536;
const DEFAULT_EMBEDDING_MAX_CONCURRENCY: usize = 10;
const DEFAULT_MPSC_CHANNEL_SIZE: usize = 1_000;
const DEFAULT_SHORT_TERM_COUNT: usize = 20;

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

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Ok(Self {
            redis_url: env::var("REDIS_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string()),
            openai_api_key: required_env("OPENAI_API_KEY")?,
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
        })
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

    use super::{Config, ConfigError};

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

    fn restore_env(name: &str, value: Option<String>) {
        match value {
            Some(value) => unsafe { env::set_var(name, value) },
            None => unsafe { env::remove_var(name) },
        }
    }
}