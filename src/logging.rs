use std::env;
use std::sync::Once;

use tracing_subscriber::EnvFilter;

static TRACING_INIT: Once = Once::new();

pub fn init_tracing() {
    TRACING_INIT.call_once(|| {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info"));
        let log_format = env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());

        if log_format.eq_ignore_ascii_case("json") {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .json()
                .try_init();
        } else {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .pretty()
                .try_init();
        }
    });
}