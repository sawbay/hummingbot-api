use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub port: u16,
    pub broker_host: String,
    pub broker_port: u16,
    pub broker_username: Option<String>,
    pub broker_password: Option<String>,
    pub broker_ssl: bool,
    pub bots_path: PathBuf,
    pub pool_bots: Vec<String>,
    pub command_timeout_secs: u64,
    pub heartbeat_timeout_secs: u64,
    pub r2_enabled: bool,
    pub r2_bucket: String,
    pub r2_endpoint_url: String,
    pub r2_access_key_id: String,
    pub r2_secret_access_key: String,
    pub r2_prefix: String,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        Ok(Self {
            port: env_parse("RS_ORCHESTRATOR_PORT", 8001),
            broker_host: std::env::var("BROKER_HOST").unwrap_or_else(|_| "localhost".to_string()),
            broker_port: env_parse("BROKER_PORT", 1883),
            broker_username: empty_to_none(std::env::var("BROKER_USERNAME").ok()),
            broker_password: empty_to_none(std::env::var("BROKER_PASSWORD").ok()),
            broker_ssl: env_parse("BROKER_SSL", false),
            bots_path: PathBuf::from(
                std::env::var("BOTS_PATH").unwrap_or_else(|_| ".".to_string()),
            ),
            pool_bots: parse_pool_bots(),
            command_timeout_secs: env_parse("COMMAND_TIMEOUT_SECS", 30),
            heartbeat_timeout_secs: env_parse("HEARTBEAT_TIMEOUT_SECS", 30),
            r2_enabled: env_parse("R2_ENABLED", false),
            r2_bucket: std::env::var("R2_BUCKET").unwrap_or_default(),
            r2_endpoint_url: std::env::var("R2_ENDPOINT_URL").unwrap_or_default(),
            r2_access_key_id: std::env::var("R2_ACCESS_KEY_ID").unwrap_or_default(),
            r2_secret_access_key: std::env::var("R2_SECRET_ACCESS_KEY").unwrap_or_default(),
            r2_prefix: std::env::var("R2_PREFIX").unwrap_or_else(|_| "bots".to_string()),
        })
    }

    pub fn command_timeout(&self) -> Duration {
        Duration::from_secs(self.command_timeout_secs)
    }

    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_secs(self.heartbeat_timeout_secs)
    }
}

fn env_parse<T>(key: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<T>().ok())
        .unwrap_or(default)
}

fn parse_pool_bots() -> Vec<String> {
    std::env::var("POOL_BOTS")
        .unwrap_or_else(|_| "warmbot_1,warmbot_2,warmbot_3".to_string())
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn empty_to_none(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        if item.trim().is_empty() {
            None
        } else {
            Some(item)
        }
    })
}
