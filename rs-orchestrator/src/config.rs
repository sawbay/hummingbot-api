use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    pub port: u16,
    pub database_url: String,
    pub broker_host: String,
    pub broker_port: u16,
    pub broker_username: Option<String>,
    pub broker_password: Option<String>,
    pub broker_ssl: bool,
    pub bots_path: PathBuf,
    pub pool_bots: Vec<String>,
    pub command_timeout_secs: u64,
    pub heartbeat_timeout_secs: u64,
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        let raw_database_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://hbot:hummingbot-api@localhost:5432/hummingbot_api".to_string()
        });
        let database_url = raw_database_url.replace("postgresql+asyncpg://", "postgresql://");

        Ok(Self {
            port: env_parse("RS_ORCHESTRATOR_PORT", 8001),
            database_url,
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
        .unwrap_or_else(|_| "bot_1,bot_2,bot_3".to_string())
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
