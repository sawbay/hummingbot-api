use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlotStatus {
    Offline,
    Bootstrapping,
    Idle,
    Reserved,
    Configuring,
    Running,
    Stopping,
    Cleanup,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SlotState {
    pub bot_name: String,
    pub status: SlotStatus,
    pub assigned_run_id: Option<i32>,
    pub account_name: Option<String>,
    pub last_heartbeat: Option<DateTime<Utc>>,
    pub current_config_name: Option<String>,
    pub current_controller_ids: Vec<String>,
    pub last_error: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl SlotState {
    pub fn new(bot_name: String) -> Self {
        Self {
            bot_name,
            status: SlotStatus::Offline,
            assigned_run_id: None,
            account_name: None,
            last_heartbeat: None,
            current_config_name: None,
            current_controller_ids: Vec::new(),
            last_error: None,
            updated_at: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct V2ControllerDeployment {
    pub instance_name: String,
    pub credentials_profile: String,
    pub controllers_config: Vec<String>,
    pub max_global_drawdown_quote: Option<f64>,
    pub max_controller_drawdown_quote: Option<f64>,
    #[serde(default = "default_image")]
    pub image: String,
    pub script_config: Option<String>,
    #[serde(default)]
    pub headless: bool,
}

fn default_image() -> String {
    "hummingbot/hummingbot:latest".to_string()
}

#[derive(Clone, Debug, Deserialize)]
pub struct StopBotAction {
    pub bot_name: String,
    #[serde(default)]
    pub skip_order_cancellation: bool,
    #[serde(default)]
    pub async_backend: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct DeployResponse {
    pub success: bool,
    pub message: String,
    pub bot_name: String,
    pub unique_instance_name: String,
    pub script_config_generated: String,
    pub controllers_deployed: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub status: &'static str,
    pub data: T,
}

#[derive(Clone, Debug, Serialize, sqlx::FromRow)]
pub struct BotRunRow {
    pub id: i32,
    pub bot_name: String,
    pub instance_name: String,
    pub deployed_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub strategy_type: String,
    pub strategy_name: String,
    pub config_name: Option<String>,
    pub account_name: String,
    pub image_version: Option<String>,
    pub deployment_status: String,
    pub run_status: String,
    pub deployment_config: Option<String>,
    pub final_status: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct DeploymentFiles {
    pub script_config_name: String,
    pub controllers: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct StatusEvent {
    pub bot_name: String,
    pub kind: Option<String>,
    pub msg: Option<String>,
    pub payload: Value,
}
