use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

use crate::config::Settings;
use crate::docker::{ContainerHealth, DockerClient};
use crate::error::{AppError, AppResult};
use crate::fs_ops;
use crate::mqtt::MqttBus;
use crate::r2::R2Client;
use crate::slot_store::SlotStore;
use crate::types::{
    ApiResponse, DeployResponse, OrchestrationRequest, SlotState, SlotStatus, StopBotAction,
    V2ControllerDeployment,
};

const ORCHESTRATE_STATUS_TOPIC: &str = "orchestrate/status";

#[derive(Clone)]
pub struct Orchestrator {
    settings: Settings,
    slots: SlotStore,
    mqtt: Arc<MqttBus>,
    docker: DockerClient,
    r2: R2Client,
}

impl Orchestrator {
    pub fn new(
        settings: Settings,
        slots: SlotStore,
        mqtt: Arc<MqttBus>,
        docker: DockerClient,
        r2: R2Client,
    ) -> Self {
        let this = Self {
            settings,
            slots,
            mqtt,
            docker,
            r2,
        };
        this.spawn_heartbeat_reaper();
        this.spawn_orchestration_listener();
        this
    }

    pub async fn health(&self) -> Value {
        json!({
            "service": "rs-orchestrator",
            "status": "ok",
            "mqtt_connected": self.mqtt.is_connected(),
        })
    }

    pub async fn list_slots(&self) -> Vec<SlotState> {
        self.slots.list().await
    }

    pub async fn get_slot(&self, bot_name: &str) -> AppResult<SlotState> {
        self.slots
            .get(bot_name)
            .await
            .ok_or_else(|| AppError::NotFound(format!("Pool slot '{bot_name}' not found")))
    }

    pub async fn deploy_v2_controllers(
        &self,
        mut deployment: V2ControllerDeployment,
    ) -> AppResult<DeployResponse> {
        if deployment.controllers_config.is_empty() {
            return Err(AppError::BadRequest(
                "controllers_config must not be empty".to_string(),
            ));
        }

        let slot = self.slots.reserve_idle().await.ok_or_else(|| {
            AppError::Conflict("No idle warm-pool slots are available".to_string())
        })?;

        let result = self
            .deploy_into_reserved_slot(&slot.bot_name, &mut deployment)
            .await;
        if let Err(err) = &result {
            self.slots.mark_error(&slot.bot_name, err.to_string()).await;
        }
        result
    }

    async fn deploy_into_reserved_slot(
        &self,
        bot_name: &str,
        deployment: &mut V2ControllerDeployment,
    ) -> AppResult<DeployResponse> {
        let timestamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
        let unique_instance_name = format!("{}-{timestamp}", deployment.instance_name);
        let script_config_name = format!("{bot_name}-{timestamp}.yml");
        deployment.instance_name = unique_instance_name.clone();
        deployment.script_config = Some(script_config_name.clone());

        let files = fs_ops::prepare_controller_deployment(
            &self.settings.bots_path,
            bot_name,
            &script_config_name,
            deployment,
        )
        .await?;

        self.slots
            .assign_configuring(
                bot_name,
                Some(unique_instance_name.clone()),
                deployment.credentials_profile.clone(),
                Some(script_config_name.clone()),
                files.controllers.clone(),
            )
            .await;

        if let Err(err) = self
            .start_strategy(
                bot_name,
                "v2_with_controllers.py",
                Some(&script_config_name),
            )
            .await
        {
            let logs = self.failure_diagnostics(bot_name, err.to_string()).await;
            self.slots.mark_error(bot_name, logs.clone()).await;
            return Err(AppError::ServiceUnavailable(logs));
        }

        self.slots
            .assign_running(
                bot_name,
                Some(unique_instance_name.clone()),
                deployment.credentials_profile.clone(),
                Some(script_config_name.clone()),
                files.controllers.clone(),
            )
            .await;

        Ok(DeployResponse {
            success: true,
            message: format!("Assigned {unique_instance_name} to warm pool slot {bot_name}."),
            bot_name: bot_name.to_string(),
            unique_instance_name,
            script_config_generated: files.script_config_name,
            controllers_deployed: files.controllers,
        })
    }

    async fn start_strategy(
        &self,
        bot_name: &str,
        script_file_name: &str,
        script_config_name: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut start_payload = json!({
            "log_level": "INFO",
            "is_quickstart": true,
            "async_backend": true,
        });
        if let Some(script_config_name) = script_config_name {
            start_payload["v2_conf"] = json!(script_config_name);
        } else {
            start_payload["script"] = json!(script_file_name);
        }
        self.mqtt
            .publish_command(bot_name, "start", start_payload)
            .await?;

        match self
            .mqtt
            .wait_for_strategy_status(bot_name, "running", self.settings.command_timeout())
            .await?
        {
            Some(event) if event.msg.as_deref() == Some("running") => Ok(()),
            Some(event) => anyhow::bail!("strategy failed while starting: {}", event.payload),
            None => anyhow::bail!("timed out waiting for strategy running status"),
        }
    }

    async fn handle_orchestration_request(&self, request: OrchestrationRequest) {
        tracing::info!(
            request_id = %request.request_id,
            instance_name = %request.instance_name,
            strategy_type = %request.strategy_type,
            strategy_name = %request.strategy_name,
            "processing orchestration request"
        );
        if let Err(err) = self
            .deploy_from_orchestration_request(request.clone())
            .await
        {
            tracing::error!("orchestration request {} failed: {err}", request.request_id);
            let _ = self
                .publish_orchestration_status(&request, None, "failed", Some(err.to_string()))
                .await;
        }
    }

    async fn deploy_from_orchestration_request(
        &self,
        request: OrchestrationRequest,
    ) -> AppResult<()> {
        let slot = self.slots.reserve_idle().await.ok_or_else(|| {
            AppError::Conflict("No idle warm-pool slots are available".to_string())
        })?;
        tracing::info!(
            request_id = %request.request_id,
            instance_name = %request.instance_name,
            bot_name = %slot.bot_name,
            "reserved warm-pool slot for orchestration request"
        );
        self.publish_orchestration_status(&request, Some(&slot.bot_name), "reserved", None)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;

        self.publish_orchestration_status(&request, Some(&slot.bot_name), "hydrating", None)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;

        let keys = request.r2.keys.flatten();
        tracing::info!(
            request_id = %request.request_id,
            bot_name = %slot.bot_name,
            key_count = keys.len(),
            "hydrating orchestration files from R2/local storage"
        );
        if let Err(err) = self.r2.hydrate_keys(&keys).await {
            let error = err.to_string();
            self.slots.mark_error(&slot.bot_name, error.clone()).await;
            let _ = self
                .publish_orchestration_status(&request, Some(&slot.bot_name), "failed", Some(error))
                .await;
            return Ok(());
        }

        let files = match fs_ops::prepare_existing_deployment(
            &self.settings.bots_path,
            &slot.bot_name,
            request.script_config.as_deref(),
            &request.controllers_config,
            &request.credentials_profile,
        )
        .await
        {
            Ok(files) => files,
            Err(err) => {
                let error = err.to_string();
                self.slots.mark_error(&slot.bot_name, error.clone()).await;
                let _ = self
                    .publish_orchestration_status(
                        &request,
                        Some(&slot.bot_name),
                        "failed",
                        Some(error),
                    )
                    .await;
                return Ok(());
            }
        };

        self.slots
            .assign_configuring(
                &slot.bot_name,
                Some(request.instance_name.clone()),
                request.credentials_profile.clone(),
                request.script_config.clone(),
                files.controllers.clone(),
            )
            .await;
        self.publish_orchestration_status(&request, Some(&slot.bot_name), "configuring", None)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;
        tracing::info!(
            request_id = %request.request_id,
            bot_name = %slot.bot_name,
            script_config = ?request.script_config,
            controllers = ?request.controllers_config,
            "copied orchestration files into warm-pool slot"
        );

        let script_file_name = script_file_name_for_request(&request);
        tracing::info!(
            request_id = %request.request_id,
            bot_name = %slot.bot_name,
            script = %script_file_name,
            v2_conf = ?request.script_config,
            "starting strategy on warm-pool bot"
        );
        if let Err(err) = self
            .start_strategy(
                &slot.bot_name,
                &script_file_name,
                request.script_config.as_deref(),
            )
            .await
        {
            let error = err.to_string();
            self.slots.mark_error(&slot.bot_name, error.clone()).await;
            let _ = self
                .publish_orchestration_status(&request, Some(&slot.bot_name), "failed", Some(error))
                .await;
            return Ok(());
        }

        self.slots
            .assign_running(
                &slot.bot_name,
                Some(request.instance_name.clone()),
                request.credentials_profile.clone(),
                request.script_config.clone(),
                files.controllers,
            )
            .await;
        self.publish_orchestration_status(&request, Some(&slot.bot_name), "running", None)
            .await
            .map_err(|err| AppError::Internal(err.into()))?;
        tracing::info!(
            request_id = %request.request_id,
            instance_name = %request.instance_name,
            bot_name = %slot.bot_name,
            "orchestration request is running"
        );
        Ok(())
    }

    async fn publish_orchestration_status(
        &self,
        request: &OrchestrationRequest,
        bot_name: Option<&str>,
        status: &str,
        error: Option<String>,
    ) -> anyhow::Result<()> {
        self.mqtt
            .publish_raw(
                ORCHESTRATE_STATUS_TOPIC,
                json!({
                    "request_id": request.request_id,
                    "instance_name": request.instance_name,
                    "bot_name": bot_name,
                    "status": status,
                    "error": error,
                }),
            )
            .await
    }

    async fn failure_diagnostics(&self, bot_name: &str, error: String) -> String {
        let mqtt_logs = self.mqtt.recent_logs(bot_name).await;
        let docker_logs = self.docker.logs(bot_name, 100).await.unwrap_or_default();
        format!(
            "{error}\n\nMQTT logs:\n{}\n\nContainer logs:\n{}",
            serde_json::to_string_pretty(&mqtt_logs).unwrap_or_else(|_| "[]".to_string()),
            docker_logs
        )
    }

    pub async fn stop_bot(&self, action: StopBotAction) -> AppResult<Value> {
        let slot = self.get_slot(&action.bot_name).await?;
        let config_name = slot.current_config_name.clone();
        let controllers = slot.current_controller_ids.clone();

        self.slots
            .mark_status(&action.bot_name, SlotStatus::Stopping)
            .await;
        self.mqtt
            .publish_command(
                &action.bot_name,
                "stop",
                json!({
                    "skip_order_cancellation": action.skip_order_cancellation,
                    "async_backend": action.async_backend,
                }),
            )
            .await
            .map_err(|err| AppError::ServiceUnavailable(err.to_string()))?;

        let stopped = self
            .mqtt
            .wait_for_strategy_status(&action.bot_name, "stopped", self.settings.command_timeout())
            .await
            .map_err(|err| AppError::ServiceUnavailable(err.to_string()))?;

        if stopped.is_none() {
            tracing::warn!(
                "timed out waiting for stopped status from {}",
                action.bot_name
            );
        }

        if let Some(event) = stopped {
            tracing::info!(
                bot_name = %action.bot_name,
                payload = %event.payload,
                "received stopped status"
            );
        }

        self.slots
            .mark_status(&action.bot_name, SlotStatus::Cleanup)
            .await;
        if let Some(config_name) = config_name {
            fs_ops::cleanup_assignment(
                &self.settings.bots_path,
                &action.bot_name,
                &config_name,
                &controllers,
            )
            .await?;
        }
        self.mqtt.clear_logs(&action.bot_name).await;
        self.slots.release_idle(&action.bot_name).await;

        Ok(json!({
            "success": true,
            "bot_name": action.bot_name,
            "status": "idle",
        }))
    }

    pub async fn deployment_status(&self, instance_name: &str) -> AppResult<Value> {
        let Some(slot) = self.slots.find_by_instance(instance_name).await else {
            return Err(AppError::NotFound(format!(
                "Deployment '{instance_name}' not found"
            )));
        };

        let container = self
            .docker
            .health(&slot.bot_name, matches!(slot.status, SlotStatus::Error))
            .await;
        let overall_status = derive_overall_status(&slot, &container);

        Ok(json!({
            "instance_name": instance_name,
            "overall_status": overall_status,
            "orchestrator": {
                "bot_name": slot.bot_name,
                "slot": slot,
            },
            "container": container
        }))
    }

    fn spawn_heartbeat_reaper(&self) {
        let slots = self.slots.clone();
        let timeout = self.settings.heartbeat_timeout();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;
                slots.mark_stale_offline(timeout).await;
            }
        });
    }

    fn spawn_orchestration_listener(&self) {
        let mut rx = self.mqtt.subscribe_orchestrate();
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(payload) => match serde_json::from_value::<OrchestrationRequest>(payload) {
                        Ok(request) => {
                            let worker = this.clone();
                            tokio::spawn(async move {
                                worker.handle_orchestration_request(request).await;
                            });
                        }
                        Err(err) => tracing::warn!("invalid orchestration payload: {err}"),
                    },
                    Err(err) => tracing::warn!("orchestration subscription error: {err}"),
                }
            }
        });
    }
}

fn script_file_name_for_request(request: &OrchestrationRequest) -> String {
    if request.strategy_type == "controller" {
        "v2_with_controllers.py".to_string()
    } else if request.strategy_name.ends_with(".py") {
        request.strategy_name.clone()
    } else {
        format!("{}.py", request.strategy_name)
    }
}

fn derive_overall_status(slot: &SlotState, container: &ContainerHealth) -> &'static str {
    if matches!(slot.status, SlotStatus::Error) {
        "failed"
    } else if matches!(slot.status, SlotStatus::Running) {
        "running"
    } else if !container.running && container.exit_code.is_some_and(|code| code != 0) {
        "failed"
    } else {
        "deploying"
    }
}

#[allow(dead_code)]
fn _api_response<T: serde::Serialize>(data: T) -> ApiResponse<T> {
    ApiResponse {
        status: "success",
        data,
    }
}
