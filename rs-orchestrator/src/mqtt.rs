use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, Transport};
use serde_json::{json, Value};
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::{sleep, timeout, Instant};

use crate::config::Settings;
use crate::slot_store::SlotStore;
use crate::types::SlotStatus;

pub const ORCHESTRATE_DEPLOY_TOPIC: &str = "orchestrate/deploy";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StrategyContentState {
    Running,
    Stopped,
    Unknown,
}

#[derive(Clone)]
pub struct MqttBus {
    client: AsyncClient,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    orchestrate_tx: broadcast::Sender<Value>,
    logs: Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
}

impl MqttBus {
    pub async fn connect(settings: Settings, slots: SlotStore) -> anyhow::Result<Self> {
        let mut options = MqttOptions::new(
            format!("rs-orchestrator-{}", millis()),
            settings.broker_host.clone(),
            settings.broker_port,
        );
        options.set_keep_alive(Duration::from_secs(30));
        if let (Some(username), Some(password)) =
            (&settings.broker_username, &settings.broker_password)
        {
            options.set_credentials(username, password);
        }
        if settings.broker_ssl {
            options.set_transport(Transport::tls_with_default_config());
        }

        let (client, event_loop) = AsyncClient::new(options, 100);
        let (orchestrate_tx, _) = broadcast::channel(256);
        let bus = Self {
            client,
            pending: Arc::new(Mutex::new(HashMap::new())),
            orchestrate_tx,
            logs: Arc::new(Mutex::new(HashMap::new())),
        };

        bus.subscribe_defaults().await?;
        bus.spawn_event_loop(event_loop, slots);
        Ok(bus)
    }

    pub async fn subscribe_defaults(&self) -> anyhow::Result<()> {
        for topic in [
            "hbot/+/hb",
            "hbot/+/status_updates",
            "hbot/+/log",
            "hbot/+/notify",
            "hbot/+/performance",
            "hummingbot-api/response/+",
            ORCHESTRATE_DEPLOY_TOPIC,
        ] {
            self.client.subscribe(topic, QoS::AtLeastOnce).await?;
        }
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        true
    }

    pub fn subscribe_orchestrate(&self) -> broadcast::Receiver<Value> {
        self.orchestrate_tx.subscribe()
    }

    pub async fn recent_logs(&self, bot_name: &str) -> Vec<Value> {
        self.logs
            .lock()
            .await
            .get(bot_name)
            .map(|logs| logs.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub async fn clear_logs(&self, bot_name: &str) {
        self.logs.lock().await.remove(bot_name);
    }

    pub async fn publish_command(
        &self,
        bot_name: &str,
        command: &str,
        data: Value,
    ) -> anyhow::Result<()> {
        let topic = format!("hbot/{}/{}", bot_name.replace('.', "/"), command);
        let request_id = format!("{}-{}", command, millis());
        let reply_to = format!("hummingbot-api/response/{request_id}");
        tracing::info!(
            topic = %topic,
            reply_to = %reply_to,
            payload = %data,
            "publishing MQTT command"
        );
        let message = json!({
            "header": {
                "timestamp": millis(),
                "reply_to": reply_to,
                "msg_id": millis(),
                "node_id": "rs-orchestrator",
                "agent": "rs-orchestrator",
                "properties": {},
            },
            "data": data,
        });
        self.client
            .publish(
                topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&message)?,
            )
            .await?;
        Ok(())
    }

    pub async fn publish_raw(&self, topic: &str, data: Value) -> anyhow::Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, false, serde_json::to_vec(&data)?)
            .await?;
        Ok(())
    }

    pub async fn publish_command_and_wait(
        &self,
        bot_name: &str,
        command: &str,
        data: Value,
        wait: Duration,
    ) -> anyhow::Result<Option<Value>> {
        let request_id = format!("{}-{}", command, millis());
        let reply_to = format!("hummingbot-api/response/{request_id}");
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(reply_to.clone(), tx);

        let topic = format!("hbot/{}/{}", bot_name.replace('.', "/"), command);
        tracing::info!(
            topic = %topic,
            reply_to = %reply_to,
            payload = %data,
            "publishing MQTT command and waiting for response"
        );
        let message = json!({
            "header": {
                "timestamp": millis(),
                "reply_to": reply_to,
                "msg_id": millis(),
                "node_id": "rs-orchestrator",
                "agent": "rs-orchestrator",
                "properties": {},
            },
            "data": data,
        });

        self.client
            .publish(
                topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&message)?,
            )
            .await?;

        match timeout(wait, rx).await {
            Ok(Ok(value)) => Ok(Some(value)),
            Ok(Err(_)) => Ok(None),
            Err(_) => {
                self.pending
                    .lock()
                    .await
                    .remove(&format!("hummingbot-api/response/{request_id}"));
                Ok(None)
            }
        }
    }

    pub async fn wait_for_strategy_status_content(
        &self,
        bot_name: &str,
        expected: StrategyContentState,
        wait: Duration,
    ) -> anyhow::Result<Option<Value>> {
        let deadline = Instant::now() + wait;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }

            let response = self
                .publish_command_and_wait(
                    bot_name,
                    "status",
                    json!({"async_backend": false}),
                    remaining.min(Duration::from_secs(5)),
                )
                .await?;

            if let Some(response) = response {
                let state = classify_strategy_status_response(&response);
                tracing::debug!(
                    bot_name = %bot_name,
                    expected = ?expected,
                    observed = ?state,
                    payload = %response,
                    "polled strategy status content"
                );
                if state == expected {
                    return Ok(Some(response));
                }
            }

            sleep(remaining.min(Duration::from_secs(1))).await;
        }
    }

    fn spawn_event_loop(&self, mut event_loop: EventLoop, slots: SlotStore) {
        let pending = self.pending.clone();
        let orchestrate_tx = self.orchestrate_tx.clone();
        let logs = self.logs.clone();

        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(packet))) => {
                        handle_publish(
                            packet.topic,
                            packet.payload.to_vec(),
                            &pending,
                            &orchestrate_tx,
                            &logs,
                            &slots,
                        )
                        .await;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        tracing::warn!("mqtt event loop error: {err}");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }
}

async fn handle_publish(
    topic: String,
    payload: Vec<u8>,
    pending: &Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    orchestrate_tx: &broadcast::Sender<Value>,
    logs: &Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
    slots: &SlotStore,
) {
    let value = parse_payload(&payload);

    if topic == ORCHESTRATE_DEPLOY_TOPIC {
        tracing::info!(topic = %topic, payload = %value, "received orchestration request");
        let _ = orchestrate_tx.send(value);
        return;
    }

    if topic.starts_with("hummingbot-api/response/") {
        if let Some(tx) = pending.lock().await.remove(&topic) {
            let _ = tx.send(value);
        } else {
            tracing::info!(topic = %topic, payload = %value, "received MQTT command response");
        }
        return;
    }

    let parts: Vec<_> = topic.split('/').collect();
    if parts.len() < 3 || parts[0] != "hbot" {
        return;
    }

    let bot_name = parts[1].to_string();
    let channel = parts[2..].join("/");

    match channel.as_str() {
        "hb" => {
            slots.mark_heartbeat(&bot_name).await;
        }
        "status_updates" => {
            let kind = value
                .get("type")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let msg = value
                .get("msg")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            if kind.as_deref() == Some("bootstrap") && msg.as_deref() == Some("bootstrapping") {
                slots
                    .mark_status(&bot_name, SlotStatus::Bootstrapping)
                    .await;
            } else if kind.as_deref() == Some("strategy") && msg.as_deref() == Some("idle") {
                slots.mark_status(&bot_name, SlotStatus::Idle).await;
            } else if kind.as_deref() == Some("strategy") && msg.as_deref() == Some("running") {
                slots.mark_status(&bot_name, SlotStatus::Running).await;
            } else if kind.as_deref() == Some("strategy") && msg.as_deref() == Some("failed") {
                slots.mark_error(&bot_name, value.to_string()).await;
            }
        }
        "log" => {
            let mut guard = logs.lock().await;
            let bot_logs = guard
                .entry(bot_name)
                .or_insert_with(|| VecDeque::with_capacity(100));
            if bot_logs.len() >= 100 {
                bot_logs.pop_front();
            }
            bot_logs.push_back(value);
        }
        _ => {}
    }
}

fn parse_payload(payload: &[u8]) -> Value {
    serde_json::from_slice(payload)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(payload).to_string()))
}

fn classify_strategy_status_response(value: &Value) -> StrategyContentState {
    let status = response_field(value, "status").and_then(Value::as_i64);
    let msg = response_field(value, "msg")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let normalized_msg = msg.to_ascii_lowercase();

    if normalized_msg.contains("no strategy is currently running") {
        return StrategyContentState::Stopped;
    }

    if status == Some(200) && !msg.is_empty() {
        return StrategyContentState::Running;
    }

    StrategyContentState::Unknown
}

fn response_field<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value
        .get(field)
        .or_else(|| value.get("data").and_then(|data| data.get(field)))
}

fn millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{classify_strategy_status_response, parse_payload, StrategyContentState};

    #[test]
    fn parses_plain_string_payload() {
        assert_eq!(parse_payload(b"hello").as_str(), Some("hello"));
    }

    #[test]
    fn classifies_status_content_as_running() {
        let response = json!({
            "status": 200,
            "msg": "Markets:\n  exchange: binance",
            "data": ""
        });

        assert_eq!(
            classify_strategy_status_response(&response),
            StrategyContentState::Running
        );
    }

    #[test]
    fn classifies_status_content_as_stopped() {
        let response = json!({
            "status": 400,
            "msg": "No strategy is currently running!",
            "data": ""
        });

        assert_eq!(
            classify_strategy_status_response(&response),
            StrategyContentState::Stopped
        );
    }

    #[test]
    fn classifies_nested_status_content() {
        let response = json!({
            "data": {
                "status": 200,
                "msg": "Strategy status"
            }
        });

        assert_eq!(
            classify_strategy_status_response(&response),
            StrategyContentState::Running
        );
    }
}
