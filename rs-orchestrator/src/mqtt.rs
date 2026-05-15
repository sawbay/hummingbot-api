use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, QoS, Transport};
use serde_json::{json, Value};
use tokio::sync::{broadcast, oneshot, Mutex};
use tokio::time::timeout;

use crate::config::Settings;
use crate::slot_store::SlotStore;
use crate::types::{SlotStatus, StatusEvent};

#[derive(Clone)]
pub struct MqttBus {
    client: AsyncClient,
    pending: Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>,
    status_tx: broadcast::Sender<StatusEvent>,
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
        let (status_tx, _) = broadcast::channel(256);
        let bus = Self {
            client,
            pending: Arc::new(Mutex::new(HashMap::new())),
            status_tx,
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
        ] {
            self.client.subscribe(topic, QoS::AtLeastOnce).await?;
        }
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        true
    }

    pub fn subscribe_status(&self) -> broadcast::Receiver<StatusEvent> {
        self.status_tx.subscribe()
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
        let message = json!({
            "header": {
                "timestamp": millis(),
                "reply_to": format!("hummingbot-api-response-{}", millis()),
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

    pub async fn wait_for_strategy_status(
        &self,
        bot_name: &str,
        expected: &str,
        wait: Duration,
    ) -> anyhow::Result<Option<StatusEvent>> {
        let mut rx = self.subscribe_status();
        let bot_name = bot_name.to_string();
        let expected = expected.to_string();
        let fut = async move {
            loop {
                let event = rx.recv().await.context("status channel closed")?;
                if event.bot_name == bot_name
                    && event.kind.as_deref() == Some("strategy")
                    && event.msg.as_deref() == Some(expected.as_str())
                {
                    return Ok::<_, anyhow::Error>(event);
                }
                if event.bot_name == bot_name
                    && event.kind.as_deref() == Some("strategy")
                    && event.msg.as_deref() == Some("failed")
                {
                    return Ok(event);
                }
            }
        };
        match timeout(wait, fut).await {
            Ok(result) => result.map(Some),
            Err(_) => Ok(None),
        }
    }

    fn spawn_event_loop(&self, mut event_loop: EventLoop, slots: SlotStore) {
        let pending = self.pending.clone();
        let status_tx = self.status_tx.clone();
        let logs = self.logs.clone();

        tokio::spawn(async move {
            loop {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(packet))) => {
                        handle_publish(
                            packet.topic,
                            packet.payload.to_vec(),
                            &pending,
                            &status_tx,
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
    status_tx: &broadcast::Sender<StatusEvent>,
    logs: &Arc<Mutex<HashMap<String, VecDeque<Value>>>>,
    slots: &SlotStore,
) {
    let value = parse_payload(&payload);

    if topic.starts_with("hummingbot-api/response/") {
        if let Some(tx) = pending.lock().await.remove(&topic) {
            let _ = tx.send(value);
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
            let _ = status_tx.send(StatusEvent {
                bot_name,
                kind,
                msg,
                payload: value,
            });
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

fn millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::parse_payload;

    #[test]
    fn parses_plain_string_payload() {
        assert_eq!(parse_payload(b"hello").as_str(), Some("hello"));
    }
}
