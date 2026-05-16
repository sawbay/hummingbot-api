use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Mutex;

use crate::types::{SlotState, SlotStatus};

#[derive(Clone)]
pub struct SlotStore {
    inner: Arc<Mutex<HashMap<String, SlotState>>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserves_only_one_idle_slot() {
        let store = SlotStore::new(vec!["warmbot_1".to_string()]);
        store.mark_status("warmbot_1", SlotStatus::Idle).await;

        let first = store.reserve_idle().await;
        let second = store.reserve_idle().await;

        assert_eq!(first.unwrap().bot_name, "warmbot_1");
        assert!(second.is_none());
        assert_eq!(
            store.get("warmbot_1").await.unwrap().status,
            SlotStatus::Reserved
        );
    }
}

impl SlotStore {
    pub fn new(bot_names: Vec<String>) -> Self {
        let slots = bot_names
            .into_iter()
            .map(|name| {
                let slot = SlotState::new(name.clone());
                (name, slot)
            })
            .collect();
        Self {
            inner: Arc::new(Mutex::new(slots)),
        }
    }

    pub async fn list(&self) -> Vec<SlotState> {
        let guard = self.inner.lock().await;
        let mut slots: Vec<_> = guard.values().cloned().collect();
        slots.sort_by(|a, b| a.bot_name.cmp(&b.bot_name));
        slots
    }

    pub async fn get(&self, bot_name: &str) -> Option<SlotState> {
        self.inner.lock().await.get(bot_name).cloned()
    }

    pub async fn reserve_idle(&self) -> Option<SlotState> {
        let mut guard = self.inner.lock().await;
        let mut names: Vec<_> = guard.keys().cloned().collect();
        names.sort();

        for name in names {
            let slot = guard.get_mut(&name)?;
            if slot.status == SlotStatus::Idle {
                slot.status = SlotStatus::Reserved;
                slot.last_error = None;
                slot.updated_at = Utc::now();
                return Some(slot.clone());
            }
        }
        None
    }

    pub async fn mark_heartbeat(&self, bot_name: &str) {
        let mut guard = self.inner.lock().await;
        let slot = guard
            .entry(bot_name.to_string())
            .or_insert_with(|| SlotState::new(bot_name.to_string()));
        slot.last_heartbeat = Some(Utc::now());
        if matches!(slot.status, SlotStatus::Offline | SlotStatus::Bootstrapping) {
            slot.status = if slot.assigned_instance_name.is_some() {
                SlotStatus::Running
            } else {
                SlotStatus::Idle
            };
        }
        slot.updated_at = Utc::now();
    }

    pub async fn mark_status(&self, bot_name: &str, status: SlotStatus) {
        let mut guard = self.inner.lock().await;
        if let Some(slot) = guard.get_mut(bot_name) {
            slot.status = status;
            slot.updated_at = Utc::now();
        }
    }

    pub async fn mark_error(&self, bot_name: &str, error: String) {
        let mut guard = self.inner.lock().await;
        if let Some(slot) = guard.get_mut(bot_name) {
            slot.status = SlotStatus::Error;
            slot.last_error = Some(error);
            slot.updated_at = Utc::now();
        }
    }

    pub async fn assign_running(
        &self,
        bot_name: &str,
        instance_name: Option<String>,
        account_name: String,
        config_name: Option<String>,
        controllers: Vec<String>,
    ) {
        let mut guard = self.inner.lock().await;
        if let Some(slot) = guard.get_mut(bot_name) {
            slot.status = SlotStatus::Running;
            slot.assigned_instance_name = instance_name;
            slot.account_name = Some(account_name);
            slot.current_config_name = config_name;
            slot.current_controller_ids = controllers;
            slot.last_error = None;
            slot.updated_at = Utc::now();
        }
    }

    pub async fn assign_configuring(
        &self,
        bot_name: &str,
        instance_name: Option<String>,
        account_name: String,
        config_name: Option<String>,
        controllers: Vec<String>,
    ) {
        let mut guard = self.inner.lock().await;
        if let Some(slot) = guard.get_mut(bot_name) {
            slot.status = SlotStatus::Configuring;
            slot.assigned_instance_name = instance_name;
            slot.account_name = Some(account_name);
            slot.current_config_name = config_name;
            slot.current_controller_ids = controllers;
            slot.last_error = None;
            slot.updated_at = Utc::now();
        }
    }

    pub async fn release_idle(&self, bot_name: &str) {
        let mut guard = self.inner.lock().await;
        if let Some(slot) = guard.get_mut(bot_name) {
            slot.status = SlotStatus::Idle;
            slot.assigned_instance_name = None;
            slot.account_name = None;
            slot.current_config_name = None;
            slot.current_controller_ids.clear();
            slot.last_error = None;
            slot.updated_at = Utc::now();
        }
    }

    pub async fn mark_stale_offline(&self, timeout: std::time::Duration) {
        let mut guard = self.inner.lock().await;
        let now = Utc::now();
        for slot in guard.values_mut() {
            if matches!(
                slot.status,
                SlotStatus::Reserved | SlotStatus::Configuring | SlotStatus::Stopping
            ) {
                continue;
            }
            let stale = slot
                .last_heartbeat
                .map(|seen| now.signed_duration_since(seen).to_std().unwrap_or_default() > timeout)
                .unwrap_or(true);
            if stale {
                slot.status = SlotStatus::Offline;
                slot.updated_at = now;
            }
        }
    }

    pub async fn find_by_instance(&self, instance_name: &str) -> Option<SlotState> {
        let guard = self.inner.lock().await;
        guard
            .values()
            .find(|slot| slot.assigned_instance_name.as_deref() == Some(instance_name))
            .cloned()
    }
}
