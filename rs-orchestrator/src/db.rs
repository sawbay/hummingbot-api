use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::types::{BotRunRow, V2ControllerDeployment};

#[derive(Clone)]
pub struct Db {
    pool: PgPool,
    slot_names: Vec<String>,
}

impl Db {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await?;
        Ok(Self {
            pool,
            slot_names: vec![
                "warmbot_1".to_string(),
                "warmbot_2".to_string(),
                "warmbot_3".to_string(),
            ],
        })
    }

    pub fn with_slot_names(mut self, slot_names: Vec<String>) -> Self {
        self.slot_names = slot_names;
        self
    }

    pub async fn create_bot_run(
        &self,
        bot_name: &str,
        instance_name: &str,
        config_name: &str,
        deployment: &V2ControllerDeployment,
    ) -> anyhow::Result<BotRunRow> {
        let deployment_config = serde_json::to_string(deployment)?;
        let row = sqlx::query_as::<_, BotRunRow>(
            r#"
            INSERT INTO bot_runs (
                bot_name, instance_name, strategy_type, strategy_name, config_name,
                account_name, image_version, deployment_config, deployment_status, run_status
            )
            VALUES ($1, $2, 'controller', 'v2_with_controllers', $3, $4, $5, $6, 'DEPLOYED', 'CREATED')
            RETURNING id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                      config_name, account_name, image_version, deployment_status, run_status,
                      deployment_config, final_status, error_message
            "#,
        )
        .bind(bot_name)
        .bind(instance_name)
        .bind(config_name)
        .bind(&deployment.credentials_profile)
        .bind(&deployment.image)
        .bind(deployment_config)
        .fetch_one(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn mark_running(&self, bot_name: &str) -> anyhow::Result<Option<BotRunRow>> {
        self.update_latest_status(bot_name, "RUNNING", "DEPLOYED", None, false)
            .await
    }

    pub async fn mark_failed(
        &self,
        bot_name: &str,
        error_message: &str,
    ) -> anyhow::Result<Option<BotRunRow>> {
        self.update_latest_status(bot_name, "ERROR", "FAILED", Some(error_message), true)
            .await
    }

    pub async fn mark_stopped(
        &self,
        bot_name: &str,
        final_status: Option<Value>,
    ) -> anyhow::Result<Option<BotRunRow>> {
        let final_status = final_status.map(|value| value.to_string());
        let row = sqlx::query_as::<_, BotRunRow>(
            r#"
            UPDATE bot_runs
            SET run_status = 'STOPPED',
                stopped_at = NOW(),
                final_status = COALESCE($2, final_status)
            WHERE id = (
                SELECT id FROM bot_runs
                WHERE bot_name = $1 AND run_status IN ('RUNNING', 'CREATED')
                ORDER BY deployed_at DESC
                LIMIT 1
            )
            RETURNING id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                      config_name, account_name, image_version, deployment_status, run_status,
                      deployment_config, final_status, error_message
            "#,
        )
        .bind(bot_name)
        .bind(final_status)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn update_latest_status(
        &self,
        bot_name: &str,
        run_status: &str,
        deployment_status: &str,
        error_message: Option<&str>,
        set_stopped: bool,
    ) -> anyhow::Result<Option<BotRunRow>> {
        let stopped_at_sql = if set_stopped { "NOW()" } else { "stopped_at" };
        let sql = format!(
            r#"
            UPDATE bot_runs
            SET run_status = $2,
                deployment_status = $3,
                error_message = COALESCE($4, error_message),
                stopped_at = {stopped_at_sql}
            WHERE id = (
                SELECT id FROM bot_runs
                WHERE bot_name = $1 AND run_status IN ('CREATED', 'RUNNING')
                ORDER BY deployed_at DESC
                LIMIT 1
            )
            RETURNING id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                      config_name, account_name, image_version, deployment_status, run_status,
                      deployment_config, final_status, error_message
            "#
        );
        let row = sqlx::query_as::<_, BotRunRow>(&sql)
            .bind(bot_name)
            .bind(run_status)
            .bind(deployment_status)
            .bind(error_message)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    pub async fn latest_by_instance(
        &self,
        instance_name: &str,
    ) -> anyhow::Result<Option<BotRunRow>> {
        let row = sqlx::query_as::<_, BotRunRow>(
            r#"
            SELECT id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                   config_name, account_name, image_version, deployment_status, run_status,
                   deployment_config, final_status, error_message
            FROM bot_runs
            WHERE instance_name = $1
            ORDER BY deployed_at DESC
            LIMIT 1
            "#,
        )
        .bind(instance_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn list_bot_runs(&self, limit: i64, offset: i64) -> anyhow::Result<Vec<BotRunRow>> {
        let rows = sqlx::query_as::<_, BotRunRow>(
            r#"
            SELECT id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                   config_name, account_name, image_version, deployment_status, run_status,
                   deployment_config, final_status, error_message
            FROM bot_runs
            ORDER BY deployed_at DESC
            LIMIT $1 OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn active_runs_for_slots(&self) -> anyhow::Result<Vec<BotRunRow>> {
        let rows = sqlx::query_as::<_, BotRunRow>(
            r#"
            SELECT DISTINCT ON (bot_name)
                   id, bot_name, instance_name, deployed_at, stopped_at, strategy_type, strategy_name,
                   config_name, account_name, image_version, deployment_status, run_status,
                   deployment_config, final_status, error_message
            FROM bot_runs
            WHERE bot_name = ANY($1)
              AND run_status = 'RUNNING'
              AND deployment_status = 'DEPLOYED'
            ORDER BY bot_name, deployed_at DESC
            "#,
        )
        .bind(&self.slot_names)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }
}

#[allow(dead_code)]
fn _assert_datetime_send_sync(_: DateTime<Utc>) {}
