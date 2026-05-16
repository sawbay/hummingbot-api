mod config;
mod db;
mod docker;
mod error;
mod fs_ops;
mod mqtt;
mod orchestrator;
mod r2;
mod routes;
mod slot_store;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::Router;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Settings;
use crate::db::Db;
use crate::docker::DockerClient;
use crate::mqtt::MqttBus;
use crate::orchestrator::Orchestrator;
use crate::r2::R2Client;
use crate::slot_store::SlotStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let settings = Settings::load().context("load settings")?;
    let db = Db::connect(&settings.database_url)
        .await
        .context("connect database")?
        .with_slot_names(settings.pool_bots.clone());
    let slots = SlotStore::new(settings.pool_bots.clone());
    slots.rebuild_from_db(&db).await;

    let docker = DockerClient::new();
    let r2 = R2Client::from_settings(&settings)
        .await
        .context("initialize r2")?;
    let mqtt = Arc::new(
        MqttBus::connect(settings.clone(), slots.clone())
            .await
            .context("connect mqtt")?,
    );

    let orchestrator = Arc::new(Orchestrator::new(
        settings.clone(),
        db,
        slots,
        mqtt,
        docker,
        r2,
    ));

    let app: Router = routes::router(orchestrator).layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], settings.port));
    let listener = TcpListener::bind(addr).await?;
    tracing::info!("rs-orchestrator listening on {addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}
