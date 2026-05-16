use std::sync::Arc;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::error::AppResult;
use crate::orchestrator::Orchestrator;
use crate::types::{ApiResponse, StopBotAction, V2ControllerDeployment};

pub fn router(orchestrator: Arc<Orchestrator>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/bot-orchestration/pool/slots", get(list_slots))
        .route("/bot-orchestration/pool/slots/:bot_name", get(get_slot))
        .route(
            "/bot-orchestration/deploy-v2-controllers",
            post(deploy_v2_controllers),
        )
        .route("/bot-orchestration/stop-bot", post(stop_bot))
        .route(
            "/bot-orchestration/deployment-status/:instance_name",
            get(deployment_status),
        )
        .with_state(orchestrator)
}

async fn health(State(orchestrator): State<Arc<Orchestrator>>) -> Json<ApiResponse<Value>> {
    Json(ApiResponse {
        status: "success",
        data: orchestrator.health().await,
    })
}

async fn list_slots(State(orchestrator): State<Arc<Orchestrator>>) -> Json<ApiResponse<Value>> {
    Json(ApiResponse {
        status: "success",
        data: json!(orchestrator.list_slots().await),
    })
}

async fn get_slot(
    State(orchestrator): State<Arc<Orchestrator>>,
    Path(bot_name): Path<String>,
) -> AppResult<Json<ApiResponse<Value>>> {
    Ok(Json(ApiResponse {
        status: "success",
        data: json!(orchestrator.get_slot(&bot_name).await?),
    }))
}

async fn deploy_v2_controllers(
    State(orchestrator): State<Arc<Orchestrator>>,
    Json(deployment): Json<V2ControllerDeployment>,
) -> AppResult<Json<Value>> {
    let response = orchestrator.deploy_v2_controllers(deployment).await?;
    Ok(Json(json!(response)))
}

async fn stop_bot(
    State(orchestrator): State<Arc<Orchestrator>>,
    Json(action): Json<StopBotAction>,
) -> AppResult<Json<ApiResponse<Value>>> {
    Ok(Json(ApiResponse {
        status: "success",
        data: orchestrator.stop_bot(action).await?,
    }))
}

async fn deployment_status(
    State(orchestrator): State<Arc<Orchestrator>>,
    Path(instance_name): Path<String>,
) -> AppResult<Json<ApiResponse<Value>>> {
    Ok(Json(ApiResponse {
        status: "success",
        data: orchestrator.deployment_status(&instance_name).await?,
    }))
}
