use bollard::container::{InspectContainerOptions, LogsOptions};
use bollard::Docker;
use futures_util::StreamExt;
use serde::Serialize;

#[derive(Clone)]
pub struct DockerClient {
    docker: Option<Docker>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ContainerHealth {
    pub found: bool,
    pub status: String,
    pub running: bool,
    pub exit_code: Option<i64>,
    pub error: Option<String>,
    pub logs: Option<String>,
}

impl DockerClient {
    pub fn new() -> Self {
        let docker = Docker::connect_with_local_defaults().ok();
        Self { docker }
    }

    pub async fn health(&self, container_name: &str, include_logs: bool) -> ContainerHealth {
        let Some(docker) = &self.docker else {
            return ContainerHealth {
                found: false,
                status: "docker_unavailable".to_string(),
                running: false,
                exit_code: None,
                error: Some("Docker client unavailable".to_string()),
                logs: None,
            };
        };

        match docker
            .inspect_container(container_name, None::<InspectContainerOptions>)
            .await
        {
            Ok(info) => {
                let state = info.state;
                let status = state
                    .as_ref()
                    .and_then(|s| s.status.as_ref())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let running = state.as_ref().and_then(|s| s.running).unwrap_or(false);
                let exit_code = state.as_ref().and_then(|s| s.exit_code);
                let error = if !running && !matches!(exit_code, None | Some(0)) {
                    Some(format!(
                        "Container exited with code {}",
                        exit_code.unwrap_or_default()
                    ))
                } else {
                    None
                };
                let logs = if include_logs || error.is_some() {
                    self.logs(container_name, 100).await.ok()
                } else {
                    None
                };
                ContainerHealth {
                    found: true,
                    status,
                    running,
                    exit_code,
                    error,
                    logs,
                }
            }
            Err(err) => ContainerHealth {
                found: false,
                status: "not_found".to_string(),
                running: false,
                exit_code: None,
                error: Some(err.to_string()),
                logs: None,
            },
        }
    }

    pub async fn logs(&self, container_name: &str, tail: usize) -> anyhow::Result<String> {
        let Some(docker) = &self.docker else {
            return Ok("[docker unavailable]".to_string());
        };

        let options = LogsOptions::<String> {
            stdout: true,
            stderr: true,
            timestamps: false,
            tail: tail.to_string(),
            ..Default::default()
        };

        let mut stream = docker.logs(container_name, Some(options));
        let mut output = String::new();
        while let Some(item) = stream.next().await {
            output.push_str(&item?.to_string());
        }
        Ok(output)
    }
}
