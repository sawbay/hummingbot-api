use std::path::{Path, PathBuf};

use serde_json::json;
use tokio::fs;

use crate::error::{AppError, AppResult};
use crate::types::{DeploymentFiles, V2ControllerDeployment};

pub async fn prepare_controller_deployment(
    bots_path: &Path,
    bot_name: &str,
    script_config_name: &str,
    deployment: &V2ControllerDeployment,
) -> AppResult<DeploymentFiles> {
    validate_profile(bots_path, &deployment.credentials_profile).await?;
    validate_pool_slot(bots_path, bot_name).await?;
    sync_credentials_profile_to_pool(bots_path, bot_name, &deployment.credentials_profile).await?;
    let controllers = normalize_controllers(&deployment.controllers_config);
    validate_controllers(bots_path, &controllers).await?;

    let source_scripts_dir = bots_path.join("bots/conf/scripts");
    let pool_scripts_dir = bots_path
        .join("bots/pools")
        .join(bot_name)
        .join("conf/scripts");
    let pool_controllers_dir = bots_path
        .join("bots/pools")
        .join(bot_name)
        .join("conf/controllers");

    fs::create_dir_all(&source_scripts_dir)
        .await
        .map_err(to_internal)?;
    fs::create_dir_all(&pool_scripts_dir)
        .await
        .map_err(to_internal)?;
    fs::create_dir_all(&pool_controllers_dir)
        .await
        .map_err(to_internal)?;

    let mut script_config = json!({
        "script_file_name": "v2_with_controllers.py",
        "controllers_config": controllers,
    });
    if let Some(value) = deployment.max_global_drawdown_quote {
        script_config["max_global_drawdown_quote"] = json!(value);
    }
    if let Some(value) = deployment.max_controller_drawdown_quote {
        script_config["max_controller_drawdown_quote"] = json!(value);
    }

    let yaml = serde_yaml::to_string(&script_config).map_err(to_internal)?;
    let source_script = source_scripts_dir.join(script_config_name);
    let pool_script = pool_scripts_dir.join(script_config_name);
    fs::write(&source_script, yaml.as_bytes())
        .await
        .map_err(to_internal)?;
    fs::write(&pool_script, yaml.as_bytes())
        .await
        .map_err(to_internal)?;

    for controller in &controllers {
        let source = bots_path.join("bots/conf/controllers").join(controller);
        let dest = pool_controllers_dir.join(controller);
        fs::copy(source, dest).await.map_err(to_internal)?;
    }

    Ok(DeploymentFiles {
        script_config_name: script_config_name.to_string(),
        controllers,
    })
}

pub async fn prepare_existing_deployment(
    bots_path: &Path,
    bot_name: &str,
    script_config_name: Option<&str>,
    controllers: &[String],
    credentials_profile: &str,
) -> AppResult<DeploymentFiles> {
    validate_profile(bots_path, credentials_profile).await?;
    validate_pool_slot(bots_path, bot_name).await?;
    sync_credentials_profile_to_pool(bots_path, bot_name, credentials_profile).await?;
    let controllers = normalize_controllers(controllers);
    validate_controllers(bots_path, &controllers).await?;

    let pool_scripts_dir = bots_path
        .join("bots/pools")
        .join(bot_name)
        .join("conf/scripts");
    let pool_controllers_dir = bots_path
        .join("bots/pools")
        .join(bot_name)
        .join("conf/controllers");

    fs::create_dir_all(&pool_scripts_dir)
        .await
        .map_err(to_internal)?;
    fs::create_dir_all(&pool_controllers_dir)
        .await
        .map_err(to_internal)?;

    if let Some(script_config_name) = script_config_name {
        let source_script = bots_path.join("bots/conf/scripts").join(script_config_name);
        if !fs::metadata(&source_script)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            return Err(AppError::BadRequest(format!(
                "Script config '{script_config_name}' not found at {}",
                source_script.display()
            )));
        }

        fs::copy(&source_script, pool_scripts_dir.join(script_config_name))
            .await
            .map_err(to_internal)?;
    }

    for controller in &controllers {
        let source = bots_path.join("bots/conf/controllers").join(controller);
        let dest = pool_controllers_dir.join(controller);
        fs::copy(source, dest).await.map_err(to_internal)?;
    }

    Ok(DeploymentFiles {
        script_config_name: script_config_name.unwrap_or_default().to_string(),
        controllers,
    })
}

pub async fn cleanup_assignment(
    bots_path: &Path,
    bot_name: &str,
    config_name: &str,
    controllers: &[String],
) -> AppResult<()> {
    let pool_conf = bots_path.join("bots/pools").join(bot_name).join("conf");
    let scripts_dir = pool_conf.join("scripts");
    let controllers_dir = pool_conf.join("controllers");

    ensure_inside(&scripts_dir.join(config_name), &scripts_dir)?;
    let _ = fs::remove_file(scripts_dir.join(config_name)).await;

    for controller in controllers {
        let candidate = controllers_dir.join(controller);
        ensure_inside(&candidate, &controllers_dir)?;
        let _ = fs::remove_file(candidate).await;
    }

    Ok(())
}

fn normalize_controllers(controllers: &[String]) -> Vec<String> {
    controllers
        .iter()
        .map(|name| {
            if name.ends_with(".yml") {
                name.clone()
            } else {
                format!("{name}.yml")
            }
        })
        .collect()
}

async fn validate_profile(bots_path: &Path, profile: &str) -> AppResult<()> {
    let path = bots_path.join("bots/credentials").join(profile);
    if fs::metadata(&path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "Credentials profile '{profile}' not found at {}",
            path.display()
        )))
    }
}

async fn sync_credentials_profile_to_pool(
    bots_path: &Path,
    bot_name: &str,
    profile: &str,
) -> AppResult<()> {
    let source = bots_path.join("bots/credentials").join(profile);
    let destination = bots_path.join("bots/pools").join(bot_name).join("conf");
    let bot_name = bot_name.to_string();

    tokio::task::spawn_blocking(move || -> AppResult<()> {
        std::fs::create_dir_all(&destination).map_err(to_internal)?;

        for entry in std::fs::read_dir(&source).map_err(to_internal)? {
            let entry = entry.map_err(to_internal)?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if name_str == "scripts" || name_str == "controllers" {
                continue;
            }

            let source_path = entry.path();
            let destination_path = destination.join(&name);
            if source_path.is_dir() {
                if destination_path.exists() {
                    std::fs::remove_dir_all(&destination_path).map_err(to_internal)?;
                }
                copy_dir_recursive(&source_path, &destination_path)?;
            } else if source_path.is_file() {
                std::fs::copy(&source_path, &destination_path).map_err(to_internal)?;
            }
        }

        let conf_client = destination.join("conf_client.yml");
        if conf_client.exists() {
            let content = std::fs::read_to_string(&conf_client).map_err(to_internal)?;
            let mut yaml: serde_yaml::Value =
                serde_yaml::from_str(&content).map_err(to_internal)?;
            if let serde_yaml::Value::Mapping(mapping) = &mut yaml {
                mapping.insert(
                    serde_yaml::Value::String("instance_id".to_string()),
                    serde_yaml::Value::String(bot_name),
                );
            }
            let rendered = serde_yaml::to_string(&yaml).map_err(to_internal)?;
            std::fs::write(conf_client, rendered).map_err(to_internal)?;
        }

        Ok(())
    })
    .await
    .map_err(to_internal)??;

    Ok(())
}

fn copy_dir_recursive(source: &Path, destination: &Path) -> AppResult<()> {
    std::fs::create_dir_all(destination).map_err(to_internal)?;
    for entry in std::fs::read_dir(source).map_err(to_internal)? {
        let entry = entry.map_err(to_internal)?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if source_path.is_file() {
            std::fs::copy(&source_path, &destination_path).map_err(to_internal)?;
        }
    }
    Ok(())
}

async fn validate_controllers(bots_path: &Path, controllers: &[String]) -> AppResult<()> {
    for controller in controllers {
        let path = bots_path.join("bots/conf/controllers").join(controller);
        if !fs::metadata(&path)
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
        {
            return Err(AppError::BadRequest(format!(
                "Controller config '{controller}' not found at {}",
                path.display()
            )));
        }
    }
    Ok(())
}

async fn validate_pool_slot(bots_path: &Path, bot_name: &str) -> AppResult<()> {
    let path = bots_path.join("bots/pools").join(bot_name).join("conf");
    if fs::metadata(&path)
        .await
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        Ok(())
    } else {
        Err(AppError::BadRequest(format!(
            "Pool bot '{bot_name}' conf directory not found at {}",
            path.display()
        )))
    }
}

fn ensure_inside(path: &Path, root: &Path) -> AppResult<()> {
    let parent = path.parent().unwrap_or(Path::new(""));
    if parent == root {
        Ok(())
    } else {
        Err(AppError::BadRequest(
            "cleanup path escapes assignment directory".to_string(),
        ))
    }
}

fn to_internal<E: std::error::Error + Send + Sync + 'static>(err: E) -> AppError {
    AppError::Internal(anyhow::Error::new(err))
}

#[allow(dead_code)]
fn _pathbuf(_: PathBuf) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_controller_extensions() {
        let controllers = normalize_controllers(&["a".to_string(), "b.yml".to_string()]);
        assert_eq!(controllers, vec!["a.yml".to_string(), "b.yml".to_string()]);
    }

    #[tokio::test]
    async fn cleanup_removes_only_assignment_files() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let conf = root.join("bots/pools/warmbot_1/conf");
        fs::create_dir_all(conf.join("scripts")).await.unwrap();
        fs::create_dir_all(conf.join("controllers")).await.unwrap();
        fs::write(conf.join("conf_client.yml"), "baseline")
            .await
            .unwrap();
        fs::write(conf.join("scripts/run.yml"), "run")
            .await
            .unwrap();
        fs::write(conf.join("controllers/controller.yml"), "controller")
            .await
            .unwrap();

        cleanup_assignment(
            root,
            "warmbot_1",
            "run.yml",
            &["controller.yml".to_string()],
        )
        .await
        .unwrap();

        assert!(fs::metadata(conf.join("conf_client.yml")).await.is_ok());
        assert!(fs::metadata(conf.join("scripts/run.yml")).await.is_err());
        assert!(fs::metadata(conf.join("controllers/controller.yml"))
            .await
            .is_err());
    }
}
