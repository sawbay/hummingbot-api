use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures_util::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

use crate::config::Settings;

#[derive(Clone)]
pub struct R2Client {
    enabled: bool,
    prefix: String,
    bots_path: PathBuf,
    store: Option<Arc<dyn ObjectStore>>,
}

impl R2Client {
    pub async fn from_settings(settings: &Settings) -> anyhow::Result<Self> {
        if !settings.r2_enabled {
            return Ok(Self {
                enabled: false,
                prefix: settings.r2_prefix.trim_matches('/').to_string(),
                bots_path: settings.bots_path.clone(),
                store: None,
            });
        }

        if settings.r2_bucket.is_empty() || settings.r2_endpoint_url.is_empty() {
            anyhow::bail!("R2 is enabled but R2_BUCKET or R2_ENDPOINT_URL is empty");
        }

        let store = AmazonS3Builder::new()
            .with_bucket_name(&settings.r2_bucket)
            .with_endpoint(&settings.r2_endpoint_url)
            .with_access_key_id(&settings.r2_access_key_id)
            .with_secret_access_key(&settings.r2_secret_access_key)
            .with_region("auto")
            .with_virtual_hosted_style_request(false)
            .build()?;

        Ok(Self {
            enabled: true,
            prefix: settings.r2_prefix.trim_matches('/').to_string(),
            bots_path: settings.bots_path.clone(),
            store: Some(Arc::new(store)),
        })
    }

    pub async fn hydrate_keys(&self, keys: &[String]) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        for key in keys {
            if key.ends_with('/') {
                self.download_prefix(key).await?;
            } else {
                self.download_key(key).await?;
            }
        }
        Ok(())
    }

    async fn download_prefix(&self, prefix: &str) -> anyhow::Result<()> {
        let store = self.store.as_ref().expect("R2 store missing");
        let object_prefix = ObjectPath::from(prefix.trim_end_matches('/'));
        let mut stream = store.list(Some(&object_prefix));
        while let Some(meta) = stream.try_next().await? {
            self.download_key(meta.location.as_ref()).await?;
        }
        Ok(())
    }

    async fn download_key(&self, key: &str) -> anyhow::Result<()> {
        let store = self.store.as_ref().expect("R2 store missing");
        let destination = self.local_path_for_key(key)?;
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let bytes = store.get(&ObjectPath::from(key)).await?.bytes().await?;
        tokio::fs::write(&destination, bytes.as_ref()).await?;
        Ok(())
    }

    fn local_path_for_key(&self, key: &str) -> anyhow::Result<PathBuf> {
        let relative = self.relative_key(key)?;
        Ok(self.bots_path.join("bots").join(relative))
    }

    fn relative_key<'a>(&self, key: &'a str) -> anyhow::Result<&'a str> {
        let clean_prefix = self.prefix.trim_matches('/');
        let relative = if clean_prefix.is_empty() {
            key
        } else {
            key.strip_prefix(&format!("{clean_prefix}/"))
                .ok_or_else(|| {
                    anyhow::anyhow!("R2 key {key} does not start with prefix {clean_prefix}/")
                })?
        };

        if relative.contains("..") || Path::new(relative).is_absolute() {
            anyhow::bail!("Unsafe R2 key path: {key}");
        }
        Ok(relative.trim_end_matches('/'))
    }
}
