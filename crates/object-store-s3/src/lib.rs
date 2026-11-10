//! Content-addressed, namespace-scoped objects on AWS S3, RustFS, or Cloudflare R2.
use async_trait::async_trait;
use loom_contracts::{Bytes, Error, ObjectRef, ObjectStore, Result};
use object_store::{
    ClientOptions, ObjectStore as Provider, ObjectStoreExt, PutMode, PutOptions, RetryConfig,
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

/// Endpoint and layout configuration. Credentials are supplied to AmazonS3Builder,
/// never persisted in object references or coordination records.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub prefix: String,
    pub force_path_style: bool,
    pub allow_http: bool,
}
impl S3Config {
    /// Compatibility constructor for AWS S3 (`false`) and RustFS (`true`).
    pub fn from_env(rustfs: bool) -> Result<Self> {
        Self::from_provider_env(if rustfs { "rustfs" } else { "s3" })
    }
    /// Load an S3-compatible provider preset: `s3`, `rustfs`, or `r2`.
    pub fn from_provider_env(provider: &str) -> Result<Self> {
        Self::from_values(provider, env)
    }
    fn from_values(provider: &str, get: impl Fn(&str) -> Result<Option<String>>) -> Result<Self> {
        if !matches!(provider, "s3" | "rustfs" | "r2") {
            return Err(invalid("S3 provider must be s3, rustfs, or r2"));
        }
        let config = Self {
            bucket: get("LOOM_S3_BUCKET")?.ok_or_else(|| invalid("set LOOM_S3_BUCKET"))?,
            // R2 requires auto, regardless of AWS configuration on the same host.
            region: if provider == "r2" {
                "auto".into()
            } else {
                get("AWS_REGION")?
                    .or(get("AWS_DEFAULT_REGION")?)
                    .unwrap_or_else(|| "us-east-1".into())
            },
            endpoint: get("LOOM_S3_ENDPOINT")?,
            prefix: get("LOOM_S3_PREFIX")?.unwrap_or_else(|| "loom".into()),
            force_path_style: bool_value(
                "LOOM_S3_FORCE_PATH_STYLE",
                get("LOOM_S3_FORCE_PATH_STYLE")?,
                provider != "s3",
            )?,
            allow_http: bool_value("LOOM_S3_ALLOW_HTTP", get("LOOM_S3_ALLOW_HTTP")?, false)?,
        };
        if provider != "s3" && config.endpoint.is_none() {
            return Err(invalid(&format!("{provider} requires LOOM_S3_ENDPOINT")));
        }
        config.validate()?;
        Ok(config)
    }
    fn validate(&self) -> Result<()> {
        if self.bucket.is_empty() || self.region.is_empty() {
            return Err(invalid("S3 bucket and region must not be empty"));
        }
        if self.prefix.is_empty()
            || self
                .prefix
                .split('/')
                .any(|s| s.is_empty() || s == "." || s == "..")
            || !self
                .prefix
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/-_.".contains(&b))
        {
            return Err(invalid(
                "S3 prefix must contain nonempty path segments using letters, digits, /, -, _, or .",
            ));
        }
        if let Some(endpoint) = &self.endpoint {
            if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
                return Err(invalid("S3 endpoint must be an http(s) URL"));
            }
            if endpoint.starts_with("http://") && !self.allow_http {
                return Err(invalid("HTTP endpoint requires LOOM_S3_ALLOW_HTTP=true"));
            }
        }
        Ok(())
    }
    /// Use the provider's refreshable environment / EC2 / ECS / web-identity
    /// credentials. For custom providers, construct with `with_builder` instead.
    pub fn connect(&self, namespace: &str) -> Result<S3ObjectStore> {
        let values = std::env::vars_os()
            .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)));
        self.with_builder(namespace, environment_builder(values))
    }
    pub fn with_builder(&self, namespace: &str, builder: AmazonS3Builder) -> Result<S3ObjectStore> {
        self.validate()?;
        let mut builder = builder
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_virtual_hosted_style_request(!self.force_path_style)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_client_options(
                ClientOptions::new()
                    .with_allow_http(self.allow_http)
                    .with_timeout(Duration::from_secs(60))
                    .with_connect_timeout(Duration::from_secs(10)),
            )
            .with_retry(RetryConfig {
                max_retries: 3,
                retry_timeout: Duration::from_secs(60),
                ..Default::default()
            });
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        S3ObjectStore::wrap(
            Arc::new(builder.build().map_err(provider_error)?),
            &self.prefix,
            namespace,
        )
    }
}

/// Select a shared object provider at the application's composition root.
/// The existing default is returned unchanged unless a provider is explicitly selected.
pub fn configured(namespace: &str, default: Arc<dyn ObjectStore>) -> Result<Arc<dyn ObjectStore>> {
    match env("LOOM_OBJECT_STORE")?.as_deref().unwrap_or("postgres") {
        "postgres" => Ok(default),
        provider @ ("s3" | "rustfs" | "r2") => Ok(Arc::new(
            S3Config::from_provider_env(provider)?.connect(namespace)?,
        )),
        _ => Err(invalid(
            "LOOM_OBJECT_STORE must be postgres, s3, rustfs, or r2",
        )),
    }
}

#[derive(Clone)]
pub struct S3ObjectStore {
    inner: Arc<dyn Provider>,
    root: String,
}
impl S3ObjectStore {
    fn wrap(inner: Arc<dyn Provider>, prefix: &str, namespace: &str) -> Result<Self> {
        if namespace.is_empty() || namespace.len() > 256 {
            return Err(invalid("object namespace must contain 1..256 bytes"));
        }
        // Encode the full namespace, not a lossy path normalization.
        let encoded: String = namespace
            .as_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        Ok(Self {
            inner,
            root: format!("{prefix}/v1/{encoded}"),
        })
    }
    fn key(&self, object: &ObjectRef) -> Result<Path> {
        if object.digest.len() != 64
            || !object
                .digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid(
                "object digest must be 64 lowercase hexadecimal characters",
            ));
        }
        Ok(Path::from(format!(
            "{}/{}/{}",
            self.root,
            &object.digest[..2],
            object.digest
        )))
    }
}
#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, bytes: Bytes) -> Result<ObjectRef> {
        let object = ObjectRef::new(format!("{:x}", Sha256::digest(&bytes)));
        let key = self.key(&object)?;
        let len = bytes.len();
        // Never overwrite: publish atomically using If-None-Match: *.
        let options = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self.inner.put_opts(&key, bytes.into(), options).await {
            Ok(_) => Ok(object),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let existing = self.get(&object).await?;
                if existing.len() != len {
                    return Err(invalid("existing object length differs"));
                }
                Ok(object)
            }
            Err(error) => Err(provider_error(error)),
        }
    }
    async fn get(&self, object: &ObjectRef) -> Result<Bytes> {
        let bytes = self
            .inner
            .get(&self.key(object)?)
            .await
            .map_err(provider_error)?
            .bytes()
            .await
            .map_err(provider_error)?;
        if format!("{:x}", Sha256::digest(&bytes)) != object.digest {
            return Err(invalid("object checksum mismatch"));
        }
        Ok(bytes.to_vec())
    }
    async fn size(&self, object: &ObjectRef) -> Result<u64> {
        Ok(self
            .inner
            .head(&self.key(object)?)
            .await
            .map_err(provider_error)?
            .size)
    }
    async fn get_range(&self, object: &ObjectRef, offset: u64, length: u64) -> Result<Bytes> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("object range overflow"))?;
        // S3 may truncate an overlong Range header. Preserve Loom's exact-range contract,
        // including zero-length reads at/beyond EOF and reads of missing objects.
        if end > self.size(object).await? {
            return Err(invalid("object range exceeds length"));
        }
        if length == 0 {
            return Ok(vec![]);
        }
        let bytes = self
            .inner
            .get_range(&self.key(object)?, offset..end)
            .await
            .map_err(provider_error)?;
        if bytes.len() as u64 != length {
            return Err(invalid("S3 returned a short range"));
        }
        Ok(bytes.to_vec())
    }
}
// Empty optional values are common in Compose. Do not let them select an empty
// static credential instead of a role, or attach an empty session-token header.
fn environment_builder(values: impl IntoIterator<Item = (String, String)>) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::new();
    for (key, value) in values {
        if key.starts_with("AWS_")
            && !value.is_empty()
            && let Ok(key) = key.to_ascii_lowercase().parse()
        {
            builder = builder.with_config(key, value);
        }
    }
    builder
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.into())
}
fn provider_error(error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { path, .. } => Error::NotFound(path),
        other => Error::Unavailable(format!("S3: {other}")),
    }
}
fn env(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(invalid(&format!("{name} must be UTF-8"))),
    }
}
fn bool_value(name: &str, value: Option<String>, default: bool) -> Result<bool> {
    value
        .map(|v| {
            v.parse()
                .map_err(|_| invalid(&format!("{name} must be true or false")))
        })
        .unwrap_or(Ok(default))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(provider: &str, values: &[(&str, &str)]) -> Result<S3Config> {
        S3Config::from_values(provider, |name| {
            Ok(values
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
                .filter(|v| !v.is_empty()))
        })
    }

    #[test]
    fn provider_defaults_and_validation() {
        let values = [
            ("LOOM_S3_BUCKET", "loom"),
            (
                "LOOM_S3_ENDPOINT",
                "https://account.eu.r2.cloudflarestorage.com",
            ),
            ("AWS_REGION", "us-west-2"),
            ("LOOM_S3_FORCE_PATH_STYLE", ""),
        ];
        let r2 = config("r2", &values).unwrap();
        assert_eq!(r2.region, "auto");
        assert!(r2.force_path_style);
        assert!(!r2.allow_http);
        assert_eq!(r2.endpoint.as_deref(), Some(values[1].1));
        let s3 = config("s3", &values).unwrap();
        assert_eq!(s3.region, "us-west-2");
        assert!(!s3.force_path_style);
        assert!(config("rustfs", &values).unwrap().force_path_style);
        let bucket = [("LOOM_S3_BUCKET", "loom")];
        assert!(config("r2", &bucket).is_err());
        assert!(config("rustfs", &bucket).is_err());
        assert_eq!(config("s3", &bucket).unwrap().region, "us-east-1");
        assert!(config("unknown", &bucket).is_err());
        assert!(config("r2", &[]).is_err());
        let mut values = values.to_vec();
        values[3].1 = "false";
        assert!(!config("r2", &values).unwrap().force_path_style);
        values[3].1 = "invalid";
        assert!(config("r2", &values).is_err());
        values[3].1 = "";
        values[1].1 = "http://localhost:9000";
        assert!(config("r2", &values).is_err());
    }

    /// Exercise the actual SDK request, not just the preset's configuration fields.
    #[tokio::test]
    async fn r2_signs_with_auto_and_uses_conditional_path_style_put() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 4096];
            loop {
                let count = socket.read(&mut buf).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&buf[..count]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]).to_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|l| l.strip_prefix("content-length: "))
                        .unwrap()
                        .parse()
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nETag: \"test\"\r\nConnection: close\r\n\r\n").await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let config = config(
            "r2",
            &[
                ("LOOM_S3_BUCKET", "test-bucket"),
                ("LOOM_S3_ENDPOINT", &endpoint),
                ("LOOM_S3_ALLOW_HTTP", "true"),
                ("AWS_REGION", "us-west-2"),
            ],
        )
        .unwrap();
        let store = config
            .with_builder(
                "test",
                AmazonS3Builder::new()
                    .with_access_key_id("test-access")
                    .with_secret_access_key("test-secret"),
            )
            .unwrap();
        let object = store.put(b"hello".to_vec()).await.unwrap();
        let request = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();
        assert!(request.starts_with(&format!(
            "PUT /test-bucket/loom/v1/74657374/{}/{} HTTP/1.1",
            &object.digest[..2],
            object.digest
        )));
        let lower = request.to_lowercase();
        assert!(lower.contains("/auto/s3/aws4_request"));
        assert!(lower.contains("if-none-match: *\r\n"));
        assert!(request.ends_with("hello"));
    }

    #[test]
    fn empty_credentials_do_not_override_roles() {
        use object_store::aws::AmazonS3ConfigKey;
        let builder = environment_builder([
            ("AWS_ACCESS_KEY_ID".into(), "".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "".into()),
            ("AWS_SESSION_TOKEN".into(), "".into()),
            ("AWS_ROLE_ARN".into(), "arn:aws:iam::123:role/test".into()),
        ]);
        assert!(
            builder
                .get_config_value(&AmazonS3ConfigKey::AccessKeyId)
                .is_none()
        );
        assert!(
            builder
                .get_config_value(&AmazonS3ConfigKey::Token)
                .is_none()
        );
        assert!(
            builder
                .get_config_value(&AmazonS3ConfigKey::RoleArn)
                .is_some()
        );
    }

    #[tokio::test]
    async fn integrity_and_namespace_paths() {
        let provider = Arc::new(object_store::memory::InMemory::new());
        let a = S3ObjectStore::wrap(provider.clone(), "loom", "a/b").unwrap();
        let b = S3ObjectStore::wrap(provider.clone(), "loom", "a%2Fb").unwrap();
        let object = a.put(b"hello".to_vec()).await.unwrap();
        assert_eq!(a.get_range(&object, 1, 3).await.unwrap(), b"ell");
        assert!(matches!(b.get(&object).await, Err(Error::NotFound(_))));
        provider
            .put(&a.key(&object).unwrap(), b"wrong".to_vec().into())
            .await
            .unwrap();
        assert!(
            a.get(&object)
                .await
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        assert!(a.put(b"hello".to_vec()).await.is_err());
        assert!(a.get(&ObjectRef::new("../escape".into())).await.is_err());
    }
}
