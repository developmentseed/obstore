use std::sync::Arc;

use aws_config::SdkConfig;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::AmazonS3Builder;
use object_store::CredentialProvider;

pub fn from_sdk_config(config: SdkConfig) -> AmazonS3Builder {
    let mut builder = AmazonS3Builder::new();
    if let Some(region) = config.region() {
        builder = builder.with_region(region.as_ref());
    }
    if let Some(credentials_provider) = config.credentials_provider() {
        builder = builder.with_credentials(Arc::new(WrappedAwsCredentialsProvider(
            credentials_provider,
        )));
    }
    if let Some(endpoint) = config.endpoint_url() {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(retry_config) = config.retry_config() {
        builder = builder.with_retry(from_retry_config(retry_config));
    }

    builder
}

fn from_retry_config(config: &aws_config::retry::RetryConfig) -> object_store::RetryConfig {
    let backoff = object_store::BackoffConfig {
        init_backoff: config.initial_backoff(),
        max_backoff: config.max_backoff(),
        ..Default::default()
    };

    object_store::RetryConfig {
        backoff,
        max_retries: config.max_attempts() as _,
        ..Default::default()
    }
}

#[derive(Debug)]
struct WrappedAwsCredentialsProvider(SharedCredentialsProvider);

#[async_trait::async_trait]
impl CredentialProvider for WrappedAwsCredentialsProvider {
    type Credential = object_store::aws::AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let credentials =
            self.0
                .provide_credentials()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "S3",
                    source: Box::new(e),
                })?;
        let credentials = object_store::aws::AwsCredential {
            key_id: credentials.access_key_id().to_string(),
            secret_key: credentials.secret_access_key().to_string(),
            token: credentials.session_token().map(|s| s.to_string()),
        };
        Ok(Arc::new(credentials))
    }
}
