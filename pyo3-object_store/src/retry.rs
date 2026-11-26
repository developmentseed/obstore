use std::time::Duration;

use async_trait::async_trait;
use http::StatusCode;
use object_store::client::HttpErrorKind;
use object_store::retry::{RetryContext, RetryFailure, RetryPolicy};
use object_store::{BackoffConfig, RetryConfig};
use pyo3::intern;
use pyo3::prelude::*;

#[derive(Clone, Debug, IntoPyObject, IntoPyObjectRef, PartialEq)]
pub struct PyBackoffConfig {
    #[pyo3(item)]
    init_backoff: Duration,
    #[pyo3(item)]
    max_backoff: Duration,
    #[pyo3(item)]
    base: f64,
}

impl<'py> FromPyObject<'_, 'py> for PyBackoffConfig {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let mut backoff_config = BackoffConfig::default();
        let py = obj.py();
        if let Ok(init_backoff) = obj.get_item(intern!(py, "init_backoff")) {
            backoff_config.init_backoff = init_backoff.extract()?;
        }
        if let Ok(max_backoff) = obj.get_item(intern!(py, "max_backoff")) {
            backoff_config.max_backoff = max_backoff.extract()?;
        }
        if let Ok(base) = obj.get_item(intern!(py, "base")) {
            backoff_config.base = base.extract()?;
        }
        Ok(backoff_config.into())
    }
}

impl From<PyBackoffConfig> for BackoffConfig {
    fn from(value: PyBackoffConfig) -> Self {
        BackoffConfig {
            init_backoff: value.init_backoff,
            max_backoff: value.max_backoff,
            base: value.base,
        }
    }
}

impl From<BackoffConfig> for PyBackoffConfig {
    fn from(value: BackoffConfig) -> Self {
        PyBackoffConfig {
            init_backoff: value.init_backoff,
            max_backoff: value.max_backoff,
            base: value.base,
        }
    }
}

#[derive(Clone, Debug, IntoPyObject, IntoPyObjectRef, PartialEq)]
pub struct PyRetryConfig {
    #[pyo3(item)]
    backoff: PyBackoffConfig,
    #[pyo3(item)]
    max_retries: usize,
    #[pyo3(item)]
    retry_timeout: Duration,
}

impl<'py> FromPyObject<'_, 'py> for PyRetryConfig {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        let mut retry_config = RetryConfig::default();
        let py = obj.py();
        if let Ok(backoff) = obj.get_item(intern!(py, "backoff")) {
            retry_config.backoff = backoff.extract::<PyBackoffConfig>()?.into();
        }
        if let Ok(max_retries) = obj.get_item(intern!(py, "max_retries")) {
            retry_config.max_retries = max_retries.extract()?;
        }
        if let Ok(retry_timeout) = obj.get_item(intern!(py, "retry_timeout")) {
            retry_config.retry_timeout = retry_timeout.extract()?;
        }
        Ok(retry_config.into())
    }
}

impl From<PyRetryConfig> for RetryConfig {
    fn from(value: PyRetryConfig) -> Self {
        RetryConfig {
            backoff: value.backoff.into(),
            max_retries: value.max_retries,
            retry_timeout: value.retry_timeout,
        }
    }
}

impl From<RetryConfig> for PyRetryConfig {
    fn from(value: RetryConfig) -> Self {
        PyRetryConfig {
            backoff: value.backoff.into(),
            max_retries: value.max_retries,
            retry_timeout: value.retry_timeout,
        }
    }
}

/// A [`RetryPolicy`] for the parts of a multipart upload, configured from a Python `RetryConfig`.
///
/// Install it with [`PutMultipartOptions::with_retry_policy`][object_store::PutMultipartOptions::with_retry_policy].
/// `object_store` consults it after a part's own request retries are exhausted. It then uploads
/// the part again, with a newly built and signed request, if the failure is one that the store's
/// [`RetryConfig`] also retries, fewer than `max_retries` part retries have been made, and no more
/// than `retry_timeout` has passed since the part's first attempt. Before each retry it waits
/// `init_backoff * base^n`, capped at `max_backoff`, without jitter.
#[derive(Clone, Debug)]
pub struct PyMultipartRetryPolicy(RetryConfig);

impl<'py> FromPyObject<'_, 'py> for PyMultipartRetryPolicy {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, pyo3::PyAny>) -> PyResult<Self> {
        Ok(Self(obj.extract::<PyRetryConfig>()?.into()))
    }
}

impl PyMultipartRetryPolicy {
    fn backoff(&self, attempt: usize) -> Duration {
        let BackoffConfig {
            init_backoff,
            max_backoff,
            base,
        } = &self.0.backoff;
        let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let backoff = init_backoff.as_secs_f64() * base.powi(exponent);
        Duration::try_from_secs_f64(backoff)
            .map_or(*max_backoff, |backoff| backoff.min(*max_backoff))
    }
}

#[async_trait]
impl RetryPolicy for PyMultipartRetryPolicy {
    async fn retry(&self, context: RetryContext) -> bool {
        if !is_transient(context.failure)
            || context.attempt > self.0.max_retries
            || context.elapsed > self.0.retry_timeout
        {
            return false;
        }
        tokio::time::sleep(self.backoff(context.attempt)).await;
        true
    }
}

/// Whether the store's own request retries would retry this failure of an idempotent request.
fn is_transient(failure: RetryFailure) -> bool {
    match failure {
        // A success status means the response body reported an error, like S3's `SlowDown`
        RetryFailure::Status(status) => {
            status.is_success()
                || status.is_server_error()
                || status == StatusCode::TOO_MANY_REQUESTS
                || status == StatusCode::REQUEST_TIMEOUT
        }
        RetryFailure::Transport(kind) => matches!(
            kind,
            HttpErrorKind::Connect
                | HttpErrorKind::Request
                | HttpErrorKind::Timeout
                | HttpErrorKind::Interrupted
        ),
        _ => false,
    }
}
