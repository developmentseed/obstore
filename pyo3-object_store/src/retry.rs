use std::time::Duration;

use object_store::{BackoffConfig, RetryConfig};
use pyo3::prelude::*;

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub struct PyBackoffConfig {
    #[pyo3(item)]
    init_backoff: Option<Duration>,
    #[pyo3(item)]
    max_backoff: Option<Duration>,
    #[pyo3(item)]
    base: Option<f64>,
}

impl From<PyBackoffConfig> for BackoffConfig {
    fn from(value: PyBackoffConfig) -> Self {
        let mut backoff_config = BackoffConfig::default();
        if let Some(init_backoff) = value.init_backoff {
            backoff_config.init_backoff = init_backoff;
        }
        if let Some(max_backoff) = value.max_backoff {
            backoff_config.max_backoff = max_backoff;
        }
        if let Some(base) = value.base {
            backoff_config.base = base;
        }
        backoff_config
    }
}

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub struct PyRetryConfig {
    #[pyo3(item)]
    backoff: Option<PyBackoffConfig>,
    #[pyo3(item)]
    max_retries: Option<usize>,
    #[pyo3(item)]
    retry_timeout: Option<Duration>,
}

impl From<PyRetryConfig> for RetryConfig {
    fn from(value: PyRetryConfig) -> Self {
        let mut retry_config = RetryConfig::default();
        if let Some(backoff) = value.backoff {
            retry_config.backoff = backoff.into();
        }
        if let Some(max_retries) = value.max_retries {
            retry_config.max_retries = max_retries;
        }
        if let Some(retry_timeout) = value.retry_timeout {
            retry_config.retry_timeout = retry_timeout;
        }
        retry_config
    }
}
