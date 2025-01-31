use std::time::Duration;

use object_store::{BackoffConfig, RetryConfig};
use pyo3::prelude::*;

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub struct PyBackoffConfig {
    #[pyo3(item)]
    init_backoff: Duration,
    #[pyo3(item)]
    max_backoff: Duration,
    #[pyo3(item)]
    base: f64,
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

#[derive(Clone, Debug, FromPyObject, IntoPyObject)]
pub struct PyRetryConfig {
    #[pyo3(item)]
    backoff: PyBackoffConfig,
    #[pyo3(item)]
    max_retries: usize,
    #[pyo3(item)]
    retry_timeout: Duration,
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
