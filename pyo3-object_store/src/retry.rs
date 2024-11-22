use std::time::Duration;

use object_store::{BackoffConfig, RetryConfig};
use pyo3::prelude::*;

#[derive(Debug, FromPyObject)]
#[pyo3(from_item_all)]
pub struct PyBackoffConfig {
    init_backoff: Duration,
    max_backoff: Duration,
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

#[derive(Debug, FromPyObject)]
#[pyo3(from_item_all)]
pub struct PyRetryConfig {
    backoff: PyBackoffConfig,
    max_retries: usize,
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
