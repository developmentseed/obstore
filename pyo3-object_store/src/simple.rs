use object_store::prefix::PrefixStore;
use object_store::{parse_url, parse_url_opts};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::aws::PyAmazonS3Config;
use crate::azure::PyAzureConfig;
use crate::gcp::PyGoogleConfig;
use crate::url::PyUrl;
use crate::{PyObjectStoreResult, PyPrefixStore};

#[derive(FromPyObject)]
pub enum ParseUrlConfig {
    Aws(PyAmazonS3Config),
    Azure(PyAzureConfig),
    Google(PyGoogleConfig),
}

/// Wrapper around [`object_store::parse_url`] for simple construction of stores by url.
#[pyfunction]
#[pyo3(signature = (url, *, config=None, **kwargs))]
pub fn new_store(
    url: PyUrl,
    config: Option<ParseUrlConfig>,
    kwargs: Option<ParseUrlConfig>,
) -> PyObjectStoreResult<PyPrefixStore> {
    let (store, path) = match (config, kwargs) {
        (Some(_), Some(_)) => {
            // TODO: merge configs
            return Err(PyValueError::new_err("Cannot pass both config and kwargs").into());
        }
        // Note: In theory, we could avoid a match by implementing `IntoIterator` on
        // `ParseUrlConfig`, but I can't figure out the generics.
        (None, Some(config)) | (Some(config), None) => match config {
            ParseUrlConfig::Aws(aws_config) => {
                parse_url_opts(url.as_ref(), aws_config.into_inner())
            }
            ParseUrlConfig::Azure(azure_config) => {
                parse_url_opts(url.as_ref(), azure_config.into_inner())
            }
            ParseUrlConfig::Google(google_config) => {
                parse_url_opts(url.as_ref(), google_config.into_inner())
            }
        },
        (None, None) => parse_url(url.as_ref()),
    }?;
    Ok(PrefixStore::new(store.into(), path).into())
}
