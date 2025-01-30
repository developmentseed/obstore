use object_store::prefix::PrefixStore;
use object_store::{parse_url, parse_url_opts, ObjectStoreScheme};
use pyo3::prelude::*;

use crate::aws::PyAmazonS3Config;
use crate::azure::PyAzureConfig;
use crate::gcp::PyGoogleConfig;
use crate::url::PyUrl;
use crate::{PyObjectStoreResult, PyPrefixStore};

/// Wrapper around [`object_store::parse_url`] for simple construction of stores by url.
// Note: We don't extract the PyObject in the function signature because it's possible that
// AWS/Azure/Google config keys could overlap. And so we don't want to accidentally parse a config
// as an AWS config before knowing that the URL scheme is AWS.
#[pyfunction]
#[pyo3(signature = (url, *, config=None, **kwargs))]
pub fn new_store(
    url: PyUrl,
    config: Option<Bound<PyAny>>,
    kwargs: Option<Bound<PyAny>>,
) -> PyObjectStoreResult<PyPrefixStore> {
    let url = url.into_inner();
    let (scheme, _) = ObjectStoreScheme::parse(&url).map_err(object_store::Error::from)?;
    let (store, path) = match scheme {
        ObjectStoreScheme::AmazonS3 => {
            let config = config
                .map(|x| x.extract::<PyAmazonS3Config>())
                .transpose()?;
            let kwargs = kwargs
                .map(|x| x.extract::<PyAmazonS3Config>())
                .transpose()?;
            // TODO: merge config and kwargs
            parse_url_opts(&url, config.unwrap().into_inner())?
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let config = config.map(|x| x.extract::<PyGoogleConfig>()).transpose()?;
            let kwargs = kwargs.map(|x| x.extract::<PyGoogleConfig>()).transpose()?;
            // TODO: merge config and kwargs
            parse_url_opts(&url, config.unwrap().into_inner())?
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let config = config.map(|x| x.extract::<PyAzureConfig>()).transpose()?;
            let kwargs = kwargs.map(|x| x.extract::<PyAzureConfig>()).transpose()?;
            // TODO: merge config and kwargs
            parse_url_opts(&url, config.unwrap().into_inner())?
        }
        // TODO: assert no config or kwargs provided
        _ => parse_url(&url)?,
        // scheme => {
        //     return Err(PyValueError::new_err(format!(
        //         "Cannot pass config parameters for scheme {:?}",
        //         scheme,
        //     ))
        //     .into())
        // }
    };

    Ok(PrefixStore::new(store.into(), path).into())
}
