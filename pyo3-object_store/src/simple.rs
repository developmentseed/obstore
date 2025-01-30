use object_store::prefix::PrefixStore;
use object_store::{parse_url, parse_url_opts, ObjectStoreScheme};
use pyo3::prelude::*;

use crate::error::ObstoreError;
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
            let combined = crate::aws::combine_config_kwargs(
                config.map(|x| x.extract()).transpose()?,
                kwargs.map(|x| x.extract()).transpose()?,
            )?
            .unwrap_or_default();
            parse_url_opts(&url, combined.into_inner())?
        }
        ObjectStoreScheme::GoogleCloudStorage => {
            let combined = crate::gcp::combine_config_kwargs(
                config.map(|x| x.extract()).transpose()?,
                kwargs.map(|x| x.extract()).transpose()?,
            )?
            .unwrap_or_default();
            parse_url_opts(&url, combined.into_inner())?
        }
        ObjectStoreScheme::MicrosoftAzure => {
            let combined = crate::azure::combine_config_kwargs(
                config.map(|x| x.extract()).transpose()?,
                kwargs.map(|x| x.extract()).transpose()?,
            )?
            .unwrap_or_default();
            parse_url_opts(&url, combined.into_inner())?
        }
        scheme => {
            if config.is_some() || kwargs.is_some() {
                return Err(ObstoreError::new_err(format!(
                    "Cannot pass config or keyword parameters for scheme {:?}",
                    scheme,
                ))
                .into());
            }
            parse_url(&url)?
        }
    };

    Ok(PrefixStore::new(store.into(), path).into())
}
