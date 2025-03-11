use object_store::ObjectStoreScheme;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_object_store::{PyObjectStoreResult, PyUrl};

#[pyfunction]
pub(crate) fn parse_scheme(url: PyUrl) -> PyObjectStoreResult<&'static str> {
    let (scheme, _) =
        object_store::ObjectStoreScheme::parse(url.as_ref()).map_err(object_store::Error::from)?;
    match scheme {
        ObjectStoreScheme::AmazonS3 => Ok("s3"),
        ObjectStoreScheme::GoogleCloudStorage => Ok("gcs"),
        ObjectStoreScheme::Http => Ok("http"),
        ObjectStoreScheme::Local => Ok("local"),
        ObjectStoreScheme::Memory => Ok("memory"),
        ObjectStoreScheme::MicrosoftAzure => Ok("azure"),
        _ => Err(PyValueError::new_err("Unknown scheme: {scheme:?}").into()),
    }
}
