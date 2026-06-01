//! AWS S3 object store binding.

mod credentials;
mod store;

pub use credentials::PyAWSCredentialProvider;
pub use store::{PyAmazonS3Config, PyAmazonS3ConfigKey, PyS3Store};
