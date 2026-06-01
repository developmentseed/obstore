//! Google Cloud Storage Object Store Python bindings.

mod credentials;
mod store;

pub use credentials::PyGcpCredentialProvider;
pub use store::{PyGCSStore, PyGoogleConfig, PyGoogleConfigKey};
