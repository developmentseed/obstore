#![doc = include_str!("../README.md")]
#![warn(missing_docs)]

mod api;
mod aws;
mod azure;
mod client;
pub mod error;
mod gcp;
mod http;
mod local;
mod memory;
mod retry;
mod store;

pub use api::{register_exceptions_module, register_store_module};
pub use aws::PyS3Store;
pub use azure::PyAzureStore;
pub use client::{PyClientConfigKey, PyClientOptions};
pub use gcp::PyGCSStore;
pub use http::PyHttpStore;
pub use local::PyLocalStore;
pub use memory::PyMemoryStore;
pub use store::PyObjectStore;
