#[cfg(feature = "aws-config")]
mod shared_config;
mod store;

pub use store::PyS3Store;
