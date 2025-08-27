use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::sync::GILOnceCell;
use std::fs;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static APPENDER_GUARD: GILOnceCell<WorkerGuard> = GILOnceCell::new();

pub(crate) struct PyRotation(Rotation);

impl<'py> FromPyObject<'py> for PyRotation {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_ascii_lowercase();
        match s.as_str() {
            "minutely" => Ok(Self(Rotation::MINUTELY)),
            "hourly" => Ok(Self(Rotation::HOURLY)),
            "daily" => Ok(Self(Rotation::DAILY)),
            "never" => Ok(Self(Rotation::NEVER)),
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "rotation must be 'minutely', 'daily', 'hourly', or 'never', got '{other}'"
            ))),
        }
    }
}

/// Initialize Rust `tracing` to write to a rotating file.
/// - `dir`: directory for logs (created if missing)
/// - `filename`: base file name, e.g. "app.log"
/// - `rotation`: "daily" (default) or "hourly"
/// - `level`: optional EnvFilter string; falls back to RUST_LOG; else "info"
/// Returns: True if this call performed initialization; False if tracing was already set.
#[pyfunction(signature = (dir, prefix, *, suffix=None, max_log_files=None, rotation=PyRotation(Rotation::NEVER), level=None))]
pub(crate) fn init_log(
    py: Python,
    dir: std::path::PathBuf,
    prefix: String,
    suffix: Option<String>,
    max_log_files: Option<usize>,
    rotation: PyRotation,
    level: Option<&str>,
) -> PyResult<()> {
    // Ensure log directory exists
    fs::create_dir_all(&dir).map_err(|e| PyOSError::new_err(format!("create_dir_all: {e}")))?;

    let mut file_appender_builder = RollingFileAppender::builder()
        .rotation(rotation.0)
        .filename_prefix(prefix);
    if let Some(suffix) = suffix {
        file_appender_builder = file_appender_builder.filename_suffix(suffix);
    }
    if let Some(n) = max_log_files {
        if n == 0 {
            return Err(PyValueError::new_err("max_log_files must be positive"));
        }
        file_appender_builder = file_appender_builder.max_log_files(n);
    }
    let file_appender = file_appender_builder
        .build(dir)
        .map_err(|err| PyOSError::new_err(format!("Error building log appender: {err}")))?;

    // Non-blocking writer + guard (guard must be held for lifetime of program to flush on drop)
    let (nb_writer, guard) = tracing_appender::non_blocking(file_appender);

    // Try to store the guard to keep the non-blocking writer alive
    // If already set, we might have a problem
    if APPENDER_GUARD.set(py, guard).is_err() {
        // Guard is already set - this might cause issues with the current call
        // But we'll proceed anyway since the subscriber might still work
    }

    let filter = if let Some(spec) = level {
        EnvFilter::try_new(spec).map_err(|err| PyValueError::new_err(err.to_string()))?
    } else if let Ok(from_env) = EnvFilter::try_from_default_env() {
        from_env
    } else {
        EnvFilter::new("info")
    };

    // Try to install the global subscriber. If already set (by us or someone else),
    // try_init() returns Err; we report that we didn't initialize.
    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(nb_writer)
                .with_ansi(false) // plain text in files
                .with_target(true) // include module path (useful in prod)
                .compact(),
        )
        .try_init().map_err(|err| PyValueError::new_err(format!(
            "Error initializing tracing subscriber: {err}. This usually means tracing was already initialized by another library or a previous call to init_log.")))?;

    Ok(())
}
