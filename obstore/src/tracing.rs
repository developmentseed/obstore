use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::sync::GILOnceCell;
use std::fs;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use tracing_subscriber::{Layer, Registry};

static APPENDER_GUARD: GILOnceCell<WorkerGuard> = GILOnceCell::new();

#[derive(Clone)]
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
#[pyfunction(signature = ( *, stderr=None, stdout=None, file=None, level=None))]
pub(crate) fn init_log(
    py: Python,
    stderr: Option<LogLayerConfig>,
    stdout: Option<LogLayerConfig>,
    file: Option<FileLogConfig>,
    level: Option<&str>,
) -> PyResult<()> {
    // Collect all layers into a Vec
    let mut layers: Vec<Box<dyn Layer<Registry> + Send + Sync>> = Vec::new();

    if let Some(stderr_config) = stderr {
        layers.push(create_log_layer(stderr_config, std::io::stderr));
    }
    if let Some(stdout_config) = stdout {
        layers.push(create_log_layer(stdout_config, std::io::stdout));
    }
    if let Some(file_config) = file {
        let appender = file_config.file.create_appender()?;
        let (nb_writer, guard) = tracing_appender::non_blocking(appender);
        // Try to store the guard to keep the non-blocking writer alive
        // If already set, we might have a problem
        if APPENDER_GUARD.set(py, guard).is_err() {
            // Guard is already set - this might cause issues with the current call
            // But we'll proceed anyway since the subscriber might still work
        }
        let file_layer = create_log_layer(file_config.into(), nb_writer);
        layers.push(file_layer);
    }

    // // Add file layer
    // let file_layer = tracing_subscriber::fmt::layer()
    //     .with_writer(nb_writer)
    //     .with_ansi(false) // plain text in files
    //     .with_target(true) // include module path (useful in prod)
    //     .json()
    //     .boxed();
    // layers.push(file_layer);

    let filter = if let Some(spec) = level {
        EnvFilter::try_new(spec).map_err(|err| PyValueError::new_err(err.to_string()))?
    } else if let Ok(from_env) = EnvFilter::try_from_default_env() {
        from_env
    } else {
        EnvFilter::new("info")
    };

    // Build subscriber with all layers at once using Vec - layers first, then filter
    tracing_subscriber::registry()
        .with(layers)
        .with(filter)
        .try_init().map_err(|err| PyValueError::new_err(format!(
            "Error initializing tracing subscriber: {err}. This usually means tracing was already initialized by another library or a previous call to init_log.")))?;

    Ok(())
}

#[derive(Clone)]
enum LogFormat {
    Compact,
    Pretty,
    Json,
}

impl<'py> FromPyObject<'py> for LogFormat {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_ascii_lowercase();
        match s.as_str() {
            "compact" => Ok(Self::Compact),
            "pretty" => Ok(Self::Pretty),
            "json" => Ok(Self::Json),
            other => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "format must be 'compact', 'pretty', or 'json', got '{other}'"
            ))),
        }
    }
}

#[derive(Clone, FromPyObject)]
struct FileConfig {
    dir: std::path::PathBuf,
    prefix: String,
    suffix: Option<String>,
    max_log_files: Option<usize>,
    rotation: PyRotation,
}

impl FileConfig {
    fn create_appender(&self) -> PyResult<RollingFileAppender> {
        // Ensure log directory exists
        fs::create_dir_all(&self.dir)
            .map_err(|e| PyOSError::new_err(format!("create_dir_all: {e}")))?;

        let mut builder = RollingFileAppender::builder()
            .rotation(self.rotation.0.clone())
            .filename_prefix(&self.prefix);
        if let Some(suffix) = &self.suffix {
            builder = builder.filename_suffix(suffix);
        }
        if let Some(n) = self.max_log_files {
            if n == 0 {
                return Err(PyValueError::new_err("max_log_files must be positive"));
            }
            builder = builder.max_log_files(n);
        }
        builder
            .build(&self.dir)
            .map_err(|err| PyOSError::new_err(format!("Error building log appender: {err}")))
    }
}

#[derive(Clone, FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct FileLogConfig {
    file: FileConfig,
    format: LogFormat,
    #[pyo3(default)]
    show_ansi: Option<bool>,
    #[pyo3(default)]
    show_target: Option<bool>,
    #[pyo3(default)]
    show_thread_names: Option<bool>,
    #[pyo3(default)]
    show_thread_ids: Option<bool>,
    #[pyo3(default)]
    show_level: Option<bool>,
    #[pyo3(default)]
    show_filename: Option<bool>,
    #[pyo3(default)]
    show_line_number: Option<bool>,
}

impl From<FileLogConfig> for LogLayerConfig {
    fn from(file_log_config: FileLogConfig) -> Self {
        LogLayerConfig {
            format: file_log_config.format,
            show_ansi: file_log_config.show_ansi,
            show_target: file_log_config.show_target,
            show_thread_names: file_log_config.show_thread_names,
            show_thread_ids: file_log_config.show_thread_ids,
            show_level: file_log_config.show_level,
            show_filename: file_log_config.show_filename,
            show_line_number: file_log_config.show_line_number,
        }
    }
}

#[derive(Clone, FromPyObject)]
#[pyo3(from_item_all)]
pub(crate) struct LogLayerConfig {
    format: LogFormat,
    #[pyo3(default)]
    show_ansi: Option<bool>,
    #[pyo3(default)]
    show_target: Option<bool>,
    #[pyo3(default)]
    show_thread_names: Option<bool>,
    #[pyo3(default)]
    show_thread_ids: Option<bool>,
    #[pyo3(default)]
    show_level: Option<bool>,
    #[pyo3(default)]
    show_filename: Option<bool>,
    #[pyo3(default)]
    show_line_number: Option<bool>,
}

fn create_log_layer<W2: for<'writer> MakeWriter<'writer> + Send + Sync + 'static>(
    config: LogLayerConfig,
    writer: W2,
) -> Box<dyn Layer<Registry> + Send + Sync> {
    let mut configured_layer = tracing_subscriber::fmt::layer().with_writer(writer);

    if let Some(show_ansi) = config.show_ansi {
        configured_layer = configured_layer.with_ansi(show_ansi);
    }
    if let Some(show_target) = config.show_target {
        configured_layer = configured_layer.with_target(show_target);
    }
    if let Some(show_thread_names) = config.show_thread_names {
        configured_layer = configured_layer.with_thread_names(show_thread_names);
    }
    if let Some(show_thread_ids) = config.show_thread_ids {
        configured_layer = configured_layer.with_thread_ids(show_thread_ids);
    }
    if let Some(show_level) = config.show_level {
        configured_layer = configured_layer.with_level(show_level);
    }
    if let Some(show_filename) = config.show_filename {
        configured_layer = configured_layer.with_file(show_filename);
    }
    if let Some(show_line_number) = config.show_line_number {
        configured_layer = configured_layer.with_line_number(show_line_number);
    }

    match config.format {
        LogFormat::Compact => configured_layer.compact().boxed(),
        LogFormat::Pretty => configured_layer.pretty().boxed(),
        LogFormat::Json => configured_layer.json().boxed(),
    }
}
