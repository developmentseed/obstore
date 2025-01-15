use std::time::Duration;

use humantime::format_duration;
use pyo3::prelude::*;

/// A wrapper around `String` used to store values for config values.
///
/// Supported Python input:
///
/// - `True` and `False` (becomes `"true"` and `"false"`)
/// - `timedelta`
/// - `str`
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PyConfigValue(pub String);

impl<'py> FromPyObject<'py> for PyConfigValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        if let Ok(val) = ob.extract::<bool>() {
            Ok(Self(val.to_string()))
        } else if let Ok(duration) = ob.extract::<Duration>() {
            Ok(Self(format_duration(duration).to_string()))
        } else {
            Ok(Self(ob.extract()?))
        }
    }
}
