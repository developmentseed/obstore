//! Support for Python buffer protocol

use std::os::raw::c_int;
use std::ptr::NonNull;

use bytes::Bytes;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;

/// A wrapper around a [`bytes::Bytes`][].
///
/// This implements both import and export via the Python buffer protocol.
///
/// ### Buffer protocol import
///
/// This can be very useful as a general way to support ingest of a Python buffer protocol object.
///
/// The underlying [Bytes] manages the external memory, automatically calling the Python
/// buffer's release callback when the internal reference count reaches 0.
///
/// Note that converting this [`Bytes`] into a [BytesMut][::bytes::BytesMut] will always create a
/// deep copy of the buffer into newly allocated memory, since this `Bytes` is constructed from an
/// owner.
///
/// ### Buffer protocol export
///
/// PyBytes implements the Python buffer protocol to enable Python to access the underlying `Bytes`
/// data view without copies. In Python, this `PyBytes` object can be passed to Python `bytes` or
/// `memoryview` constructors, `numpy.frombuffer`, or any other function that supports buffer
/// protocol input.
#[pyclass(name = "Bytes", subclass, frozen)]
pub struct PyBytes(Bytes);

impl AsRef<Bytes> for PyBytes {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}

impl AsRef<[u8]> for PyBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PyBytes {
    /// Construct a new [PyBytes]
    pub fn new(buffer: Bytes) -> Self {
        Self(buffer)
    }

    /// Consume and return the [Bytes]
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl From<PyBytes> for Bytes {
    fn from(value: PyBytes) -> Self {
        value.0
    }
}

impl From<Bytes> for PyBytes {
    fn from(value: Bytes) -> Self {
        PyBytes(value)
    }
}

#[pymethods]
impl PyBytes {
    // By setting the argument to PyBytes, this means that any buffer-protocol object is supported
    // here, since it will use the FromPyObject impl.
    #[new]
    fn py_new(buf: PyBytes) -> Self {
        buf
    }

    /// Copy this buffer's contents to a Python `bytes` object
    fn to_bytes<'py>(&'py self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new(py, &self.0)
    }

    /// The number of bytes in this Bytes
    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        format!("Bytes(len={})", self.0.len())
    }

    /// This is taken from opendal:
    /// https://github.com/apache/opendal/blob/d001321b0f9834bc1e2e7d463bcfdc3683e968c9/bindings/python/src/utils.rs#L51-L72
    unsafe fn __getbuffer__(
        slf: PyRef<Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let bytes = slf.0.as_ref();
        let ret = ffi::PyBuffer_FillInfo(
            view,
            slf.as_ptr() as *mut _,
            bytes.as_ptr() as *mut _,
            bytes.len().try_into().unwrap(),
            1, // read only
            flags,
        );
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    // Comment from david hewitt on discord:
    // > I think normally `__getbuffer__` takes a pointer to the owning Python object, so you
    // > don't need to treat the allocation as owned separately. It should be good enough to keep
    // > the allocation owned by the object.
    // https://discord.com/channels/1209263839632424990/1324816949464666194/1328299411427557397
    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

impl<'py> FromPyObject<'py> for PyBytes {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let buffer = ob.extract::<PyBytesWrapper>()?;
        let bytes = Bytes::from_owner(buffer);
        Ok(Self(bytes))
    }
}

/// A wrapper around a PyBuffer that applies a custom destructor that checks if the Python
/// interpreter is still initialized before freeing the buffer memory.
///
/// This also implements AsRef<[u8]> because that is required for Bytes::from_owner
#[derive(Debug)]
struct PyBytesWrapper(Option<PyBuffer<u8>>);

impl Drop for PyBytesWrapper {
    fn drop(&mut self) {
        // Only call the underlying Drop of PyBuffer if the Python interpreter is still
        // initialized. Sometimes the Drop can attempt to happen after the Python interpreter was
        // already finalized.
        // https://github.com/kylebarron/arro3/issues/230
        let is_initialized = unsafe { ffi::Py_IsInitialized() };
        if let Some(val) = self.0.take() {
            if is_initialized == 0 {
                std::mem::forget(val);
            } else {
                std::mem::drop(val);
            }
        }
    }
}

impl AsRef<[u8]> for PyBytesWrapper {
    fn as_ref(&self) -> &[u8] {
        let buffer = self.0.as_ref().expect("Buffer already disposed");
        let len = buffer.item_count();

        let ptr = NonNull::new(buffer.buf_ptr() as _).expect("Expected buffer ptr to be non null");

        // Safety:
        //
        // This requires that the data will not be mutated from Python. Sadly, the buffer protocol
        // does not uphold this invariant always for us, and the Python user must take care not to
        // mutate the provided buffer.
        unsafe { std::slice::from_raw_parts(ptr.as_ptr() as *const u8, len) }
    }
}

fn validate_buffer(buf: &PyBuffer<u8>) -> PyResult<()> {
    if !buf.is_c_contiguous() {
        return Err(PyValueError::new_err("Buffer is not C contiguous"));
    }

    if buf.shape().iter().any(|s| *s == 0) {
        return Err(PyValueError::new_err("0-length dimension not supported."));
    }

    if buf.strides().iter().any(|s| *s == 0) {
        return Err(PyValueError::new_err("Non-zero strides not supported."));
    }

    Ok(())
}

impl<'py> FromPyObject<'py> for PyBytesWrapper {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let buffer = ob.extract::<PyBuffer<u8>>()?;
        validate_buffer(&buffer)?;
        Ok(Self(Some(buffer)))
    }
}
