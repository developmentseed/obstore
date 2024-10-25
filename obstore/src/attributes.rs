use std::borrow::Cow;
use std::collections::HashMap;

use indexmap::IndexMap;
use object_store::{Attribute, AttributeValue, Attributes};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PyAttribute(Attribute);

impl<'py> FromPyObject<'py> for PyAttribute {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let s = ob.extract::<PyBackedStr>()?.to_ascii_lowercase();
        match s.as_str() {
            "content-disposition" | "contentdisposition" => Ok(Self(Attribute::ContentDisposition)),
            "Content-Encoding" | "ContentEncoding" => Ok(Self(Attribute::ContentEncoding)),
            "Content-Language" | "ContentLanguage" => Ok(Self(Attribute::ContentLanguage)),
            "Content-Type" | "ContentType" => Ok(Self(Attribute::ContentType)),
            "Cache-Control" | "CacheControl" => Ok(Self(Attribute::CacheControl)),
            _ => Ok(Self(Attribute::Metadata(Cow::Owned(s)))),
        }
    }
}

fn attribute_to_string(attribute: &Attribute) -> Cow<'static, str> {
    match attribute {
        Attribute::ContentDisposition => Cow::Borrowed("Content-Disposition"),
        Attribute::ContentEncoding => Cow::Borrowed("Content-Encoding"),
        Attribute::ContentLanguage => Cow::Borrowed("Content-Language"),
        Attribute::ContentType => Cow::Borrowed("Content-Type"),
        Attribute::CacheControl => Cow::Borrowed("Cache-Control"),
        Attribute::Metadata(x) => x.clone(),
        other => panic!("Unexpected attribute: {:?}", other),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PyAttributeValue(AttributeValue);

impl<'py> FromPyObject<'py> for PyAttributeValue {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        Ok(Self(ob.extract::<String>()?.into()))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PyAttributes(Attributes);

impl PyAttributes {
    pub fn new(attributes: Attributes) -> Self {
        Self(attributes)
    }

    pub fn into_inner(self) -> Attributes {
        self.0
    }
}

impl<'py> FromPyObject<'py> for PyAttributes {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let d = ob.extract::<HashMap<PyAttribute, PyAttributeValue>>()?;
        let mut attributes = Attributes::with_capacity(d.len());
        for (k, v) in d.into_iter() {
            attributes.insert(k.0, v.0);
        }
        Ok(Self(attributes))
    }
}

impl IntoPy<PyObject> for PyAttributes {
    fn into_py(self, py: Python<'_>) -> PyObject {
        let mut d = IndexMap::with_capacity(self.0.len());
        for (k, v) in self.0.into_iter() {
            d.insert(attribute_to_string(k), v.as_ref());
        }
        d.into_py(py)
    }
}
