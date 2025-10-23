use std::borrow::Cow;
use std::collections::HashMap;

use indexmap::IndexMap;
use object_store::{Attribute, AttributeValue, Attributes};
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::PyDict;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PyAttribute(Attribute);

impl<'py> FromPyObject<'_, 'py> for PyAttribute {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s = obj.extract::<PyBackedStr>()?;
        match s.to_ascii_lowercase().as_str() {
            "content-disposition" | "contentdisposition" => Ok(Self(Attribute::ContentDisposition)),
            "content-encoding" | "contentencoding" => Ok(Self(Attribute::ContentEncoding)),
            "content-language" | "contentlanguage" => Ok(Self(Attribute::ContentLanguage)),
            "content-type" | "contenttype" => Ok(Self(Attribute::ContentType)),
            "cache-control" | "cachecontrol" => Ok(Self(Attribute::CacheControl)),
            _ => Ok(Self(Attribute::Metadata(Cow::Owned(s.to_string())))),
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
        other => panic!("Unexpected attribute: {other:?}"),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PyAttributeValue(AttributeValue);

impl<'py> FromPyObject<'_, 'py> for PyAttributeValue {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        Ok(Self(obj.extract::<String>()?.into()))
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

impl<'py> FromPyObject<'_, 'py> for PyAttributes {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let d = obj.extract::<HashMap<PyAttribute, PyAttributeValue>>()?;
        let mut attributes = Attributes::with_capacity(d.len());
        for (k, v) in d.into_iter() {
            attributes.insert(k.0, v.0);
        }
        Ok(Self(attributes))
    }
}

impl<'py> IntoPyObject<'py> for PyAttributes {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let mut d = IndexMap::with_capacity(self.0.len());
        for (k, v) in self.0.into_iter() {
            d.insert(attribute_to_string(k), v.as_ref());
        }
        d.into_pyobject(py)
    }
}
