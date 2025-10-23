use std::collections::HashMap;

use object_store::TagSet;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;

pub(crate) struct PyTagSet(TagSet);

impl PyTagSet {
    pub fn into_inner(self) -> TagSet {
        self.0
    }
}

impl<'py> FromPyObject<'_, 'py> for PyTagSet {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let input = obj.extract::<HashMap<PyBackedStr, PyBackedStr>>()?;
        let mut tag_set = TagSet::default();
        for (key, value) in input.into_iter() {
            tag_set.push(&key, &value);
        }
        Ok(Self(tag_set))
    }
}
