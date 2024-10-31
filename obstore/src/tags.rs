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

impl<'py> FromPyObject<'py> for PyTagSet {
    fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
        let input = ob.extract::<HashMap<PyBackedStr, PyBackedStr>>()?;
        let mut tag_set = TagSet::default();
        for (key, value) in input.into_iter() {
            tag_set.push(&key, &value);
        }
        Ok(Self(tag_set))
    }
}
