use async_trait::async_trait;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
};
use object_store::{ClientOptions, Result};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedStr;
use pyo3::types::{PyDict, PyString};

use crate::client::options::PyHeaderMap;
use crate::PyClientOptions;

/// An [HttpConnector] defined from Python.
#[derive(Debug)]
pub struct PyHttpConnector(Py<PyAny>);

impl HttpConnector for PyHttpConnector {
    fn connect(&self, options: &ClientOptions) -> Result<HttpClient> {
        let py_options = PyClientOptions::from(options.clone());
        let http_service = Python::attach(|py| {
            self.0
                .call_method1(py, intern!(py, "connect"), (py_options,))
        })
        .expect("httpconnector.connect");
        let client = HttpClient::new(PyHttpService(http_service));
        Ok(client)
    }
}

/// An [HttpService] defined from Python.
#[derive(Debug)]
pub struct PyHttpService(Py<PyAny>);

#[async_trait]
impl HttpService for PyHttpService {
    /// Perform [`HttpRequest`] returning [`HttpResponse`]
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let py_req = PyHttpRequest(req);
        let py_resp = Python::attach(|py| {
            self.0
                .call1(py, (py_req,))
                .expect("httpservice.call")
                .extract::<PyHttpResponse>(py)
                .expect("py http response extraction")
        });
        Ok(py_resp.0)
    }
}

pub struct PyHttpRequest(HttpRequest);

impl<'py> IntoPyObject<'py> for PyHttpRequest {
    type Target = PyDict;
    type Output = Bound<'py, PyDict>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let (parts, body) = self.0.into_parts();

        let dict = PyDict::new(py);
        dict.set_item(
            intern!(py, "method"),
            PyHttpMethod(parts.method).into_pyobject(py)?,
        )?;
        dict.set_item(intern!(py, "uri"), PyUri(parts.uri).into_pyobject(py)?)?;
        dict.set_item(
            intern!(py, "version"),
            PyHttpVersion(parts.version).into_pyobject(py)?,
        )?;
        dict.set_item(
            intern!(py, "headers"),
            PyHeaderMap(parts.headers).into_pyobject(py)?,
        )?;

        // TODO: body doesn't currently offer a way to access the underlying PutPayload for
        // Inner::PutPayload.
        dict.set_item(intern!(py, "body"), body.as_bytes().map(|v| v.as_ref()))?;

        Ok(dict)
    }
}

pub struct PyHttpMethod(http::Method);

impl<'py> IntoPyObject<'py> for PyHttpMethod {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        // TODO: in the future we could cache these http method constants so we aren't continually
        // accessing them
        let py_http_mod = py
            .import(intern!(py, "http"))?
            .get_item(intern!(py, "HTTPMethod"))?;
        py_http_mod.call1((self.0.as_str(),))
    }
}

pub struct PyUri(http::Uri);

impl<'py> IntoPyObject<'py> for PyUri {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let uri_parts = self.0.into_parts();

        let mut scheme = "";
        let mut netloc = "";
        let mut path = "";
        // TODO: upstream doesn't have a way to access params?
        let params = "";
        let mut query = "";
        // TODO: upstream doesn't have a way to access fragment?
        let fragment = "";

        if let Some(s) = &uri_parts.scheme {
            scheme = s.as_str();
        }

        if let Some(path_and_query) = &uri_parts.path_and_query {
            path = path_and_query.path();
            if let Some(q) = path_and_query.query() {
                query = q;
            }
        }

        if let Some(authority) = &uri_parts.authority {
            netloc = authority.as_str();
        }

        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "scheme"), PyString::new(py, scheme))?;
        kwargs.set_item(intern!(py, "netloc"), PyString::new(py, netloc))?;
        kwargs.set_item(intern!(py, "path"), path)?;
        kwargs.set_item(intern!(py, "params"), params)?;
        kwargs.set_item(intern!(py, "query"), query)?;
        kwargs.set_item(intern!(py, "fragment"), fragment)?;

        let urllib_parse_mod = py.import(intern!(py, "urllib.parse"))?;
        let parse_result_cls = urllib_parse_mod.getattr(intern!(py, "ParseResult"))?;
        parse_result_cls.call((), Some(&kwargs))
    }
}

pub struct PyHttpVersion(http::Version);

impl<'py> IntoPyObject<'py> for PyHttpVersion {
    type Target = PyString;
    type Output = Bound<'py, PyString>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        use http::Version;

        let s = match self.0 {
            Version::HTTP_09 => intern!(py, "0.9"),
            Version::HTTP_10 => intern!(py, "1.0"),
            Version::HTTP_11 => intern!(py, "1.1"),
            Version::HTTP_2 => intern!(py, "2.0"),
            Version::HTTP_3 => intern!(py, "3.0"),
            _ => unimplemented!("Unknown http version"),
        };
        Ok(s.clone())
    }
}

impl<'py> FromPyObject<'_, 'py> for PyHttpVersion {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> std::result::Result<Self, Self::Error> {
        use http::Version;

        let version_input = obj.extract::<PyBackedStr>()?;
        let http_version = match version_input.as_ref() {
            "0.9" => Version::HTTP_09,
            "1.0" => Version::HTTP_10,
            "1.1" => Version::HTTP_11,
            "2.0" => Version::HTTP_2,
            "3.0" => Version::HTTP_3,
            _ => panic!("Unsupported HTTP version"),
        };
        Ok(Self(http_version))
    }
}

pub struct PyHttpResponse(HttpResponse);

impl<'py> FromPyObject<'_, 'py> for PyHttpResponse {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> std::result::Result<Self, Self::Error> {
        let py = obj.py();
        let status = obj
            .getattr(intern!(py, "status"))?
            .extract::<PyHttpStatusCode>()?;
        let version = obj
            .getattr(intern!(py, "version"))?
            .extract::<PyHttpVersion>()?;
        let headers = obj
            .getattr(intern!(py, "headers"))?
            .extract::<PyHeaderMap>()?;

        // TODO: construct body. This probably will have to be a Python object that we poll?

        let resp = http::Response::new(body);
        let (mut parts, body) = resp.into_parts();

        parts.status = status.0;
        parts.version = version.0;
        parts.headers = headers.0;

        // There's also a `http::response::Builder` API but I can't figure out how I'd convert that
        // `Builder` to `Response`??
        Ok(Self(http::Response::from_parts(parts, body)))
    }
}
pub struct PyHttpStatusCode(http::StatusCode);

impl<'py> FromPyObject<'_, 'py> for PyHttpStatusCode {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> std::result::Result<Self, Self::Error> {
        let code = obj.extract::<u16>()?;
        Ok(PyHttpStatusCode(
            http::StatusCode::from_u16(code).expect("invalid http status code"),
        ))
    }
}

pub struct PyHttpResponseBody(HttpResponseBody);

pub struct PyHttpError(HttpError);

impl<'py> FromPyObject<'_, 'py> for PyHttpError {
    type Error = PyErr;

    fn extract(_obj: Borrowed<'_, 'py, PyAny>) -> std::result::Result<Self, Self::Error> {
        todo!("create http error kind")
    }
}
