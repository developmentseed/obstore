use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use http::header::{CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use object_store::path::Path;
use object_store::{
    Attributes, CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::intern;
use pyo3::prelude::*;
use serde::Deserialize;
use url::Url;

const STORE: &str = "RemoteSignedS3";

struct PySigner(Py<PyAny>);

impl Clone for PySigner {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl Debug for PySigner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PySigner(..)")
    }
}

impl<'py> FromPyObject<'_, 'py> for PySigner {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if !obj.hasattr(intern!(obj.py(), "__call__"))? {
            return Err(PyTypeError::new_err("Expected callable object for signer."));
        }
        Ok(Self(obj.as_unbound().clone_ref(obj.py())))
    }
}

enum PySignerResult {
    Async(Py<PyAny>),
    Sync((String, HashMap<String, String>)),
}

impl PySignerResult {
    async fn resolve(self) -> PyResult<(String, HashMap<String, String>)> {
        match self {
            Self::Sync(result) => Ok(result),
            Self::Async(coroutine) => {
                let future = Python::attach(|py| {
                    pyo3_async_runtimes::tokio::into_future(coroutine.bind(py).clone())
                })?;
                let result = future.await?;
                Python::attach(|py| result.extract(py))
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for PySignerResult {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if obj.hasattr(intern!(obj.py(), "__await__"))? {
            Ok(Self::Async(obj.as_unbound().clone_ref(obj.py())))
        } else {
            Ok(Self::Sync(obj.extract()?))
        }
    }
}

impl PySigner {
    async fn sign(
        &self,
        method: &Method,
        uri: &Url,
        headers: &HeaderMap,
    ) -> Result<(Url, HeaderMap)> {
        let headers: HashMap<String, String> = headers
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.as_str().to_owned(),
                    value
                        .to_str()
                        .map_err(|error| error_for("invalid request header", error))?
                        .to_owned(),
                ))
            })
            .collect::<Result<_>>()?;
        let result = Python::attach(|py| {
            self.0
                .call1(py, (method.as_str(), uri.as_str(), headers))?
                .extract::<PySignerResult>(py)
        })
        .map_err(|error| error_for("signer callback failed", error))?
        .resolve()
        .await
        .map_err(|error| error_for("signer callback failed", error))?;
        let uri = Url::parse(&result.0).map_err(|error| error_for("signer returned invalid URI", error))?;
        let headers = result
            .1
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    HeaderName::from_bytes(name.as_bytes())
                        .map_err(|error| error_for("signer returned invalid header name", error))?,
                    HeaderValue::from_str(&value)
                        .map_err(|error| error_for("signer returned invalid header value", error))?,
                ))
            })
            .collect::<Result<HeaderMap>>()?;
        Ok((uri, headers))
    }
}

/// An S3-compatible object store that obtains a signature for every HTTP request from Python.
#[derive(Debug, Clone)]
pub struct RemoteSignedS3Store {
    base_url: Url,
    bucket_url: Url,
    prefix: String,
    signer: PySigner,
    client: reqwest::Client,
}

impl RemoteSignedS3Store {
    fn object_url(&self, location: &Path) -> Result<Url> {
        self.base_url
            .join(location.as_ref())
            .map_err(|error| error_for("invalid object URL", error))
    }

    async fn request(
        &self,
        method: Method,
        location: &Path,
        headers: HeaderMap,
        body: Option<Bytes>,
    ) -> Result<reqwest::Response> {
        self.request_url(method, location, self.object_url(location)?, headers, body).await
    }

    async fn request_url(
        &self,
        method: Method,
        location: &Path,
        uri: Url,
        headers: HeaderMap,
        body: Option<Bytes>,
    ) -> Result<reqwest::Response> {
        let (uri, headers) = self.signer.sign(&method, &uri, &headers).await?;
        let mut request = self.client.request(method, uri).headers(headers);
        if let Some(body) = body {
            request = request.body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| error_for("HTTP request failed", error))?;
        check_status(location, response.status())?;
        Ok(response)
    }

    async fn list_page(
        &self,
        prefix: Option<&Path>,
        delimiter: Option<&str>,
        token: Option<&str>,
    ) -> Result<ListPage> {
        let mut uri = self.bucket_url.clone();
        let prefix = join_prefix(&self.prefix, prefix.map(Path::as_ref));
        {
            let mut query = uri.query_pairs_mut();
            query.append_pair("list-type", "2");
            query.append_pair("prefix", &prefix);
            if let Some(delimiter) = delimiter {
                query.append_pair("delimiter", delimiter);
            }
            if let Some(token) = token {
                query.append_pair("continuation-token", token);
            }
        }
        let response = self
            .request_url(Method::GET, &Path::default(), uri, HeaderMap::new(), None)
            .await?;
        let body = response
            .bytes()
            .await
            .map_err(|error| error_for("failed to read list response", error))?;
        let response: ListObjectsV2 = quick_xml::de::from_reader(body.as_ref())
            .map_err(|error| error_for("invalid ListObjectsV2 response", error))?;
        let objects = response
            .contents
            .into_iter()
            .map(|object| object.into_meta(&self.prefix))
            .collect::<Result<Vec<_>>>()?;
        let common_prefixes = response
            .common_prefixes
            .into_iter()
            .filter_map(|prefix| strip_prefix(&self.prefix, &prefix.prefix).map(str::to_owned))
            .map(Path::parse)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(ListPage { objects, common_prefixes, next_token: response.next_continuation_token })
    }

    // ponytail: list buffers all pages; stream page-by-page if very large prefixes matter.
    async fn list_all(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut token = None;
        loop {
            let page = self.list_page(prefix, None, token.as_deref()).await?;
            objects.extend(page.objects);
            match page.next_token {
                Some(next) => token = Some(next),
                None => return Ok(objects),
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename = "ListBucketResult")]
struct ListObjectsV2 {
    #[serde(rename = "Contents", default)]
    contents: Vec<ListObject>,
    #[serde(rename = "CommonPrefixes", default)]
    common_prefixes: Vec<ListPrefix>,
    #[serde(rename = "NextContinuationToken")]
    next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
struct ListObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "LastModified")]
    last_modified: String,
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "ETag")]
    e_tag: Option<String>,
}

impl ListObject {
    fn into_meta(self, store_prefix: &str) -> Result<ObjectMeta> {
        let location = strip_prefix(store_prefix, &self.key)
            .ok_or_else(|| error_for("list response escaped store prefix", std::io::Error::other(self.key.clone())))?;
        Ok(ObjectMeta {
            location: Path::parse(location)?,
            last_modified: DateTime::parse_from_rfc3339(&self.last_modified)
                .map_err(|error| error_for("invalid list LastModified", error))?
                .with_timezone(&Utc),
            size: self.size,
            e_tag: self.e_tag,
            version: None,
        })
    }
}

#[derive(Deserialize)]
struct ListPrefix {
    #[serde(rename = "Prefix")]
    prefix: String,
}

struct ListPage {
    objects: Vec<ObjectMeta>,
    common_prefixes: Vec<Path>,
    next_token: Option<String>,
}

fn join_prefix(store_prefix: &str, path: Option<&str>) -> String {
    match path {
        Some(path) if !path.is_empty() => format!("{store_prefix}{path}"),
        _ => store_prefix.to_owned(),
    }
}

fn strip_prefix<'a>(store_prefix: &str, path: &'a str) -> Option<&'a str> {
    path.strip_prefix(store_prefix)
}


impl Display for RemoteSignedS3Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE}({})", self.base_url)
    }
}

#[async_trait]
impl ObjectStore for RemoteSignedS3Store {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        if !opts.tags.is_empty() || !opts.attributes.is_empty() {
            return Err(object_store::Error::NotSupported {
                source: "object attributes and tags are not supported by RemoteSignedS3Store".into(),
            });
        }
        let body = Bytes::from(payload.into_iter().flatten().collect::<Vec<_>>());
        let is_create = matches!(opts.mode, PutMode::Create);
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_LENGTH, HeaderValue::from_str(&body.len().to_string()).expect("valid content length"));
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => { headers.insert("if-none-match", HeaderValue::from_static("*")); }
            PutMode::Update(version) => {
                if let Some(e_tag) = version.e_tag {
                    headers.insert("if-match", HeaderValue::from_str(&e_tag).map_err(|error| error_for("invalid ETag", error))?);
                }
            }
        }
        let response = self
            .request(Method::PUT, location, headers, Some(body))
            .await
            .map_err(|error| precondition_as_already_exists(is_create, location, error))?;
        let e_tag = response.headers().get(ETAG).and_then(|value| value.to_str().ok()).map(str::to_owned);
        let version = response.headers().get("x-amz-version-id").and_then(|value| value.to_str().ok()).map(str::to_owned);
        Ok(PutResult { e_tag, version, extensions: Default::default() })
    }

    async fn put_multipart_opts(&self, _: &Path, _: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        Err(not_implemented("put_multipart_opts"))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut headers = HeaderMap::new();
        if let Some(range) = &options.range {
            headers.insert(RANGE, HeaderValue::from_str(&range.to_string()).expect("valid range header"));
        }
        if let Some(value) = &options.if_match {
            headers.insert("if-match", HeaderValue::from_str(value).map_err(|error| error_for("invalid If-Match", error))?);
        }
        if let Some(value) = &options.if_none_match {
            headers.insert("if-none-match", HeaderValue::from_str(value).map_err(|error| error_for("invalid If-None-Match", error))?);
        }
        let method = if options.head { Method::HEAD } else { Method::GET };
        let response = self.request(method, location, headers, None).await?;
        let status = response.status();
        let headers = response.headers().clone();
        let body = if options.head { Bytes::new() } else { response.bytes().await.map_err(|error| error_for("failed to read response", error))? };
        let mut meta = object_meta(location, &headers, body.len() as u64)?;
        if let Some((_, size)) = content_range(&headers) {
            meta.size = size;
        }
        options.check_preconditions(&meta)?;
        let range = response_range(&headers, status, &meta, options.range.as_ref())?;
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: Attributes::new(),
            extensions: Default::default(),
        })
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let store = Arc::new(self.clone());
        locations.then(move |location| {
            let store = Arc::clone(&store);
            async move {
                let location = location?;
                store.request(Method::DELETE, &location, HeaderMap::new(), None).await?;
                Ok(location)
            }
        }).boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            store
                .list_all(prefix.as_ref())
                .await
                .map(|objects| stream::iter(objects.into_iter().map(Ok)))
        })
            .try_flatten()
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut token = None;
        loop {
            let page = self.list_page(prefix, Some("/"), token.as_deref()).await?;
            objects.extend(page.objects);
            common_prefixes.extend(page.common_prefixes);
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(ListResult { common_prefixes, objects, extensions: Default::default() })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let mut headers = HeaderMap::new();
        let source = self.object_url(from)?;
        headers.insert("x-amz-copy-source", HeaderValue::from_str(source.path()).map_err(|error| error_for("invalid copy source", error))?);
        let is_create = options.mode == CopyMode::Create;
        if is_create {
            headers.insert("if-none-match", HeaderValue::from_static("*"));
        }
        self.request(Method::PUT, to, headers, None)
            .await
            .map_err(|error| precondition_as_already_exists(is_create, to, error))?;
        Ok(())
    }
}

/// A failed conditional-create (`If-None-Match: *`) returns HTTP 412, but semantically
/// means the object already exists. Map it to `AlreadyExists` so callers (e.g. Zarr's
/// `set_if_not_exists`) that catch that case behave as they do with `S3Store`.
fn precondition_as_already_exists(
    is_create: bool,
    location: &Path,
    error: object_store::Error,
) -> object_store::Error {
    match error {
        object_store::Error::Precondition { source, .. } if is_create => {
            object_store::Error::AlreadyExists { path: location.to_string(), source }
        }
        other => other,
    }
}

fn error_for(message: &str, source: impl std::error::Error + Send + Sync + 'static) -> object_store::Error {
    object_store::Error::Generic { store: STORE, source: format!("{message}: {source}").into() }
}

fn not_implemented(operation: &str) -> object_store::Error {
    object_store::Error::NotImplemented { operation: operation.into(), implementer: STORE.into() }
}

fn check_status(location: &Path, status: StatusCode) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    let source = format!("HTTP {status}").into();
    Err(match status {
        StatusCode::NOT_FOUND => object_store::Error::NotFound { path: location.to_string(), source },
        StatusCode::UNAUTHORIZED => object_store::Error::Unauthenticated { path: location.to_string(), source },
        StatusCode::FORBIDDEN => object_store::Error::PermissionDenied { path: location.to_string(), source },
        StatusCode::PRECONDITION_FAILED => object_store::Error::Precondition { path: location.to_string(), source },
        StatusCode::NOT_MODIFIED => object_store::Error::NotModified { path: location.to_string(), source },
        _ => object_store::Error::Generic { store: STORE, source },
    })
}

fn object_meta(location: &Path, headers: &HeaderMap, fallback_size: u64) -> Result<ObjectMeta> {
    let size = headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .map(str::parse)
        .transpose()
        .map_err(|error| error_for("invalid Content-Length", error))?
        .unwrap_or(fallback_size);
    let last_modified = headers
        .get(LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(DateTime::parse_from_rfc2822)
        .transpose()
        .map_err(|error| error_for("invalid Last-Modified", error))?
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let e_tag = headers.get(ETAG).and_then(|value| value.to_str().ok()).map(str::to_owned);
    Ok(ObjectMeta { location: location.clone(), last_modified, size, e_tag, version: None })
}

fn content_range(headers: &HeaderMap) -> Option<(std::ops::Range<u64>, u64)> {
    let value = headers.get(CONTENT_RANGE)?.to_str().ok()?.strip_prefix("bytes ")?;
    let (range, size) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((start.parse().ok()?..end.parse::<u64>().ok()?.checked_add(1)?, size.parse().ok()?))
}

fn response_range(
    headers: &HeaderMap,
    status: StatusCode,
    meta: &ObjectMeta,
    requested: Option<&object_store::GetRange>,
) -> Result<std::ops::Range<u64>> {
    if status == StatusCode::PARTIAL_CONTENT {
        return content_range(headers)
            .map(|(range, _)| range)
            .ok_or_else(|| error_for("invalid Content-Range", std::io::Error::other("missing or malformed header")));
    }
    if requested.is_some() {
        return Err(error_for("range request did not return 206", std::io::Error::other("unexpected response status")));
    }
    Ok(0..meta.size)
}

#[derive(Debug, Clone)]
#[pyclass(name = "RemoteSignedS3Store", frozen, subclass, from_py_object)]
/// Python-native wrapper for [`RemoteSignedS3Store`].
pub struct PyRemoteSignedS3Store {
    store: Arc<RemoteSignedS3Store>,
}

impl PyRemoteSignedS3Store {
    /// Return the native store.
    pub fn as_ref(&self) -> &Arc<RemoteSignedS3Store> {
        &self.store
    }
}

#[pymethods]
impl PyRemoteSignedS3Store {
    #[new]
    #[pyo3(signature = (url, signer))]
    fn new(url: String, signer: PySigner) -> PyResult<Self> {
        let mut url = Url::parse(&url).map_err(|error| PyValueError::new_err(error.to_string()))?;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }
        let mut segments = url.path_segments().ok_or_else(|| {
            PyValueError::new_err("RemoteSignedS3Store URL must include a bucket")
        })?;
        let bucket = segments.next().filter(|bucket| !bucket.is_empty()).ok_or_else(|| {
            PyValueError::new_err("RemoteSignedS3Store URL must include a bucket")
        })?;
        let prefix = segments.filter(|segment| !segment.is_empty()).collect::<Vec<_>>().join("/");
        let prefix = if prefix.is_empty() { prefix } else { format!("{prefix}/") };
        let mut bucket_url = url.clone();
        bucket_url.set_path(&format!("/{bucket}/"));
        Ok(Self {
            store: Arc::new(RemoteSignedS3Store {
                base_url: url,
                bucket_url,
                prefix,
                signer,
                client: reqwest::Client::new(),
            }),
        })
    }

    fn __repr__(&self) -> String {
        format!("RemoteSignedS3Store(\"{}\")", self.store.base_url)
    }
}
