use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt, TryStreamExt};
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH,
    IF_UNMODIFIED_SINCE, LAST_MODIFIED, RANGE,
};
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequestBody, HttpResponse,
    ReqwestConnector,
};
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, GetOptions, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, RetryConfig, TagSet,
    UploadPart,
};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple, PyType};
use pyo3::{intern, IntoPyObjectExt};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::PyObjectStoreResult;
use crate::prefix::MaybePrefixedStore;
use crate::retry::PyRetryConfig;
use crate::{PyClientOptions, PyUrl};

const STORE: &str = "RemoteSignedS3";

/// The `x-amz-*` headers this store sets or reads itself.
const COPY_SOURCE: &str = "x-amz-copy-source";
const STORAGE_CLASS: &str = "x-amz-storage-class";
const TAGGING: &str = "x-amz-tagging";
const USER_METADATA_PREFIX: &str = "x-amz-meta-";
const VERSION_ID: &str = "x-amz-version-id";

/// The HTTP date format used by the `If-[Un]Modified-Since` headers.
const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

/// SigV4 requires the unreserved characters of RFC 3986 to be left alone and everything else to be
/// percent-encoded. `/` is excluded because it separates key segments.
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
const KEY_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'/');

/// A Python callable that signs a single S3 request.
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

impl<'py> IntoPyObject<'py> for PySigner {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(self.0.into_bound(py))
    }
}

/// A signer callback may be a plain function or a coroutine function.
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
    /// Hand `(method, uri, headers)` to the callback and return the signed `(uri, headers)`.
    ///
    /// The returned headers *replace* the headers passed in, rather than being merged into them.
    /// This is what the S3 remote-signing contract specifies: a signer computes a signature over a
    /// specific set of headers, so silently adding to or keeping headers it did not return would
    /// invalidate that signature.
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
        let uri = Url::parse(&result.0)
            .map_err(|error| error_for("signer returned invalid URI", error))?;
        let headers = result
            .1
            .into_iter()
            .map(|(name, value)| {
                Ok((
                    HeaderName::from_bytes(name.as_bytes())
                        .map_err(|error| error_for("signer returned invalid header name", error))?,
                    HeaderValue::from_str(&value).map_err(|error| {
                        error_for("signer returned invalid header value", error)
                    })?,
                ))
            })
            .collect::<Result<HeaderMap>>()?;
        Ok((uri, headers))
    }
}

/// An S3-compatible object store that obtains a signature for every HTTP request from Python.
///
/// This addresses the bucket root and takes whole keys. Any key prefix is applied by wrapping
/// it in a [`MaybePrefixedStore`], exactly as [`PyS3Store`](super::PyS3Store) does.
#[derive(Debug, Clone)]
pub struct RemoteSignedS3Store {
    /// The bucket root that keys are resolved against. Always ends in `/`.
    bucket_url: Url,
    /// The bucket name. Needed by `x-amz-copy-source`, which always names the bucket even
    /// when the request URL does not.
    bucket: String,
    signer: PySigner,
    client: HttpClient,
    retry: RetryConfig,
}

/// One S3 REST request, before signing.
///
/// Held rather than dispatched directly so that [`RemoteSignedS3Store::send`] can re-sign and
/// resend it on retry. Signatures are short-lived, so a retry must never reuse the previous
/// signature.
struct SignedRequest {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: PutPayload,
    /// Whether this request carries `If-None-Match: *`, in which case a rejected precondition
    /// means "the object already exists" rather than a generic failure.
    conditional_create: bool,
}

impl SignedRequest {
    fn new(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: PutPayload::default(),
            conditional_create: false,
        }
    }

    fn header(mut self, name: impl Into<HeaderName>, value: HeaderValue) -> Self {
        self.headers.insert(name.into(), value);
        self
    }

    fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers.extend(headers);
        self
    }

    fn body(mut self, body: PutPayload) -> Self {
        self.body = body;
        self
    }

    fn conditional_create(mut self) -> Self {
        self.headers
            .insert(IF_NONE_MATCH, HeaderValue::from_static("*"));
        self.conditional_create = true;
        self
    }
}

impl RemoteSignedS3Store {
    /// The URL of `location`, with the key percent-encoded as SigV4 requires.
    ///
    /// `Url::join` cannot be used here: it would interpret `?` and `#` in a key as the start of a
    /// query or fragment, silently addressing a different object.
    fn object_url(&self, location: &Path) -> Result<Url> {
        self.bucket_url
            .join(&encode_key(location))
            .map_err(|error| error_for("invalid object URL", error))
    }

    fn object_request(&self, method: Method, location: &Path) -> Result<SignedRequest> {
        Ok(SignedRequest::new(method, self.object_url(location)?))
    }

    /// Sign and dispatch `request`, retrying transient failures.
    ///
    /// Each attempt is signed afresh, so a retry never depends on the lifetime of an earlier
    /// signature.
    async fn send(&self, location: &Path, request: SignedRequest) -> Result<HttpResponse> {
        let deadline = Instant::now() + self.retry.retry_timeout;
        let mut backoff = self.retry.backoff.init_backoff;
        let mut attempts = 0;
        loop {
            let (uri, headers) = self
                .signer
                .sign(&request.method, &request.url, &request.headers)
                .await?;
            let mut builder = http::Request::builder()
                .method(request.method.clone())
                .uri(uri.as_str());
            if let Some(target) = builder.headers_mut() {
                *target = headers;
            }
            let http_request = builder
                .body(HttpRequestBody::from(request.body.clone()))
                .map_err(|error| error_for("invalid HTTP request", error))?;

            let result = self.client.execute(http_request).await;
            let retryable = match &result {
                Err(error) => is_retryable_error(error),
                Ok(response) => is_retryable_status(response.status()),
            };
            if !retryable || attempts >= self.retry.max_retries || Instant::now() >= deadline {
                let response = result.map_err(|error| error_for("HTTP request failed", error))?;
                return check_response(location, request.conditional_create, response).await;
            }

            tokio::time::sleep(backoff).await;
            backoff = Duration::min(
                backoff.mul_f64(self.retry.backoff.base),
                self.retry.backoff.max_backoff,
            );
            attempts += 1;
        }
    }

    async fn list_page(
        &self,
        prefix: Option<&Path>,
        delimiter: Option<&str>,
        offset: Option<&Path>,
        token: Option<&str>,
    ) -> Result<ListPage> {
        let mut url = self.bucket_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("list-type", "2");
            if let Some(prefix) = format_prefix(prefix) {
                query.append_pair("prefix", &prefix);
            }
            // Ask S3 to percent-encode keys so that control characters, which are legal in a key
            // but not in XML, survive the response.
            query.append_pair("encoding-type", "url");
            if let Some(delimiter) = delimiter {
                query.append_pair("delimiter", delimiter);
            }
            if let Some(offset) = offset {
                query.append_pair("start-after", offset.as_ref());
            }
            if let Some(token) = token {
                query.append_pair("continuation-token", token);
            }
        }
        let response = self
            .send(&Path::default(), SignedRequest::new(Method::GET, url))
            .await?;
        let body = response
            .into_body()
            .bytes()
            .await
            .map_err(|error| error_for("failed to read list response", error))?;
        let response: ListObjectsV2 = quick_xml::de::from_reader(body.as_ref())
            .map_err(|error| error_for("invalid ListObjectsV2 response", error))?;
        let objects = response
            .contents
            .into_iter()
            .map(ListObject::into_meta)
            .collect::<Result<Vec<_>>>()?;
        let common_prefixes = response
            .common_prefixes
            .into_iter()
            .map(|prefix| Path::parse(decode_key(&prefix.prefix)))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(ListPage {
            objects,
            common_prefixes,
            next_token: response.next_continuation_token,
        })
    }

    /// Stream every object under `prefix`, fetching each `ListObjectsV2` page only once the
    /// previous page has been consumed.
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        offset: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        let offset = offset.cloned();
        stream::try_unfold(Some(None::<String>), move |token| {
            let store = store.clone();
            let prefix = prefix.clone();
            let offset = offset.clone();
            async move {
                // `None` marks the stream as exhausted; `Some(None)` is the first request.
                let Some(token) = token else {
                    return Ok::<_, object_store::Error>(None);
                };
                let page = store
                    .list_page(prefix.as_ref(), None, offset.as_ref(), token.as_deref())
                    .await?;
                let next = page.next_token.map(Some);
                Ok(Some((stream::iter(page.objects.into_iter().map(Ok)), next)))
            }
        })
        .try_flatten()
        .boxed()
    }

    async fn create_multipart(&self, location: &Path, opts: PutMultipartOptions) -> Result<String> {
        let PutMultipartOptions {
            tags,
            attributes,
            extensions: _,
        } = opts;
        let mut url = self.object_url(location)?;
        url.set_query(Some("uploads="));
        let request = SignedRequest::new(Method::POST, url)
            .header(CONTENT_LENGTH, HeaderValue::from_static("0"))
            .headers(attribute_headers(&attributes)?)
            .headers(tag_headers(&tags)?);
        let body = self
            .send(location, request)
            .await?
            .into_body()
            .bytes()
            .await
            .map_err(|error| error_for("failed to read CreateMultipartUpload response", error))?;
        let response: InitiateMultipartUploadResult = quick_xml::de::from_reader(body.as_ref())
            .map_err(|error| error_for("invalid CreateMultipartUpload response", error))?;
        Ok(response.upload_id)
    }
}

/// The shared state of an in-flight multipart upload.
///
/// `MultipartUpload::put_part` must return a `'static` future so that parts can be uploaded
/// concurrently, which is why the store, upload id and completed parts are all held behind an
/// `Arc`. Part numbers are assigned when `put_part` is called, not when its future resolves, so
/// parts stay correctly ordered however they interleave.
#[derive(Debug)]
struct MultipartState {
    store: Arc<RemoteSignedS3Store>,
    location: Path,
    upload_id: String,
    parts: Mutex<Vec<Option<PartId>>>,
}

impl MultipartState {
    /// Upload a single part and record its ETag against `part_idx`.
    async fn put_part(self: Arc<Self>, part_idx: usize, data: PutPayload) -> Result<()> {
        let part = self.upload_part(part_idx, data).await?;
        let mut parts = self.parts.lock().expect("multipart state poisoned");
        if parts.len() <= part_idx {
            parts.resize(part_idx + 1, None);
        }
        parts[part_idx] = Some(part);
        Ok(())
    }

    async fn upload_part(&self, part_idx: usize, data: PutPayload) -> Result<PartId> {
        let mut url = self.store.object_url(&self.location)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("partNumber", &(part_idx + 1).to_string());
            query.append_pair("uploadId", &self.upload_id);
        }
        let request = SignedRequest::new(Method::PUT, url)
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&data.content_length().to_string())
                    .expect("content length is a valid header value"),
            )
            .body(data);
        let response = self.store.send(&self.location, request).await?;
        let e_tag = header_str(response.headers(), &ETAG).ok_or_else(|| {
            error_for(
                "UploadPart response is missing an ETag",
                std::io::Error::other(format!("part {}", part_idx + 1)),
            )
        })?;
        Ok(PartId { content_id: e_tag })
    }

    /// The ETags of every part, in order, erroring if `put_part` was not awaited for some part.
    fn finished_parts(&self, expected: usize) -> Result<Vec<PartId>> {
        let parts = self.parts.lock().expect("multipart state poisoned");
        (0..expected)
            .map(|part_idx| {
                parts.get(part_idx).and_then(Clone::clone).ok_or_else(|| {
                    error_for(
                        "multipart upload completed before all parts finished uploading",
                        std::io::Error::other(format!("part {} is missing", part_idx + 1)),
                    )
                })
            })
            .collect()
    }
}

/// A multipart upload in which every request — initiation, each part, completion and abort — is
/// independently signed by the Python callback.
#[derive(Debug)]
struct RemoteSignedMultipartUpload {
    state: Arc<MultipartState>,
    part_idx: usize,
}

#[async_trait]
impl MultipartUpload for RemoteSignedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part_idx = self.part_idx;
        self.part_idx += 1;
        let state = Arc::clone(&self.state);
        state.put_part(part_idx, data).boxed()
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let mut parts = self.state.finished_parts(self.part_idx)?;
        if parts.is_empty() {
            // S3 rejects a completion with no parts, so an empty object still needs one part.
            parts.push(self.state.upload_part(0, PutPayload::default()).await?);
            self.part_idx = 1;
        }
        let body = quick_xml::se::to_string(&CompleteMultipartUpload::from(parts))
            .map_err(|error| error_for("failed to encode CompleteMultipartUpload", error))?;

        let mut url = self.state.store.object_url(&self.state.location)?;
        url.set_query(Some(&format!("uploadId={}", self.state.upload_id)));
        let request = SignedRequest::new(Method::POST, url)
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&body.len().to_string())
                    .expect("content length is a valid header value"),
            )
            .body(PutPayload::from(body));

        let response = self.state.store.send(&self.state.location, request).await?;
        let version = header_str(response.headers(), &HeaderName::from_static(VERSION_ID));
        let body =
            response.into_body().bytes().await.map_err(|error| {
                error_for("failed to read CompleteMultipartUpload response", error)
            })?;
        // S3 can report a failed completion with a 200 status and an `<Error>` body, so the body
        // has to be inspected rather than trusting the status alone.
        if let Some(detail) = s3_error_detail(&body) {
            return Err(object_store::Error::Generic {
                store: STORE,
                source: format!("CompleteMultipartUpload failed: {detail}").into(),
            });
        }
        let response: CompleteMultipartUploadResult = quick_xml::de::from_reader(body.as_ref())
            .map_err(|error| error_for("invalid CompleteMultipartUpload response", error))?;
        Ok(PutResult {
            e_tag: Some(response.e_tag),
            version,
            extensions: Default::default(),
        })
    }

    async fn abort(&mut self) -> Result<()> {
        let mut url = self.state.store.object_url(&self.state.location)?;
        url.set_query(Some(&format!("uploadId={}", self.state.upload_id)));
        self.state
            .store
            .send(
                &self.state.location,
                SignedRequest::new(Method::DELETE, url),
            )
            .await?;
        Ok(())
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
    fn into_meta(self) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            location: Path::parse(decode_key(&self.key))?,
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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InitiateMultipartUploadResult {
    upload_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUpload {
    part: Vec<MultipartPart>,
}

impl From<Vec<PartId>> for CompleteMultipartUpload {
    fn from(value: Vec<PartId>) -> Self {
        Self {
            part: value
                .into_iter()
                .enumerate()
                .map(|(part_idx, part)| MultipartPart {
                    e_tag: part.content_id,
                    part_number: part_idx + 1,
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct MultipartPart {
    #[serde(rename = "ETag")]
    e_tag: String,
    #[serde(rename = "PartNumber")]
    part_number: usize,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CompleteMultipartUploadResult {
    #[serde(rename = "ETag")]
    e_tag: String,
}

/// The `<Error>` document S3 returns for a failed request.
#[derive(Deserialize)]
#[serde(rename = "Error", rename_all = "PascalCase")]
struct S3Error {
    code: Option<String>,
    message: Option<String>,
}

/// Percent-encode a key for use in a request path, as SigV4 requires.
fn encode_key(location: &Path) -> String {
    utf8_percent_encode(location.as_ref(), &KEY_ENCODE_SET).to_string()
}

/// The `prefix` query parameter for a `ListObjectsV2` request.
///
/// A trailing delimiter is appended so that the prefix matches on segment boundaries: without
/// it, listing `a/b` would also return `a/bc/d`. This mirrors what `object_store`'s own list
/// clients do.
fn format_prefix(prefix: Option<&Path>) -> Option<String> {
    prefix
        .filter(|prefix| !prefix.as_ref().is_empty())
        .map(|prefix| format!("{}/", prefix.as_ref()))
}

/// Undo the `encoding-type=url` encoding of a listed key, leaving it unchanged if it is not valid
/// percent-encoded UTF-8.
fn decode_key(key: &str) -> String {
    percent_decode_str(key)
        .decode_utf8()
        .map(Cow::into_owned)
        .unwrap_or_else(|_| key.to_owned())
}

impl Display for RemoteSignedS3Store {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{STORE}({})", self.bucket_url)
    }
}

#[async_trait]
impl ObjectStore for RemoteSignedS3Store {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let mut request = self
            .object_request(Method::PUT, location)?
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&payload.content_length().to_string())
                    .expect("content length is a valid header value"),
            )
            .headers(attribute_headers(&opts.attributes)?)
            .headers(tag_headers(&opts.tags)?)
            .body(payload);
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => request = request.conditional_create(),
            PutMode::Update(version) => {
                let e_tag = version
                    .e_tag
                    .ok_or_else(|| object_store::Error::NotSupported {
                        source: "RemoteSignedS3Store requires an ETag to update a specific version"
                            .into(),
                    })?;
                request = request.header(
                    IF_MATCH,
                    HeaderValue::from_str(&e_tag)
                        .map_err(|error| error_for("invalid ETag", error))?,
                );
            }
        }
        let response = self.send(location, request).await?;
        Ok(PutResult {
            e_tag: header_str(response.headers(), &ETAG),
            version: header_str(response.headers(), &HeaderName::from_static(VERSION_ID)),
            extensions: Default::default(),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let upload_id = self.create_multipart(location, opts).await?;
        Ok(Box::new(RemoteSignedMultipartUpload {
            state: Arc::new(MultipartState {
                store: Arc::new(self.clone()),
                location: location.clone(),
                upload_id,
                parts: Mutex::new(Vec::new()),
            }),
            part_idx: 0,
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let mut url = self.object_url(location)?;
        if let Some(version) = &options.version {
            url.query_pairs_mut().append_pair("versionId", version);
        }
        let mut request = SignedRequest::new(
            if options.head {
                Method::HEAD
            } else {
                Method::GET
            },
            url,
        );
        if let Some(range) = &options.range {
            request = request.header(
                RANGE,
                HeaderValue::from_str(&range.to_string()).expect("range is a valid header value"),
            );
        }
        for (name, value) in [
            (IF_MATCH, options.if_match.clone()),
            (IF_NONE_MATCH, options.if_none_match.clone()),
            (
                IF_MODIFIED_SINCE,
                options
                    .if_modified_since
                    .map(|date| date.format(HTTP_DATE_FORMAT).to_string()),
            ),
            (
                IF_UNMODIFIED_SINCE,
                options
                    .if_unmodified_since
                    .map(|date| date.format(HTTP_DATE_FORMAT).to_string()),
            ),
        ] {
            if let Some(value) = value {
                request = request.header(
                    name.clone(),
                    HeaderValue::from_str(&value)
                        .map_err(|error| error_for(name.as_str(), error))?,
                );
            }
        }

        let response = self.send(location, request).await?;
        let status = response.status();
        let headers = response.headers().clone();

        let mut meta = object_meta(location, &headers, 0)?;
        if let Some((_, size)) = content_range(&headers) {
            meta.size = size;
        }
        options.check_preconditions(&meta)?;
        let range = response_range(&headers, status, &meta, options.range.as_ref())?;
        let attributes = response_attributes(&headers);

        // Stream the body rather than collecting it, so that reading a large object does not
        // require buffering the whole object in memory.
        let payload = if options.head {
            GetResultPayload::Stream(stream::empty().boxed())
        } else {
            GetResultPayload::Stream(
                response
                    .into_body()
                    .bytes_stream()
                    .map_err(|error| error_for("failed to read response body", error))
                    .boxed(),
            )
        };
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes,
            extensions: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = Arc::new(self.clone());
        locations
            .map(move |location| {
                let store = Arc::clone(&store);
                async move {
                    let location = location?;
                    let request = store.object_request(Method::DELETE, &location)?;
                    store.send(&location, request).await?;
                    Ok(location)
                }
            })
            // S3 has no signable bulk-delete (its `POST ?delete` needs a body checksum the signer
            // never sees), so delete concurrently instead, as the S3 store does.
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_paginated(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.list_paginated(prefix, Some(offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut objects = Vec::new();
        let mut common_prefixes = Vec::new();
        let mut token = None;
        loop {
            let page = self
                .list_page(prefix, Some("/"), None, token.as_deref())
                .await?;
            objects.extend(page.objects);
            common_prefixes.extend(page.common_prefixes);
            match page.next_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(ListResult {
            common_prefixes,
            objects,
            extensions: Default::default(),
        })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        // `x-amz-copy-source` is `/<bucket>/<key>` regardless of addressing style, so it is
        // built from the bucket name rather than from the request URL's path.
        let source = format!("/{}/{}", self.bucket, encode_key(from));
        let mut request = self.object_request(Method::PUT, to)?.header(
            HeaderName::from_static(COPY_SOURCE),
            HeaderValue::from_str(&source)
                .map_err(|error| error_for("invalid copy source", error))?,
        );
        if options.mode == CopyMode::Create {
            request = request.conditional_create();
        }
        self.send(to, request).await?;
        Ok(())
    }
}

/// Map the `Attributes` obstore accepts onto the S3 headers that carry them.
fn attribute_headers(attributes: &Attributes) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    for (attribute, value) in attributes.iter() {
        let name = match attribute {
            Attribute::CacheControl => CACHE_CONTROL,
            Attribute::ContentDisposition => CONTENT_DISPOSITION,
            Attribute::ContentEncoding => CONTENT_ENCODING,
            Attribute::ContentLanguage => CONTENT_LANGUAGE,
            Attribute::ContentType => CONTENT_TYPE,
            Attribute::StorageClass => HeaderName::from_static(STORAGE_CLASS),
            Attribute::Metadata(key) => {
                HeaderName::from_bytes(format!("{USER_METADATA_PREFIX}{key}").as_bytes())
                    .map_err(|error| error_for("invalid metadata key", error))?
            }
            // `Attribute` is `#[non_exhaustive]`, so a future variant must be rejected rather
            // than silently dropped: callers set attributes expecting them to be stored.
            other => {
                return Err(object_store::Error::NotSupported {
                    source: format!("attribute {other:?} is not supported by {STORE}").into(),
                })
            }
        };
        headers.insert(
            name,
            HeaderValue::from_str(value.as_ref())
                .map_err(|error| error_for("invalid attribute value", error))?,
        );
    }
    Ok(headers)
}

fn tag_headers(tags: &TagSet) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    if !tags.is_empty() {
        headers.insert(
            HeaderName::from_static(TAGGING),
            HeaderValue::from_str(tags.encoded())
                .map_err(|error| error_for("invalid tag set", error))?,
        );
    }
    Ok(headers)
}

/// Recover the attributes of an object from its response headers.
fn response_attributes(headers: &HeaderMap) -> Attributes {
    let mut attributes = Attributes::new();
    for (attribute, name) in [
        (Attribute::CacheControl, CACHE_CONTROL),
        (Attribute::ContentDisposition, CONTENT_DISPOSITION),
        (Attribute::ContentEncoding, CONTENT_ENCODING),
        (Attribute::ContentLanguage, CONTENT_LANGUAGE),
        (Attribute::ContentType, CONTENT_TYPE),
        (
            Attribute::StorageClass,
            HeaderName::from_static(STORAGE_CLASS),
        ),
    ] {
        if let Some(value) = header_str(headers, &name) {
            attributes.insert(attribute, AttributeValue::from(value));
        }
    }
    for (name, value) in headers.iter() {
        if let Some(key) = name.as_str().strip_prefix(USER_METADATA_PREFIX) {
            if let Ok(value) = value.to_str() {
                attributes.insert(
                    Attribute::Metadata(key.to_owned().into()),
                    AttributeValue::from(value.to_owned()),
                );
            }
        }
    }
    attributes
}

fn header_str(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn error_for(
    message: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: format!("{message}: {source}").into(),
    }
}

/// Whether a transport-level failure is worth retrying.
fn is_retryable_error(error: &HttpError) -> bool {
    matches!(
        error.kind(),
        HttpErrorKind::Connect
            | HttpErrorKind::Request
            | HttpErrorKind::Timeout
            | HttpErrorKind::Interrupted
    )
}

/// Whether a response status is worth retrying.
///
/// Every request this store makes is idempotent, so a retry cannot duplicate an effect. The worst
/// case is completing an upload twice, which S3 tolerates.
fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
        )
}

/// Turn a non-2xx response into the matching `object_store` error, including whatever detail S3
/// put in the response body.
async fn check_response(
    location: &Path,
    conditional_create: bool,
    response: HttpResponse,
) -> Result<HttpResponse> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.into_body().bytes().await.unwrap_or_default();
    let source: Box<dyn std::error::Error + Send + Sync> = match s3_error_detail(&body) {
        Some(detail) => format!("HTTP {status}: {detail}").into(),
        None => format!("HTTP {status}").into(),
    };
    let path = location.to_string();
    // A conditional create that lost the race reports a rejected precondition, but callers such
    // as Zarr's `set_if_not_exists` expect `AlreadyExists`. S3 uses 412; some implementations
    // report the same race as 409.
    if conditional_create
        && matches!(
            status,
            StatusCode::PRECONDITION_FAILED | StatusCode::CONFLICT
        )
    {
        return Err(object_store::Error::AlreadyExists { path, source });
    }
    Err(match status {
        StatusCode::NOT_FOUND => object_store::Error::NotFound { path, source },
        StatusCode::UNAUTHORIZED => object_store::Error::Unauthenticated { path, source },
        StatusCode::FORBIDDEN => object_store::Error::PermissionDenied { path, source },
        StatusCode::PRECONDITION_FAILED => object_store::Error::Precondition { path, source },
        StatusCode::NOT_MODIFIED => object_store::Error::NotModified { path, source },
        _ => object_store::Error::Generic {
            store: STORE,
            source,
        },
    })
}

/// The `Code: Message` of an S3 `<Error>` body, if `body` is one.
fn s3_error_detail(body: &[u8]) -> Option<String> {
    let error: S3Error = quick_xml::de::from_reader(body).ok()?;
    match (error.code, error.message) {
        (Some(code), Some(message)) => Some(format!("{code}: {message}")),
        (Some(detail), None) | (None, Some(detail)) => Some(detail),
        (None, None) => None,
    }
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
    Ok(ObjectMeta {
        location: location.clone(),
        last_modified,
        size,
        e_tag: header_str(headers, &ETAG),
        version: header_str(headers, &HeaderName::from_static(VERSION_ID)),
    })
}

/// The `(range, total size)` of a `Content-Range: bytes <start>-<end>/<size>` header.
fn content_range(headers: &HeaderMap) -> Option<(Range<u64>, u64)> {
    let value = headers
        .get(CONTENT_RANGE)?
        .to_str()
        .ok()?
        .strip_prefix("bytes ")?;
    let (range, size) = value.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((
        start.parse().ok()?..end.parse::<u64>().ok()?.checked_add(1)?,
        size.parse().ok()?,
    ))
}

fn response_range(
    headers: &HeaderMap,
    status: StatusCode,
    meta: &ObjectMeta,
    requested: Option<&object_store::GetRange>,
) -> Result<Range<u64>> {
    if status == StatusCode::PARTIAL_CONTENT {
        return content_range(headers)
            .map(|(range, _)| range)
            .ok_or_else(|| {
                error_for(
                    "invalid Content-Range",
                    std::io::Error::other("missing or malformed header"),
                )
            });
    }
    if requested.is_some() {
        return Err(error_for(
            "range request did not return 206",
            std::io::Error::other("unexpected response status"),
        ));
    }
    Ok(0..meta.size)
}

/// Resolve an `s3://bucket/prefix` location against an S3 endpoint into the HTTPS URL that
/// [`PyRemoteSignedS3Store::new`] takes.
fn s3_endpoint_url(
    url: &Url,
    endpoint: &Url,
    virtual_hosted_style_request: bool,
) -> PyResult<String> {
    if !matches!(url.scheme(), "s3" | "s3a") {
        return Err(PyValueError::new_err(format!(
            "Expected an s3:// or s3a:// URL, got {}. Pass an HTTPS endpoint URL to \
             RemoteSignedS3Store() directly instead.",
            url.scheme(),
        )));
    }
    let bucket = url
        .host_str()
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| PyValueError::new_err(format!("{url} does not name a bucket")))?;
    let prefix = url
        .path_segments()
        .into_iter()
        .flatten()
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");

    if !matches!(endpoint.scheme(), "http" | "https") {
        return Err(PyValueError::new_err(format!(
            "endpoint must be an http:// or https:// URL, got {endpoint}",
        )));
    }
    // An endpoint served below a path would make the split between endpoint, bucket and key
    // prefix ambiguous, so require a bare origin and let the caller assemble the URL itself.
    if !matches!(endpoint.path(), "" | "/") {
        return Err(PyValueError::new_err(format!(
            "endpoint must not include a path, got {}. Build the full URL yourself and pass \
             it to RemoteSignedS3Store() instead.",
            endpoint.path(),
        )));
    }

    let mut resolved = endpoint.clone();
    resolved.set_query(None);
    resolved.set_fragment(None);
    if virtual_hosted_style_request {
        let host = endpoint.host_str().ok_or_else(|| {
            PyValueError::new_err(format!("endpoint {endpoint} does not include a host"))
        })?;
        resolved
            .set_host(Some(&format!("{bucket}.{host}")))
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        resolved.set_path(&format!("/{prefix}"));
    } else {
        resolved.set_path(&format!("/{bucket}/{prefix}"));
    }
    Ok(resolved.into())
}

/// The constructor arguments of a [`PyRemoteSignedS3Store`], retained for pickling.
#[derive(Debug, Clone)]
struct RemoteSignedS3Config {
    url: String,
    signer: PySigner,
    virtual_hosted_style_request: bool,
    client_options: Option<PyClientOptions>,
    retry_config: Option<PyRetryConfig>,
    /// The bucket named by `url`, and the key prefix it implies. Derived rather than passed,
    /// but kept here so the Python getters can report them.
    bucket: String,
    prefix: Option<Path>,
}

impl RemoteSignedS3Config {
    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let args =
            PyTuple::new(py, [self.url.clone().into_bound_py_any(py)?])?.into_bound_py_any(py)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item(intern!(py, "signer"), self.signer.clone())?;
        if self.virtual_hosted_style_request {
            kwargs.set_item(intern!(py, "virtual_hosted_style_request"), true)?;
        }
        if let Some(client_options) = &self.client_options {
            kwargs.set_item(intern!(py, "client_options"), client_options.clone())?;
        }
        if let Some(retry_config) = &self.retry_config {
            kwargs.set_item(intern!(py, "retry_config"), retry_config.clone())?;
        }
        PyTuple::new(py, [args, kwargs.into_bound_py_any(py)?])
    }

    /// Whether two stores were built from equivalent arguments.
    ///
    /// The signer is compared by identity: two distinct callables cannot be shown to sign
    /// equivalently.
    fn eq(&self, other: &Self, py: Python<'_>) -> bool {
        self.url == other.url
            && self.virtual_hosted_style_request == other.virtual_hosted_style_request
            && self.client_options == other.client_options
            && self.retry_config == other.retry_config
            && self.signer.0.bind(py).is(other.signer.0.bind(py))
    }
}

/// Python-native wrapper for [`RemoteSignedS3Store`].
#[derive(Debug, Clone)]
#[pyclass(name = "RemoteSignedS3Store", frozen, subclass, from_py_object)]
pub struct PyRemoteSignedS3Store {
    store: Arc<MaybePrefixedStore<RemoteSignedS3Store>>,
    /// The arguments used for pickling. This must stay in sync with the underlying store.
    config: RemoteSignedS3Config,
}

impl AsRef<Arc<MaybePrefixedStore<RemoteSignedS3Store>>> for PyRemoteSignedS3Store {
    fn as_ref(&self) -> &Arc<MaybePrefixedStore<RemoteSignedS3Store>> {
        &self.store
    }
}

impl PyRemoteSignedS3Store {
    /// Consume self and return the underlying [`RemoteSignedS3Store`].
    pub fn into_inner(self) -> Arc<MaybePrefixedStore<RemoteSignedS3Store>> {
        self.store
    }
}

#[pymethods]
impl PyRemoteSignedS3Store {
    #[new]
    #[pyo3(signature = (url, signer, *, virtual_hosted_style_request=false, client_options=None, retry_config=None))]
    fn new(
        url: String,
        signer: PySigner,
        virtual_hosted_style_request: bool,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Self> {
        let parsed = Url::parse(&url).map_err(|error| PyValueError::new_err(error.to_string()))?;
        let mut segments = parsed
            .path_segments()
            .ok_or_else(|| PyValueError::new_err(format!("{STORE} URL must include a path")))?
            .filter(|segment| !segment.is_empty());

        // In path style the first segment names the bucket and the rest is the key prefix; in
        // virtual-hosted style the bucket is the host's first label, so every segment is prefix.
        let mut bucket_url = parsed.clone();
        bucket_url.set_query(None);
        bucket_url.set_fragment(None);
        let (bucket, prefix) = if virtual_hosted_style_request {
            let host = parsed.host_str().ok_or_else(|| {
                PyValueError::new_err(format!(
                    "{STORE} URL must include a host naming the bucket when \
                     virtual_hosted_style_request is set"
                ))
            })?;
            let bucket = host.split('.').next().unwrap_or(host).to_owned();
            bucket_url.set_path("/");
            (bucket, segments.collect::<Vec<_>>().join("/"))
        } else {
            let bucket = segments
                .next()
                .ok_or_else(|| PyValueError::new_err(format!("{STORE} URL must include a bucket")))?
                .to_owned();
            bucket_url.set_path(&format!("/{bucket}/"));
            (bucket, segments.collect::<Vec<_>>().join("/"))
        };
        // An empty prefix stays `None`, so that `MaybePrefixedStore` passes paths straight
        // through instead of rewriting them.
        let prefix = if prefix.is_empty() {
            None
        } else {
            Some(Path::parse(prefix).map_err(|error| PyValueError::new_err(error.to_string()))?)
        };

        let options = client_options.clone().map(Into::into).unwrap_or_default();
        let client = ReqwestConnector {}.connect(&options)?;
        let store = RemoteSignedS3Store {
            bucket_url,
            bucket: bucket.clone(),
            signer: signer.clone(),
            client,
            retry: retry_config.clone().map(Into::into).unwrap_or_default(),
        };
        Ok(Self {
            store: Arc::new(MaybePrefixedStore::new(store, prefix.clone())),
            config: RemoteSignedS3Config {
                url,
                signer,
                virtual_hosted_style_request,
                client_options,
                retry_config,
                bucket,
                prefix,
            },
        })
    }

    /// Construct a store from an `s3://` location plus the S3 endpoint that serves it.
    ///
    /// Catalogs hand out locations as `s3://bucket/key` and the endpoint separately, so this
    /// saves callers assembling the HTTPS URL themselves.
    #[classmethod]
    #[pyo3(signature = (url, signer, *, endpoint, virtual_hosted_style_request=false, client_options=None, retry_config=None))]
    fn from_s3_url<'py>(
        cls: &Bound<'py, PyType>,
        url: PyUrl,
        signer: PySigner,
        endpoint: PyUrl,
        virtual_hosted_style_request: bool,
        client_options: Option<PyClientOptions>,
        retry_config: Option<PyRetryConfig>,
    ) -> PyObjectStoreResult<Bound<'py, PyAny>> {
        let url = s3_endpoint_url(
            url.as_ref(),
            endpoint.as_ref(),
            virtual_hosted_style_request,
        )?;

        // Note: we pass **back** through Python so that if cls is a subclass, we instantiate the
        // subclass
        let kwargs = PyDict::new(cls.py());
        kwargs.set_item(intern!(cls.py(), "signer"), signer)?;
        kwargs.set_item(
            intern!(cls.py(), "virtual_hosted_style_request"),
            virtual_hosted_style_request,
        )?;
        kwargs.set_item(intern!(cls.py(), "client_options"), client_options)?;
        kwargs.set_item(intern!(cls.py(), "retry_config"), retry_config)?;
        Ok(cls.call((url,), Some(&kwargs))?)
    }

    fn __eq__(&self, other: &Bound<PyAny>) -> bool {
        // Ensure we never error on __eq__ by returning false if the other object is not the same
        // type
        other
            .cast::<PyRemoteSignedS3Store>()
            .map(|other| self.config.eq(&other.get().config, other.py()))
            .unwrap_or(false)
    }

    fn __getnewargs_ex__<'py>(&'py self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        self.config.__getnewargs_ex__(py)
    }

    fn __repr__(&self) -> String {
        format!("RemoteSignedS3Store(\"{}\")", self.config.url)
    }

    #[getter]
    fn url(&self) -> &str {
        &self.config.url
    }

    #[getter]
    fn bucket(&self) -> &str {
        &self.config.bucket
    }

    #[getter]
    fn prefix(&self) -> Option<&str> {
        self.config.prefix.as_ref().map(Path::as_ref)
    }

    #[getter]
    fn signer(&self) -> PySigner {
        self.config.signer.clone()
    }

    #[getter]
    fn virtual_hosted_style_request(&self) -> bool {
        self.config.virtual_hosted_style_request
    }

    #[getter]
    fn client_options(&self) -> Option<PyClientOptions> {
        self.config.client_options.clone()
    }

    #[getter]
    fn retry_config(&self) -> Option<PyRetryConfig> {
        self.config.retry_config.clone()
    }
}
