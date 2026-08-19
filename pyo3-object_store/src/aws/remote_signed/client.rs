//! Turning an operation into a signed S3 REST request, and dispatching it with retries.

use std::time::{Duration, Instant};

use http::header::IF_NONE_MATCH;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use object_store::client::{HttpError, HttpErrorKind, HttpRequestBody, HttpResponse};
use object_store::path::Path;
use object_store::{PutPayload, Result};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use serde::Deserialize;
use url::Url;

use super::{error_for, RemoteSignedS3Store, STORE};

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

/// One S3 REST request, before signing.
///
/// Held rather than dispatched directly so that [`RemoteSignedS3Store::send`] can re-sign and
/// resend it on retry. Signatures are short-lived, so a retry must never reuse the previous
/// signature.
pub(super) struct SignedRequest {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: PutPayload,
    /// Whether this request carries `If-None-Match: *`, in which case a rejected precondition
    /// means "the object already exists" rather than a generic failure.
    conditional_create: bool,
}

impl SignedRequest {
    pub(super) fn new(method: Method, url: Url) -> Self {
        Self {
            method,
            url,
            headers: HeaderMap::new(),
            body: PutPayload::default(),
            conditional_create: false,
        }
    }

    pub(super) fn header(mut self, name: impl Into<HeaderName>, value: HeaderValue) -> Self {
        self.headers.insert(name.into(), value);
        self
    }

    pub(super) fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers.extend(headers);
        self
    }

    pub(super) fn body(mut self, body: PutPayload) -> Self {
        self.body = body;
        self
    }

    pub(super) fn conditional_create(mut self) -> Self {
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
    pub(super) fn object_url(&self, location: &Path) -> Result<Url> {
        self.bucket_url
            .join(&encode_key(location))
            .map_err(|error| error_for("invalid object URL", error))
    }

    pub(super) fn object_request(&self, method: Method, location: &Path) -> Result<SignedRequest> {
        Ok(SignedRequest::new(method, self.object_url(location)?))
    }

    /// Sign and dispatch `request`, retrying transient failures.
    ///
    /// Each attempt is signed afresh, so a retry never depends on the lifetime of an earlier
    /// signature.
    pub(super) async fn send(
        &self,
        location: &Path,
        request: SignedRequest,
    ) -> Result<HttpResponse> {
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
}

/// Percent-encode a key for use in a request path, as SigV4 requires.
pub(super) fn encode_key(location: &Path) -> String {
    utf8_percent_encode(location.as_ref(), &KEY_ENCODE_SET).to_string()
}

/// The `<Error>` document S3 returns for a failed request.
#[derive(Deserialize)]
#[serde(rename = "Error", rename_all = "PascalCase")]
struct S3Error {
    code: Option<String>,
    message: Option<String>,
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
/// Retrying cannot corrupt an object: writing the same key or part twice is idempotent, and this
/// matches what `object_store`'s own S3 client retries.
///
/// Two cases are lossy rather than harmful, and are accepted for the same reason `object_store`
/// accepts them. If a `CompleteMultipartUpload` succeeds but its response is lost, the retry sees
/// `NoSuchUpload` and reports `NotFound` even though the object exists. If a conditional create
/// (`If-None-Match: *`) succeeds but its response is lost, the retry sees a rejected precondition
/// and reports `AlreadyExists`, as though another writer had won the race.
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
pub(super) fn s3_error_detail(body: &[u8]) -> Option<String> {
    let error: S3Error = quick_xml::de::from_reader(body).ok()?;
    match (error.code, error.message) {
        (Some(code), Some(message)) => Some(format!("{code}: {message}")),
        (Some(detail), None) | (None, Some(detail)) => Some(detail),
        (None, None) => None,
    }
}
