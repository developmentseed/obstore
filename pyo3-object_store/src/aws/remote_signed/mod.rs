//! An S3-compatible store that has every request signed by a Python callback.
//!
//! The work is split so each concern stays readable on its own:
//!
//! - [`signer`] is the Python boundary,
//! - [`client`] builds, signs, dispatches and retries one request,
//! - [`list`], [`multipart`] and [`headers`] cover the S3 protocol details,
//! - [`store`] is the Python-facing class,
//! - and this module holds the store type and its [`ObjectStore`] implementation.

mod client;
mod headers;
mod list;
mod multipart;
mod signer;
mod store;

use std::fmt::{Display, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use http::header::{
    CONTENT_LENGTH, ETAG, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
};
use http::{HeaderName, HeaderValue, Method};
use object_store::client::HttpClient;
use object_store::path::Path;
use object_store::{
    CopyMode, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result, RetryConfig,
};
use url::Url;

pub use store::PyRemoteSignedS3Store;

use client::{encode_key, SignedRequest};
use headers::{
    attribute_headers, content_range, header_str, object_meta, response_attributes, response_range,
    tag_headers, HTTP_DATE_FORMAT, VERSION_ID,
};
use multipart::{MultipartState, RemoteSignedMultipartUpload};
use signer::PySigner;

const STORE: &str = "RemoteSignedS3";

/// The `x-amz-copy-source` header, which always names the bucket.
const COPY_SOURCE: &str = "x-amz-copy-source";

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

fn error_for(
    message: &str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> object_store::Error {
    object_store::Error::Generic {
        store: STORE,
        source: format!("{message}: {source}").into(),
    }
}
