//! Multipart upload: initiation, parts, completion and abort, each signed independently.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::FutureExt;
use http::header::{CONTENT_LENGTH, ETAG};
use http::{HeaderName, HeaderValue, Method};
use object_store::multipart::PartId;
use object_store::path::Path;
use object_store::{
    MultipartUpload, PutMultipartOptions, PutPayload, PutResult, Result, UploadPart,
};
use serde::{Deserialize, Serialize};

use super::client::{s3_error_detail, SignedRequest};
use super::headers::{header_str, VERSION_ID};
use super::{attribute_headers, error_for, tag_headers, RemoteSignedS3Store, STORE};

impl RemoteSignedS3Store {
    pub(super) async fn create_multipart(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<String> {
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
pub(super) struct MultipartState {
    pub(super) store: Arc<RemoteSignedS3Store>,
    pub(super) location: Path,
    pub(super) upload_id: String,
    pub(super) parts: Mutex<Vec<Option<PartId>>>,
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
pub(super) struct RemoteSignedMultipartUpload {
    pub(super) state: Arc<MultipartState>,
    pub(super) part_idx: usize,
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
        url.query_pairs_mut()
            .append_pair("uploadId", &self.state.upload_id);
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
        url.query_pairs_mut()
            .append_pair("uploadId", &self.state.upload_id);
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
