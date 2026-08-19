//! `ListObjectsV2` requests, their XML response, and paging through them.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use http::Method;
use object_store::path::Path;
use object_store::{ObjectMeta, Result};
use percent_encoding::percent_decode_str;
use serde::Deserialize;

use super::client::SignedRequest;
use super::{error_for, RemoteSignedS3Store};

impl RemoteSignedS3Store {
    pub(super) async fn list_page(
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
    pub(super) fn list_paginated(
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

pub(super) struct ListPage {
    pub(super) objects: Vec<ObjectMeta>,
    pub(super) common_prefixes: Vec<Path>,
    pub(super) next_token: Option<String>,
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

/// Build a key prefix out of the path segments of a store URL.
///
/// A `Url`'s segments are percent-encoded, but an `object_store::Path` holds a *decoded* key and
/// [`encode_key`] re-encodes it on the way out. Without decoding here, a URL ending in `caf%C3%A9`
/// would produce the literal seven-character prefix `caf%C3%A9`, be encoded again as
/// `caf%25C3%25A9`, and address the wrong object.
pub(super) fn decode_prefix<'a>(segments: impl Iterator<Item = &'a str>) -> String {
    segments.map(decode_key).collect::<Vec<_>>().join("/")
}

/// Undo the `encoding-type=url` encoding of a listed key, leaving it unchanged if it is not valid
/// percent-encoded UTF-8.
fn decode_key(key: &str) -> String {
    percent_decode_str(key)
        .decode_utf8()
        .map(Cow::into_owned)
        .unwrap_or_else(|_| key.to_owned())
}
