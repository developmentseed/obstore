//! Translating between S3 headers and the `object_store` types that carry them.

use std::ops::Range;

use chrono::{DateTime, Utc};
use http::header::{
    CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_LENGTH,
    CONTENT_RANGE, CONTENT_TYPE, ETAG, LAST_MODIFIED,
};
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use object_store::path::Path;
use object_store::{Attribute, AttributeValue, Attributes, ObjectMeta, Result, TagSet};

use super::{error_for, STORE};

/// The `x-amz-*` headers this store sets or reads itself.
pub(super) const STORAGE_CLASS: &str = "x-amz-storage-class";
pub(super) const TAGGING: &str = "x-amz-tagging";
pub(super) const USER_METADATA_PREFIX: &str = "x-amz-meta-";
pub(super) const VERSION_ID: &str = "x-amz-version-id";

/// The HTTP date format used by the `If-[Un]Modified-Since` headers.
pub(super) const HTTP_DATE_FORMAT: &str = "%a, %d %b %Y %H:%M:%S GMT";

/// Map the `Attributes` obstore accepts onto the S3 headers that carry them.
pub(super) fn attribute_headers(attributes: &Attributes) -> Result<HeaderMap> {
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

pub(super) fn tag_headers(tags: &TagSet) -> Result<HeaderMap> {
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
pub(super) fn response_attributes(headers: &HeaderMap) -> Attributes {
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

pub(super) fn header_str(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

pub(super) fn object_meta(
    location: &Path,
    headers: &HeaderMap,
    fallback_size: u64,
) -> Result<ObjectMeta> {
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
pub(super) fn content_range(headers: &HeaderMap) -> Option<(Range<u64>, u64)> {
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

pub(super) fn response_range(
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
