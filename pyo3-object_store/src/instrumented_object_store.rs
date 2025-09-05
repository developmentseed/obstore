// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

// This is vendored from instrumented_object_store
// https://github.com/datafusion-contrib/datafusion-tracing/blob/8fc214b192ad67114742f8a582292bc06e2b5247/instrumented-object-store/src/instrumented_object_store.rs

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::{
    path::Path, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
};
use std::fmt::{Display, Formatter};
use std::ops::Range;
use tracing::{instrument, Span};
use tracing_futures::Instrument;

/// A wrapper around an `ObjectStore` that instruments all public methods with tracing.
#[derive(Clone, Debug)]
pub struct InstrumentedObjectStore<T: ObjectStore> {
    inner: T,
    name: String,
}

impl<T: ObjectStore> InstrumentedObjectStore<T> {
    /// Creates a new `InstrumentedObjectStore` wrapping the provided `ObjectStore`.
    ///
    /// # Arguments
    ///
    /// * `store` - An `Arc`-wrapped `dyn ObjectStore` to be instrumented.
    pub fn new(store: T, name: &str) -> Self {
        Self {
            inner: store,
            name: name.to_owned(),
        }
    }

    /// Returns a reference to the inner `ObjectStore`.
    pub fn inner(&self) -> &T {
        &self.inner
    }
}

impl<T: ObjectStore> AsRef<T> for InstrumentedObjectStore<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

trait Instrumentable {
    fn record_fields(&self, _: &Span);
}

impl Instrumentable for () {
    fn record_fields(&self, _: &Span) {}
}

impl Instrumentable for ObjectMeta {
    fn record_fields(&self, span: &Span) {
        span.record("object_store.result.meta", format!("{self:?}"));
    }
}

impl Instrumentable for GetResult {
    fn record_fields(&self, span: &Span) {
        self.meta.record_fields(span);
        span.record("object_store.result.range", format!("{:?}", self.range));
    }
}

impl Instrumentable for PutResult {
    fn record_fields(&self, span: &Span) {
        span.record("object_store.result.e_tag", format!("{:?}", self.e_tag));
        span.record("object_store.result.version", format!("{:?}", self.version));
    }
}

impl Instrumentable for ListResult {
    fn record_fields(&self, span: &Span) {
        span.record(
            "object_store.result.object_count",
            format!("{:?}", self.objects.len()),
        );
    }
}

impl Instrumentable for Bytes {
    fn record_fields(&self, span: &Span) {
        span.record("object_store.result.content_length", self.len());
    }
}

impl Instrumentable for Vec<Bytes> {
    fn record_fields(&self, span: &Span) {
        span.record(
            "object_store.result.content_length",
            self.iter().map(|b| b.len()).sum::<usize>(),
        );
    }
}

fn instrument_result<T, E>(result: Result<T, E>) -> Result<T, E>
where
    T: Instrumentable,
    E: std::error::Error,
{
    let span = Span::current();
    match &result {
        Ok(value) => value.record_fields(&span),
        Err(e) => {
            span.record("object_store.result.err", format!("{e}"));
        }
    }
    result
}

impl<T: ObjectStore> Display for InstrumentedObjectStore<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

#[async_trait]
impl<T: ObjectStore> ObjectStore for InstrumentedObjectStore<T> {
    /// Save the provided bytes to the specified location with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.put", self.name),
            object_store.location = %location,
            object_store.content_length = %payload.content_length(),
            object_store.result.err = tracing::field::Empty,
            object_store.result.e_tag = tracing::field::Empty,
            object_store.result.version = tracing::field::Empty,
        )
    )]
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        instrument_result(self.inner.put(location, payload).await)
    }

    /// Save the provided payload to location with the given options and tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.put_opts", self.name),
            object_store.location = %location,
            object_store.content_length = %payload.content_length(),
            object_store.result.err = tracing::field::Empty,
            object_store.result.e_tag = tracing::field::Empty,
            object_store.result.version = tracing::field::Empty,
        )
    )]
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        instrument_result(self.inner.put_opts(location, payload, opts).await)
    }

    /// Perform a multipart upload with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.put_multipart", self.name),
            object_store.location = %location,
        )
    )]
    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let result = self.inner.put_multipart(location).await?;
        Ok(Box::new(InstrumentedMultiPartUpload::new(
            result, &self.name,
        )))
    }

    /// Perform a multipart upload with options and tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.put_multipart_opts", self.name),
            object_store.location = %location,
            object_store.options = ?opts,
        )
    )]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let result = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(InstrumentedMultiPartUpload::new(
            result, &self.name,
        )))
    }

    /// Return the bytes that are stored at the specified location with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.get", self.name),
            object_store.location = %location,
            object_store.result.err = tracing::field::Empty,
            object_store.result.meta = tracing::field::Empty,
            object_store.result.range = tracing::field::Empty,
        )
    )]
    async fn get(&self, location: &Path) -> Result<GetResult> {
        instrument_result(self.inner.get(location).await)
    }

    /// Perform a get request with options and tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.get_opts", self.name),
            object_store.location = %location,
            object_store.options = ?options,
            object_store.result.err = tracing::field::Empty,
            object_store.result.meta = tracing::field::Empty,
            object_store.result.range = tracing::field::Empty,
        )
    )]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        instrument_result(self.inner.get_opts(location, options).await)
    }

    /// Return the bytes that are stored at the specified location in the given byte range with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.get_range", self.name),
            object_store.location = %location,
            object_store.range = ?range,
            object_store.result.err = tracing::field::Empty,
            object_store.result.content_length = tracing::field::Empty,
        )
    )]
    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        instrument_result(self.inner.get_range(location, range).await)
    }

    /// Return the bytes that are stored at the specified location in the given byte ranges with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.get_ranges", self.name),
            object_store.location = %location,
            object_store.ranges = ?ranges,
            object_store.result.err = tracing::field::Empty,
            object_store.result.content_length = tracing::field::Empty,
        )
    )]
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        instrument_result(self.inner.get_ranges(location, ranges).await)
    }

    /// Return the metadata for the specified location with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.head", self.name),
            object_store.location = %location,
            object_store.result.err = tracing::field::Empty,
            object_store.result.meta = tracing::field::Empty,
        )
    )]
    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        instrument_result(self.inner.head(location).await)
    }

    /// Delete the object at the specified location with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.delete", self.name),
            object_store.location = %location,
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn delete(&self, location: &Path) -> Result<()> {
        instrument_result(self.inner.delete(location).await)
    }

    /// Delete all the objects at the specified locations with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.delete", self.name),
        )
    )]
    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner
            .delete_stream(locations)
            .in_current_span()
            .boxed()
    }

    /// List all the objects with the given prefix with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.list", self.name),
            object_store.prefix = %prefix.unwrap_or(&Path::default()),
        )
    )]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix).in_current_span().boxed()
    }

    /// List all the objects with the given prefix and offset with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.list_with_offset", self.name),
            object_store.prefix = %prefix.unwrap_or(&Path::default()),
            object_store.offset = %offset,
        )
    )]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner
            .list_with_offset(prefix, offset)
            .in_current_span()
            .boxed()
    }

    /// List objects with the given prefix and delimiter with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.list_with_delimiter", self.name),
            object_store.prefix = %prefix.unwrap_or(&Path::default()),
            object_store.result.err = tracing::field::Empty,
            object_store.result.object_count = tracing::field::Empty,
        )
    )]
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        instrument_result(self.inner.list_with_delimiter(prefix).await)
    }

    /// Copy an object from one path to another with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.copy", self.name),
            object_store.from = %from,
            object_store.to = %to,
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        instrument_result(self.inner.copy(from, to).await)
    }

    /// Move an object from one path to another with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.rename", self.name),
            object_store.from = %from,
            object_store.to = %to,
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        instrument_result(self.inner.rename(from, to).await)
    }

    /// Copy an object only if the destination does not exist with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.copy_if_not_exists", self.name),
            object_store.from = %from,
            object_store.to = %to,
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        instrument_result(self.inner.copy_if_not_exists(from, to).await)
    }

    /// Move an object only if the destination does not exist with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.rename_if_not_exists", self.name),
            object_store.from = %from,
            object_store.to = %to,
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        instrument_result(self.inner.rename_if_not_exists(from, to).await)
    }
}

/// A wrapper around an `ObjectStore` that instruments all public methods with tracing.
#[derive(Debug)]
struct InstrumentedMultiPartUpload {
    inner: Box<dyn MultipartUpload>,
    name: String,
}

impl InstrumentedMultiPartUpload {
    /// Creates a new `InstrumentedMultiPartUpload` wrapping the provided `MultipartUpload`.
    ///
    /// # Arguments
    ///
    /// * `upload` - A boxed `dyn MultipartUpload` to be instrumented.
    fn new(upload: Box<dyn MultipartUpload>, name: &str) -> Self {
        Self {
            inner: upload,
            name: name.to_owned(),
        }
    }
}

#[async_trait]
impl MultipartUpload for InstrumentedMultiPartUpload {
    /// Upload a part without tracing (too many parts to trace).
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    /// Complete the multipart upload with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.complete", self.name),
            object_store.result.err = tracing::field::Empty,
            object_store.result.e_tag = tracing::field::Empty,
            object_store.result.version = tracing::field::Empty,
        )
    )]
    async fn complete(&mut self) -> Result<PutResult> {
        instrument_result(self.inner.complete().await)
    }

    /// Abort the multipart upload with tracing.
    #[instrument(
        skip_all,
        fields(
            otel.name = format!("{}.abort", self.name),
            object_store.result.err = tracing::field::Empty,
        )
    )]
    async fn abort(&mut self) -> Result<()> {
        instrument_result(self.inner.abort().await)
    }
}
