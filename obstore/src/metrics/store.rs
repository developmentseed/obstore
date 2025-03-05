//! A metric instrumentation wrapper over [`ObjectStore`] implementations.

#![allow(clippy::clone_on_ref_ptr)]

use super::multipart_upload_metrics::MultipartUploadWrapper;
use object_store::{
    GetOptions, GetResultPayload, MultipartUpload, PutMultipartOpts, PutOptions, PutPayload,
    PutResult,
};
use pyo3_object_store::PyObjectStore;

use std::sync::Arc;
use std::{borrow::Cow, ops::Range};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use super::iox_time::{Time, TimeProvider};
use super::metric::{DurationHistogram, Metric, U64Counter};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};

use crate::metrics::metric;
use object_store::{path::Path, GetResult, ListResult, ObjectMeta, ObjectStore, Result};
use tokio::{sync::Mutex, task::JoinSet};

use pyo3::prelude::*;

/// A typed name of a scope / type to report the metrics under.
#[derive(Debug, Clone)]
pub struct StoreType(Cow<'static, str>);

impl<T> From<T> for StoreType
where
    T: Into<Cow<'static, str>>,
{
    fn from(v: T) -> Self {
        Self(v.into())
    }
}

#[derive(Debug, Clone)]
struct Metrics {
    success_duration: DurationHistogram,
    error_duration: DurationHistogram,
}

impl Metrics {
    fn new(registry: &metric::Registry, store_type: &StoreType, op: &'static str) -> Self {
        // Call durations broken down by op & result
        let duration: Metric<DurationHistogram> = registry.register_metric(
            "object_store_op_duration",
            "object store operation duration",
        );

        Self {
            success_duration: duration.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("success")),
            ]),
            error_duration: duration.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("error")),
            ]),
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool) {
        // Avoid exploding if time goes backwards - simply drop the measurement
        // if it happens.
        let Some(delta) = t_end.checked_duration_since(t_begin) else {
            return;
        };

        if success {
            self.success_duration.record(delta);
        } else {
            self.error_duration.record(delta);
        }
    }
}

#[derive(Debug, Clone)]
struct MetricsWithBytes {
    inner: Metrics,
    success_bytes: U64Counter,
    error_bytes: U64Counter,
}

impl MetricsWithBytes {
    fn new(registry: &metric::Registry, store_type: &StoreType, op: &'static str) -> Self {
        // Byte counts up/down
        let bytes = registry.register_metric::<U64Counter>(
            "object_store_transfer_bytes",
            "cumulative count of file content bytes transferred to/from the object store",
        );

        Self {
            inner: Metrics::new(registry, store_type, op),
            success_bytes: bytes.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("success")),
            ]),
            error_bytes: bytes.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("error")),
            ]),
        }
    }

    fn record_bytes_only(&self, success: bool, bytes: u64) {
        if success {
            self.success_bytes.inc(bytes);
        } else {
            self.error_bytes.inc(bytes);
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool, bytes: Option<u64>) {
        if let Some(bytes) = bytes {
            self.record_bytes_only(success, bytes);
        }

        self.inner.record(t_begin, t_end, success);
    }
}

#[derive(Debug, Clone)]
struct MetricsWithBytesAndTtfb {
    inner: MetricsWithBytes,
    success_duration: DurationHistogram,
    error_duration: DurationHistogram,
}

impl MetricsWithBytesAndTtfb {
    fn new(registry: &metric::Registry, store_type: &StoreType, op: &'static str) -> Self {
        // Call durations broken down by op & result
        let duration: Metric<DurationHistogram> = registry.register_metric(
            "object_store_op_ttfb",
            "Time to first byte for object store operation",
        );

        Self {
            inner: MetricsWithBytes::new(registry, store_type, op),
            success_duration: duration.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("success")),
            ]),
            error_duration: duration.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("error")),
            ]),
        }
    }

    fn record_bytes_only(&self, success: bool, bytes: u64) {
        self.inner.record_bytes_only(success, bytes);
    }

    fn record(
        &self,
        t_begin: Time,
        t_first_byte: Time,
        t_end: Time,
        success: bool,
        bytes: Option<u64>,
    ) {
        if let Some(delta) = t_first_byte.checked_duration_since(t_begin) {
            if success {
                self.success_duration.record(delta);
            } else {
                self.error_duration.record(delta);
            }
        }

        self.inner.record(t_begin, t_end, success, bytes);
    }
}

#[derive(Debug, Clone)]
struct MetricsWithCount {
    inner: Metrics,
    success_count: U64Counter,
    error_count: U64Counter,
}

impl MetricsWithCount {
    fn new(registry: &metric::Registry, store_type: &StoreType, op: &'static str) -> Self {
        let count = registry.register_metric::<U64Counter>(
            "object_store_transfer_objects",
            "cumulative count of objects transferred to/from the object store",
        );

        Self {
            inner: Metrics::new(registry, store_type, op),
            success_count: count.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("success")),
            ]),
            error_count: count.recorder([
                ("store_type", store_type.0.clone()),
                ("op", Cow::Borrowed(op)),
                ("result", Cow::Borrowed("error")),
            ]),
        }
    }

    fn record_count_only(&self, success: bool, count: u64) {
        if success {
            self.success_count.inc(count);
        } else {
            self.error_count.inc(count);
        }
    }

    fn record(&self, t_begin: Time, t_end: Time, success: bool, count: Option<u64>) {
        if let Some(count) = count {
            self.record_count_only(success, count);
        }

        self.inner.record(t_begin, t_end, success);
    }
}

/// An instrumentation decorator, wrapping an underlying [`ObjectStore`]
/// implementation and recording bytes transferred and call latency.
///
/// # Stream Duration
///
/// The [`ObjectStore::get()`] call can return a [`Stream`] which is polled
/// by the caller and may yield chunks of a file over a series of polls (as
/// opposed to all of the file data in one go). Because the caller drives the
/// polling and therefore fetching of data from the object store over the
/// lifetime of the [`Stream`], the duration of a [`ObjectStore::get()`]
/// request is measured to be the wall clock difference between the moment the
/// caller executes the [`ObjectStore::get()`] call, up until the last chunk
/// of data is yielded to the caller.
///
/// This means the duration metrics measuring consumption of returned streams
/// are recording the rate at which the application reads the data, as opposed
/// to the duration of time taken to fetch that data.
///
/// # Stream Errors
///
/// The [`ObjectStore::get()`] method can return a [`Stream`] of [`Result`]
/// instances, and returning an error when polled is not necessarily a terminal
/// state. The metric recorder allows for a caller to observe a transient error
/// and subsequently go on to complete reading the stream, recording this read
/// in the "success" histogram.
///
/// If a stream is not polled again after observing an error, the operation is
/// recorded in the "error" histogram.
///
/// A stream can return an arbitrary sequence of success and error states before
/// terminating, with the last observed poll result that yields a [`Result`]
/// dictating which histogram the operation is recorded in.
///
/// # Bytes Transferred
///
/// The metric recording bytes transferred accounts for only object data, and
/// not object metadata (such as that returned by list methods).
///
/// The total data transferred will be greater than the metric value due to
/// metadata queries, read errors, etc. The metric tracks the amount of object
/// data successfully yielded to the caller.
///
/// # Backwards Clocks
///
/// If the system clock is observed as moving backwards in time, call durations
/// are not recorded. The bytes transferred metric is not affected.
#[derive(Debug)]
pub struct ObjectStoreMetrics {
    inner: Arc<dyn ObjectStore>,
    time_provider: Arc<dyn TimeProvider>,

    put: MetricsWithBytes,
    put_multipart: Arc<MetricsWithBytes>,
    inprogress_multipart: Mutex<JoinSet<()>>,
    get: MetricsWithBytesAndTtfb,
    get_range: MetricsWithBytes,
    get_ranges: MetricsWithBytes,
    head: Metrics,
    delete: Metrics,
    delete_stream: MetricsWithCount,
    list: MetricsWithCount,
    list_with_offset: MetricsWithCount,
    list_with_delimiter: MetricsWithCount,
    copy: Metrics,
    rename: Metrics,
    copy_if_not_exists: Metrics,
    rename_if_not_exists: Metrics,
}

impl ObjectStoreMetrics {
    /// Instrument `T`, pushing to `registry`.
    pub fn new(
        inner: Arc<dyn ObjectStore>,
        time_provider: Arc<dyn TimeProvider>,
        store_type: impl Into<StoreType>,
        registry: &metric::Registry,
    ) -> Self {
        let store_type = store_type.into();

        Self {
            inner,
            time_provider,

            put: MetricsWithBytes::new(registry, &store_type, "put"),
            put_multipart: Arc::new(MetricsWithBytes::new(
                registry,
                &store_type,
                "put_multipart",
            )),
            inprogress_multipart: Default::default(),
            get: MetricsWithBytesAndTtfb::new(registry, &store_type, "get"),
            get_range: MetricsWithBytes::new(registry, &store_type, "get_range"),
            get_ranges: MetricsWithBytes::new(registry, &store_type, "get_ranges"),
            head: Metrics::new(registry, &store_type, "head"),
            delete: Metrics::new(registry, &store_type, "delete"),
            delete_stream: MetricsWithCount::new(registry, &store_type, "delete_stream"),
            list: MetricsWithCount::new(registry, &store_type, "list"),
            list_with_offset: MetricsWithCount::new(registry, &store_type, "list_with_offset"),
            list_with_delimiter: MetricsWithCount::new(
                registry,
                &store_type,
                "list_with_delimiter",
            ),
            copy: Metrics::new(registry, &store_type, "copy"),
            rename: Metrics::new(registry, &store_type, "rename"),
            copy_if_not_exists: Metrics::new(registry, &store_type, "copy_if_not_exists"),
            rename_if_not_exists: Metrics::new(registry, &store_type, "rename_if_not_exists"),
        }
    }

    #[cfg(test)]
    async fn close(&self) {
        let _ = self.inprogress_multipart.lock().await.join_next().await;
    }
}

impl std::fmt::Display for ObjectStoreMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStoreMetrics({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ObjectStoreMetrics {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let t = self.time_provider.now();
        let size = bytes.content_length();
        let res = self.inner.put_opts(location, bytes, opts).await;
        self.put
            .record(t, self.time_provider.now(), res.is_ok(), Some(size as _));
        res
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let t = self.time_provider.now();
        let inner = self.inner.put_multipart(location).await?;

        let (tx, rx) = futures::channel::oneshot::channel();
        let reporter = Arc::clone(&self.put_multipart);
        self.inprogress_multipart.lock().await.spawn(async move {
            if let Ok((res, bytes, t_end)) = rx.await {
                reporter.record(t, t_end, res, bytes);
            }
        });

        let multipart_upload =
            MultipartUploadWrapper::new(inner, Arc::clone(&self.time_provider), tx);
        Ok(Box::new(multipart_upload))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let started_at = self.time_provider.now();

        let res = self.inner.get_opts(location, options).await;

        match res {
            Ok(mut res) => {
                res.payload = match res.payload {
                    GetResultPayload::File(file, path) => {
                        let file = tokio::fs::File::from_std(file);
                        let size = file.metadata().await.ok().map(|m| m.len());
                        let file = file.into_std().await;

                        let end = self.time_provider.now();
                        self.get.record(
                            started_at,
                            // first byte wasn't really measured, so take "end" instead
                            end, end, true, size,
                        );
                        GetResultPayload::File(file, path)
                    }
                    GetResultPayload::Stream(s) => {
                        // Wrap the object store data stream in a decorator to track the
                        // yielded data / wall clock, inclusive of the inner call above.
                        GetResultPayload::Stream(Box::pin(Box::new(
                            StreamMetricRecorder::new(
                                s,
                                started_at,
                                BytesStreamDelegate::new(self.get.clone()),
                                Arc::clone(&self.time_provider),
                            )
                            .fuse(),
                        )))
                    }
                };
                Ok(res)
            }
            Err(e) => {
                let end = self.time_provider.now();
                self.get.record(started_at, end, end, false, None);
                Err(e)
            }
        }
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let t = self.time_provider.now();
        let res = self.inner.get_range(location, range).await;
        self.get_range.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref().ok().map(|b| b.len() as _),
        );
        res
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let t = self.time_provider.now();
        let res = self.inner.get_ranges(location, ranges).await;
        self.get_ranges.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref()
                .ok()
                .map(|b| b.iter().map(|b| b.len() as u64).sum()),
        );
        res
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let t = self.time_provider.now();
        let res = self.inner.head(location).await;
        self.head.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.delete(location).await;
        self.delete.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let started_at = self.time_provider.now();

        let s = self.inner.delete_stream(locations);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(
            s,
            started_at,
            CountStreamDelegate::new(self.delete_stream.clone()),
            Arc::clone(&self.time_provider),
        )
        .fuse()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let started_at = self.time_provider.now();

        let s = self.inner.list(prefix);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(
            s,
            started_at,
            CountStreamDelegate::new(self.list.clone()),
            Arc::clone(&self.time_provider),
        )
        .fuse()
        .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let started_at = self.time_provider.now();

        let s = self.inner.list_with_offset(prefix, offset);

        // Wrap the object store data stream in a decorator to track the
        // yielded data / wall clock, inclusive of the inner call above.
        StreamMetricRecorder::new(
            s,
            started_at,
            CountStreamDelegate::new(self.list_with_offset.clone()),
            Arc::clone(&self.time_provider),
        )
        .fuse()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let t = self.time_provider.now();
        let res = self.inner.list_with_delimiter(prefix).await;
        self.list_with_delimiter.record(
            t,
            self.time_provider.now(),
            res.is_ok(),
            res.as_ref().ok().map(|res| res.objects.len() as _),
        );
        res
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.copy(from, to).await;
        self.copy.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.rename(from, to).await;
        self.rename.record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.copy_if_not_exists(from, to).await;
        self.copy_if_not_exists
            .record(t, self.time_provider.now(), res.is_ok());
        res
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let t = self.time_provider.now();
        let res = self.inner.rename_if_not_exists(from, to).await;
        self.rename_if_not_exists
            .record(t, self.time_provider.now(), res.is_ok());
        res
    }
}

#[pyclass]
pub struct PyObjectStoreMetrics {
    store: ObjectStoreMetrics,
    metrics: Arc<metric::Registry>,
}

#[pymethods]
impl PyObjectStoreMetrics {
    #[new]
    fn new(store: PyObjectStore, name: String) -> PyResult<Self> {
        let metrics = Arc::new(metric::Registry::new());
        let store = ObjectStoreMetrics::new(
            store.into_inner(),
            Arc::new(super::iox_time::SystemProvider::new()),
            name,
            &metrics,
        );
        Ok(Self { store, metrics })
    }

    fn get(&self) {
        self.metrics.get_instrument("name");
    }
}

/// A [`MetricDelegate`] is called whenever the [`StreamMetricRecorder`]
/// observes an `Ok(Item)` in the stream.
trait MetricDelegate {
    /// The type this delegate observes.
    type Item;

    /// Invoked when the stream yields an `Ok(Item)`.
    fn observe_ok(&mut self, value: &Self::Item, t: Time);

    /// Finish stream.
    fn finish(&mut self, t_begin: Time, t_end: Time, success: bool);
}

/// A [`MetricDelegate`] for instrumented streams of [`Bytes`].
///
/// This impl is used to record the number of bytes yielded for
/// [`ObjectStore::get()`] calls.
#[derive(Debug)]
struct BytesStreamDelegate {
    metrics: MetricsWithBytesAndTtfb,
    first_byte: Option<Time>,
}

impl BytesStreamDelegate {
    fn new(metrics: MetricsWithBytesAndTtfb) -> Self {
        Self {
            metrics,
            first_byte: None,
        }
    }
}

impl MetricDelegate for BytesStreamDelegate {
    type Item = Bytes;

    fn observe_ok(&mut self, bytes: &Self::Item, t: Time) {
        if self.first_byte.is_none() {
            self.first_byte = Some(t);
        }

        self.metrics.record_bytes_only(true, bytes.len() as _);
    }

    fn finish(&mut self, t_begin: Time, t_end: Time, success: bool) {
        self.metrics.record(
            t_begin,
            self.first_byte.unwrap_or(t_end),
            t_end,
            success,
            None,
        );
    }
}

#[derive(Debug)]
struct CountStreamDelegate<T>(MetricsWithCount, PhantomData<T>);

impl<T> CountStreamDelegate<T> {
    fn new(metrics: MetricsWithCount) -> Self {
        Self(metrics, Default::default())
    }
}

impl<T> MetricDelegate for CountStreamDelegate<T> {
    type Item = T;

    fn observe_ok(&mut self, _value: &Self::Item, _t: Time) {
        self.0.record_count_only(true, 1);
    }

    fn finish(&mut self, t_begin: Time, t_end: Time, success: bool) {
        self.0.record(t_begin, t_end, success, None);
    }
}

/// [`StreamMetricRecorder`] decorates an underlying [`Stream`] for "get" /
/// "list" catalog operations, recording the wall clock duration and invoking
/// the metric delegate with the `Ok(T)` values.
///
/// For "gets" using the [`BytesStreamDelegate`], the bytes read counter is
/// incremented each time [`Self::poll_next()`] yields a buffer, and once the
/// [`StreamMetricRecorder`] is read to completion (specifically, until it
/// yields `Poll::Ready(None)`), or when it is dropped (whichever is sooner) the
/// decorator emits the wall clock measurement into the relevant histogram,
/// bucketed by operation result.
///
/// A stream may return a transient error when polled, and later successfully
/// emit all data in subsequent polls - therefore the duration is logged as an
/// error only if the last poll performed by the caller returned an error.
#[derive(Debug)]
#[pin_project(PinnedDrop)]
struct StreamMetricRecorder<S, D>
where
    D: MetricDelegate,
{
    #[pin]
    inner: S,

    time_provider: Arc<dyn TimeProvider>,

    // The timestamp at which the read request began, inclusive of the work
    // required to acquire the inner stream (which may involve fetching all the
    // data if the result is only pretending to be a stream).
    started_at: Time,
    // The time at which the last part of the data stream (or error) was
    // returned to the caller.
    //
    // The total get operation duration is calculated as this timestamp minus
    // the started_at timestamp.
    //
    // This field is always Some, until the end of the stream is observed at
    // which point the metrics are emitted and this field is set to None,
    // preventing the drop impl duplicating them.
    last_yielded_at: Option<Time>,
    // The error state of the last poll - true if OK, false if an error
    // occurred.
    //
    // This is used to select the correct success/error histogram which records
    // the operation duration.
    last_call_ok: bool,

    // Called when the stream yields an `Ok(T)` to allow the delegate to inspect
    // the `T`.
    metric_delegate: D,
}

impl<S, D> StreamMetricRecorder<S, D>
where
    S: Stream,
    D: MetricDelegate,
{
    fn new(
        stream: S,
        started_at: Time,
        metric_delegate: D,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        Self {
            inner: stream,

            // Set the last_yielded_at to now, ensuring the duration of work
            // already completed acquiring the steam is correctly recorded even
            // if the stream is never polled / data never read.
            last_yielded_at: Some(time_provider.now()),
            // Acquiring the stream was successful, even if the data was never
            // read.
            last_call_ok: true,

            started_at,
            time_provider,

            metric_delegate,
        }
    }
}

impl<S, T, D, E> Stream for StreamMetricRecorder<S, D>
where
    S: Stream<Item = Result<T, E>>,
    D: MetricDelegate<Item = T>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        let res = this.inner.poll_next(cx);

        match res {
            Poll::Ready(Some(Ok(value))) => {
                let now = this.time_provider.now();

                *this.last_call_ok = true;
                *this.last_yielded_at.as_mut().unwrap() = now;

                // Allow the pluggable metric delegate to record the value of T
                this.metric_delegate.observe_ok(&value, now);

                Poll::Ready(Some(Ok(value)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.last_call_ok = false;
                *this.last_yielded_at.as_mut().unwrap() = this.time_provider.now();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                // The stream has terminated - record the wall clock duration
                // immediately.
                this.metric_delegate.finish(
                    *this.started_at,
                    this.last_yielded_at
                        .take()
                        .expect("no last_yielded_at value for fused stream"),
                    *this.last_call_ok,
                );

                Poll::Ready(None)
            }
            v => v,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // Impl the default size_hint() so this wrapper doesn't mask the size
        // hint from the inner stream, if any.
        self.inner.size_hint()
    }
}

#[pinned_drop]
impl<S, D> PinnedDrop for StreamMetricRecorder<S, D>
where
    D: MetricDelegate,
{
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();

        // Only emit metrics if the end of the stream was not observed (and
        // therefore last_yielded_at is still Some).
        if let Some(last) = this.last_yielded_at {
            this.metric_delegate
                .finish(*this.started_at, *last, *this.last_call_ok);
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use std::future::ready;
//     use std::{
//         io::{Error, ErrorKind},
//         sync::Arc,
//         time::Duration,
//     };

//     use futures::{stream, FutureExt, TryStreamExt};
//     use iox_time::{MockProvider, SystemProvider};
//     use metric::Attributes;
//     use std::io::Read;
//     use std::sync::atomic::{AtomicBool, Ordering};

//     use dummy::DummyObjectStore;
//     use object_store::{local::LocalFileSystem, memory::InMemory, UploadPart};

//     use super::*;

//     #[track_caller]
//     fn assert_histogram_hit<const N: usize>(
//         metrics: &metric::Registry,
//         name: &'static str,
//         attr: [(&'static str, &'static str); N],
//     ) {
//         let histogram = metrics
//             .get_instrument::<Metric<DurationHistogram>>(name)
//             .expect("failed to read histogram")
//             .get_observer(&Attributes::from(&attr))
//             .expect("failed to get observer")
//             .fetch();

//         let hit_count = histogram.sample_count();
//         assert!(hit_count > 0, "metric {name} did not record any calls");
//     }

//     #[track_caller]
//     fn assert_histogram_not_hit<const N: usize>(
//         metrics: &metric::Registry,
//         name: &'static str,
//         attr: [(&'static str, &'static str); N],
//     ) {
//         let histogram = metrics
//             .get_instrument::<Metric<DurationHistogram>>(name)
//             .expect("failed to read histogram")
//             .get_observer(&Attributes::from(&attr))
//             .expect("failed to get observer")
//             .fetch();

//         let hit_count = histogram.sample_count();
//         assert!(hit_count == 0, "metric {name} did record {hit_count} calls");
//     }

//     #[track_caller]
//     fn assert_counter_value<const N: usize>(
//         metrics: &metric::Registry,
//         name: &'static str,
//         attr: [(&'static str, &'static str); N],
//         value: u64,
//     ) {
//         let count = metrics
//             .get_instrument::<Metric<U64Counter>>(name)
//             .expect("failed to read counter")
//             .get_observer(&Attributes::from(&attr))
//             .expect("failed to get observer")
//             .fetch();
//         assert_eq!(count, value);
//     }

//     #[tokio::test]
//     async fn test_put() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .put(
//                 &Path::from("test"),
//                 PutPayload::from([42_u8, 42, 42, 42, 42].as_slice()),
//             )
//             .await
//             .expect("put should succeed");

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put"),
//                 ("result", "success"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_put_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .put(
//                 &Path::from("test"),
//                 PutPayload::from([42_u8, 42, 42, 42, 42].as_slice()),
//             )
//             .await
//             .expect_err("put should error");

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put"),
//                 ("result", "error"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_put_multipart() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         let mut multipart_upload = store
//             .put_multipart(&Path::from("test"))
//             .await
//             .expect("should get multipart upload");
//         assert!(multipart_upload
//             .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
//             .await
//             .is_ok());
//         // demonstrate that it sums across bytes
//         assert!(multipart_upload
//             .put_part(PutPayload::from_static(&[42_u8, 42, 42]))
//             .await
//             .is_ok());
//         drop(multipart_upload);
//         store.close().await;

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "success"),
//             ],
//             8,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[derive(Debug)]
//     struct NopeMultipartUpload;

//     #[async_trait]
//     impl MultipartUpload for NopeMultipartUpload {
//         fn put_part(&mut self, _data: PutPayload) -> UploadPart {
//             async move { Err(object_store::Error::NotImplemented) }.boxed()
//         }

//         async fn complete(&mut self) -> Result<PutResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn abort(&mut self) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }
//     }

//     #[derive(Debug)]
//     struct NopeObjectStore;

//     impl std::fmt::Display for NopeObjectStore {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             write!(f, "NopeObjectStore")
//         }
//     }

//     #[async_trait]
//     impl ObjectStore for NopeObjectStore {
//         async fn put_opts(
//             &self,
//             _location: &Path,
//             _payload: PutPayload,
//             _opts: PutOptions,
//         ) -> Result<PutResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn put_multipart(&self, _location: &Path) -> Result<Box<dyn MultipartUpload>> {
//             Ok(Box::new(NopeMultipartUpload))
//         }

//         async fn put_multipart_opts(
//             &self,
//             _location: &Path,
//             _opts: PutMultipartOpts,
//         ) -> Result<Box<dyn MultipartUpload>> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn delete(&self, _location: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }

//         fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
//             futures::stream::once(ready(Err(object_store::Error::NotImplemented))).boxed()
//         }

//         async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }
//     }

//     #[tokio::test]
//     async fn test_put_multipart_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         // store returning erroring MultipartUpload
//         let store = Arc::new(NopeObjectStore);
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         let mut multipart_upload = store
//             .put_multipart(&Path::from("test"))
//             .await
//             .expect("should get multipart upload");
//         assert!(multipart_upload
//             .put_part(PutPayload::from(Bytes::from(vec![42_u8, 42, 42, 42, 42])))
//             .await
//             .is_err());
//         drop(multipart_upload);
//         store.close().await;

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[derive(Default, Debug)]
//     struct WriteOnceMultipartUpload(AtomicBool /* has previous write */);

//     #[async_trait]
//     impl MultipartUpload for WriteOnceMultipartUpload {
//         fn put_part(&mut self, _data: PutPayload) -> UploadPart {
//             let has_previous_writes = self.0.load(Ordering::Acquire);
//             self.0.fetch_or(true, Ordering::AcqRel);
//             async move {
//                 if has_previous_writes {
//                     Err(object_store::Error::NotImplemented)
//                 } else {
//                     Ok(())
//                 }
//             }
//             .boxed()
//         }

//         async fn complete(&mut self) -> Result<PutResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn abort(&mut self) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }
//     }

//     #[derive(Debug)]
//     struct WriteOnceObjectStore;

//     impl std::fmt::Display for WriteOnceObjectStore {
//         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//             write!(f, "WriteOnceObjectStore")
//         }
//     }

//     #[async_trait]
//     impl ObjectStore for WriteOnceObjectStore {
//         async fn put_opts(
//             &self,
//             _location: &Path,
//             _payload: PutPayload,
//             _opts: PutOptions,
//         ) -> Result<PutResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn put_multipart(&self, _location: &Path) -> Result<Box<dyn MultipartUpload>> {
//             Ok(Box::<WriteOnceMultipartUpload>::default())
//         }

//         async fn put_multipart_opts(
//             &self,
//             _location: &Path,
//             _opts: PutMultipartOpts,
//         ) -> Result<Box<dyn MultipartUpload>> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn delete(&self, _location: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }

//         fn list(&self, _prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
//             futures::stream::once(ready(Err(object_store::Error::NotImplemented))).boxed()
//         }

//         async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }

//         async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
//             Err(object_store::Error::NotImplemented)
//         }
//     }

//     #[tokio::test]
//     async fn test_put_multipart_delayed_write_failure() {
//         let metrics = Arc::new(metric::Registry::default());
//         // store returning erroring MultipartUpload
//         let store = Arc::new(WriteOnceObjectStore);
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         // FAILURE: one write ok, one write failure.
//         let mut multipart_upload = store
//             .put_multipart(&Path::from("test"))
//             .await
//             .expect("should get multipart upload");
//         // first write succeeds
//         assert!(multipart_upload
//             .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
//             .await
//             .is_ok());
//         // second write fails
//         assert!(multipart_upload
//             .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
//             .await
//             .is_err());
//         drop(multipart_upload);
//         store.close().await;

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//             10,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_put_multipart_delayed_flush_failure() {
//         let metrics = Arc::new(metric::Registry::default());
//         // store returning erroring MultiPartUpload
//         let store = Arc::new(WriteOnceObjectStore);
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         // FAILURE: one write ok, one flush failure.
//         let mut multipart_upload = store
//             .put_multipart(&Path::from("test"))
//             .await
//             .expect("should get multipart upload");
//         // first write succeeds
//         assert!(multipart_upload
//             .put_part(PutPayload::from_static(&[42_u8, 42, 42, 42, 42]))
//             .await
//             .is_ok());
//         // flush fails
//         assert!(multipart_upload.complete().await.is_err());
//         drop(multipart_upload);
//         store.close().await;

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "put_multipart"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_list() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         store
//             .put(&Path::from("bar"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store.list(None).try_collect::<Vec<_>>().await.unwrap();

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_objects",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list"),
//                 ("result", "success"),
//             ],
//             2,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_list_with_offset() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         store
//             .put(&Path::from("bar"), PutPayload::default())
//             .await
//             .unwrap();
//         store
//             .put(&Path::from("baz"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .list_with_offset(None, &Path::from("bar"))
//             .try_collect::<Vec<_>>()
//             .await
//             .unwrap();

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_objects",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list_with_offset"),
//                 ("result", "success"),
//             ],
//             2,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list_with_offset"),
//                 ("result", "success"),
//             ],
//         );

//         // NOT raw `list` call
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_list_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         assert!(
//             store.list(None).try_collect::<Vec<_>>().await.is_err(),
//             "mock configured to fail"
//         );

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_list_with_delimiter() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .list_with_delimiter(Some(&Path::from("test")))
//             .await
//             .expect("list should succeed");

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list_with_delimiter"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_list_with_delimiter_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         assert!(
//             store
//                 .list_with_delimiter(Some(&Path::from("test")))
//                 .await
//                 .is_err(),
//             "mock configured to fail"
//         );

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "list_with_delimiter"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_head_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .head(&Path::from("test"))
//             .await
//             .expect_err("mock configured to fail");

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "head"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_get_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .get(&Path::from("test"))
//             .await
//             .expect_err("mock configured to fail");

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "error"),
//             ],
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_ttfb",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_getrange_fails() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(DummyObjectStore::new("s3"));
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .get_range(&Path::from("test"), 0..1000)
//             .await
//             .expect_err("mock configured to fail");

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_range"),
//                 ("result", "error"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_getranges() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::from_static(b"bar"))
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .get_ranges(&Path::from("foo"), &[0..2, 1..2, 0..1])
//             .await
//             .unwrap();

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_ranges"),
//                 ("result", "success"),
//             ],
//             4,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_ranges"),
//                 ("result", "success"),
//             ],
//         );

//         // NO `get_range` used!
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_range"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_copy() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .copy(&Path::from("foo"), &Path::from("bar"))
//             .await
//             .unwrap();

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "copy"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_copy_if_not_exists() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .copy_if_not_exists(&Path::from("foo"), &Path::from("bar"))
//             .await
//             .unwrap();

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "copy_if_not_exists"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_rename() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .rename(&Path::from("foo"), &Path::from("bar"))
//             .await
//             .unwrap();

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "rename"),
//                 ("result", "success"),
//             ],
//         );

//         // NO `copy`/`delete` used!
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "copy"),
//                 ("result", "success"),
//             ],
//         );
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_rename_if_not_exists() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .rename_if_not_exists(&Path::from("foo"), &Path::from("bar"))
//             .await
//             .unwrap();

//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "rename_if_not_exists"),
//                 ("result", "success"),
//             ],
//         );

//         // NO `copy`/`copy_if_not_exists`/`delete` used!
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "copy"),
//                 ("result", "success"),
//             ],
//         );
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "copy_if_not_exists"),
//                 ("result", "success"),
//             ],
//         );
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_delete_stream() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         store
//             .put(&Path::from("foo"), PutPayload::default())
//             .await
//             .unwrap();
//         store
//             .put(&Path::from("bar"), PutPayload::default())
//             .await
//             .unwrap();
//         store
//             .put(&Path::from("baz"), PutPayload::default())
//             .await
//             .unwrap();
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         store
//             .delete_stream(
//                 stream::iter(["foo", "baz"])
//                     .map(|s| Ok(Path::from(s)))
//                     .boxed(),
//             )
//             .try_collect::<Vec<_>>()
//             .await
//             .unwrap();

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_objects",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete_stream"),
//                 ("result", "success"),
//             ],
//             2,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete_stream"),
//                 ("result", "success"),
//             ],
//         );

//         // NOT raw `delete` call
//         assert_histogram_not_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_put_get_getrange_head_delete_file() {
//         let metrics = Arc::new(metric::Registry::default());
//         // Temporary workaround for https://github.com/apache/arrow-rs/issues/2370
//         let path = std::fs::canonicalize(".").unwrap();
//         let store = Arc::new(LocalFileSystem::new_with_prefix(path).unwrap());
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         let data = Bytes::from(vec![42_u8, 42, 42, 42, 42]);
//         let path = Path::from("test");
//         store
//             .put(&path, PutPayload::from(data.clone()))
//             .await
//             .expect("put should succeed");

//         let got = store.get(&path).await.expect("should read file");
//         match got.payload {
//             GetResultPayload::File(mut file, _) => {
//                 let mut contents = vec![];
//                 file.read_to_end(&mut contents)
//                     .expect("failed to read file data");
//                 assert_eq!(Bytes::from(contents), data);
//             }
//             v => panic!("not a file: {v:?}"),
//         }

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "success"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "success"),
//             ],
//         );

//         store
//             .get_range(&path, 1..4)
//             .await
//             .expect("should clean up test file");
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_range"),
//                 ("result", "success"),
//             ],
//             3,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get_range"),
//                 ("result", "success"),
//             ],
//         );

//         store.head(&path).await.expect("should clean up test file");
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "head"),
//                 ("result", "success"),
//             ],
//         );

//         store
//             .delete(&path)
//             .await
//             .expect("should clean up test file");
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "delete"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     #[tokio::test]
//     async fn test_get_stream() {
//         let metrics = Arc::new(metric::Registry::default());
//         let store = Arc::new(InMemory::new());
//         let time = Arc::new(SystemProvider::new());
//         let store = ObjectStoreMetrics::new(store, time, "bananas", &metrics);

//         let data = Bytes::from(vec![42_u8, 42, 42, 42, 42]);
//         let path = Path::from("test");
//         store
//             .put(&path, PutPayload::from(data.clone()))
//             .await
//             .expect("put should succeed");

//         let got = store.get(&path).await.expect("should read stream");
//         match got.payload {
//             GetResultPayload::Stream(mut stream) => while (stream.next().await).is_some() {},
//             v => panic!("not a stream: {v:?}"),
//         }

//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "success"),
//             ],
//             5,
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_duration",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "success"),
//             ],
//         );
//         assert_histogram_hit(
//             &metrics,
//             "object_store_op_ttfb",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "get"),
//                 ("result", "success"),
//             ],
//         );
//     }

//     // Ensures the stream decorator correctly records the wall-clock time taken
//     // for the caller to consume all the streamed data, and incrementally tracks
//     // the number of bytes observed.
//     #[tokio::test]
//     async fn test_stream_decorator() {
//         let inner = stream::iter(
//             [
//                 Ok(Bytes::copy_from_slice(&[1])),
//                 Ok(Bytes::copy_from_slice(&[2, 3, 4])),
//             ]
//             .into_iter()
//             .collect::<Vec<Result<_, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let mut stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         // Sleep at least 10ms to assert the recorder to captures the wall clock
//         // time.
//         const SLEEP: Duration = Duration::from_millis(20);
//         time_provider.inc(SLEEP);

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 1);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );

//         // Sleep at least 10ms to assert the recorder to captures the wall clock
//         // time.
//         time_provider.inc(SLEEP);

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 3);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             4,
//         );

//         let success_hist = &m.inner.inner.success_duration;
//         let ttfb_hist = &m.success_duration;

//         // Until the stream is fully consumed, there should be no wall clock
//         // metrics emitted.
//         assert!(!success_hist.fetch().buckets.iter().any(|b| b.count > 0));
//         assert!(!ttfb_hist.fetch().buckets.iter().any(|b| b.count > 0));

//         // The stream should complete and cause metrics to be emitted.
//         assert!(stream.next().await.is_none());

//         // Now the stream is complete, the wall clock duration must have been
//         // recorded.
//         let hit_count = success_hist.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             4,
//         );

//         // Wall clock duration it must be in a total SLEEP.
//         let hit_count = success_hist.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration not recorded correctly");
//         let d = success_hist.fetch().total;
//         assert_eq!(d, SLEEP * 2, "wall clock duration not recorded correctly");

//         // TTFB after first sleep
//         let hit_count = ttfb_hist.fetch().sample_count();
//         assert_eq!(
//             hit_count, 1,
//             "ttfb wall clock duration not recorded correctly"
//         );
//         let d = ttfb_hist.fetch().total;
//         assert_eq!(d, SLEEP, "ttfb wall clock duration not recorded correctly");

//         // Metrics must not be duplicated when the decorator is dropped
//         drop(stream);
//         let hit_count = success_hist.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration duplicated");
//         let hit_count = ttfb_hist.fetch().sample_count();
//         assert_eq!(hit_count, 1, "ttfb duration duplicated");
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             4,
//         );
//     }

//     // Ensures the stream decorator correctly records the wall clock duration
//     // and consumed byte count for a partially drained stream that is then
//     // dropped.
//     #[tokio::test]
//     async fn test_stream_decorator_drop_incomplete() {
//         let inner = stream::iter(
//             [
//                 Ok(Bytes::copy_from_slice(&[1])),
//                 Ok(Bytes::copy_from_slice(&[2, 3, 4])),
//             ]
//             .into_iter()
//             .collect::<Vec<Result<_, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_millis(0).unwrap()));

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let mut stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 1);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );

//         // Sleep at least 10ms to assert the recorder to captures the wall clock
//         // time.
//         const SLEEP: Duration = Duration::from_millis(20);
//         time_provider.inc(SLEEP);

//         // Drop the stream without consuming the rest of the data.
//         drop(stream);

//         // Now the stream is complete, the wall clock duration must have been
//         // recorded.
//         let hit_count = m.inner.inner.success_duration.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

//         // And the number of bytes read must match the pre-drop value.
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );
//     }

//     // Ensures the stream decorator records the wall clock duration into the
//     // "error" histogram after the stream is dropped after emitting an error.
//     #[tokio::test]
//     async fn test_stream_decorator_transient_error_dropped() {
//         let inner = stream::iter(
//             [
//                 Ok(Bytes::copy_from_slice(&[1])),
//                 Err(Error::new(ErrorKind::Other, "oh no!")),
//                 Ok(Bytes::copy_from_slice(&[2, 3, 4])),
//             ]
//             .into_iter()
//             .collect::<Vec<Result<_, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(SystemProvider::default());

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let mut stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 1);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "error"),
//             ],
//             0,
//         );

//         let _err = stream
//             .next()
//             .await
//             .expect("should yield an error")
//             .expect_err("error configured in underlying stream");

//         // Drop after observing an error
//         drop(stream);

//         // Ensure the wall clock was added to the "error" histogram.
//         let hit_count = m.inner.inner.error_duration.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");

//         // And the number of bytes read must match
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "error"),
//             ],
//             0,
//         );
//     }

//     // Ensures the stream decorator records the wall clock duration into the
//     // "success" histogram after the stream progresses past a transient error.
//     #[tokio::test]
//     async fn test_stream_decorator_transient_error_progressed() {
//         let inner = stream::iter(
//             [
//                 Ok(Bytes::copy_from_slice(&[1])),
//                 Err(Error::new(ErrorKind::Other, "oh no!")),
//                 Ok(Bytes::copy_from_slice(&[2, 3, 4])),
//             ]
//             .into_iter()
//             .collect::<Vec<Result<_, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(SystemProvider::default());

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let mut stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 1);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             1,
//         );

//         let _err = stream
//             .next()
//             .await
//             .expect("should yield an error")
//             .expect_err("error configured in underlying stream");

//         let got = stream
//             .next()
//             .await
//             .expect("should yield data")
//             .expect("should succeed");
//         assert_eq!(got.len(), 3);
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             4,
//         );

//         // Drop after observing an error
//         drop(stream);

//         // Ensure the wall clock was added to the "success" histogram after
//         // progressing past the transient error.
//         let hit_count = m.inner.inner.success_duration.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
//         let hit_count = m.success_duration.fetch().sample_count();
//         assert_eq!(
//             hit_count, 1,
//             "ttfb wall clock duration recorded incorrectly"
//         );

//         // And the number of bytes read must match
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             4,
//         );
//     }

//     // Ensures the wall clock time recorded by the stream decorator includes the
//     // initial get even if never polled.
//     #[tokio::test]
//     async fn test_stream_immediate_drop() {
//         let inner = stream::iter(
//             [Ok(Bytes::copy_from_slice(&[1]))]
//                 .into_iter()
//                 .collect::<Vec<Result<Bytes, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(SystemProvider::default());

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         // Drop immediately
//         drop(stream);

//         // Ensure the wall clock was added to the "success" histogram
//         let hit_count = m.inner.inner.success_duration.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
//         let hit_count = m.success_duration.fetch().sample_count();
//         assert_eq!(
//             hit_count, 1,
//             "ttfb wall clock duration recorded incorrectly"
//         );

//         // And the number of bytes read must match
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             0,
//         );
//     }

//     // Ensures the wall clock time recorded by the stream decorator emits a wall
//     // clock duration even if it never yields any data.
//     #[tokio::test]
//     async fn test_stream_empty() {
//         let inner = stream::iter(
//             [].into_iter()
//                 .collect::<Vec<Result<Bytes, std::io::Error>>>(),
//         );

//         let time_provider = Arc::new(SystemProvider::default());

//         let metrics = Arc::new(metric::Registry::default());
//         let m = MetricsWithBytesAndTtfb::new(&metrics, &StoreType("bananas".into()), "test");

//         let mut stream = StreamMetricRecorder::new(
//             inner,
//             time_provider.now(),
//             BytesStreamDelegate::new(m.clone()),
//             Arc::clone(&time_provider) as _,
//         );

//         assert!(stream.next().await.is_none());

//         // Ensure the wall clock was added to the "success" histogram even
//         // though it yielded no data.
//         let hit_count = m.inner.inner.success_duration.fetch().sample_count();
//         assert_eq!(hit_count, 1, "wall clock duration recorded incorrectly");
//         let hit_count = m.success_duration.fetch().sample_count();
//         assert_eq!(
//             hit_count, 1,
//             "ttfb wall clock duration recorded incorrectly"
//         );

//         // And the number of bytes read must match
//         assert_counter_value(
//             &metrics,
//             "object_store_transfer_bytes",
//             [
//                 ("store_type", "bananas"),
//                 ("op", "test"),
//                 ("result", "success"),
//             ],
//             0,
//         );
//     }
// }
