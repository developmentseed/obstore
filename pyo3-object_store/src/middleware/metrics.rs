use std::ops::Range;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult,
};
use tokio::sync::Mutex;
use tracing::{info_span, Instrument};

#[derive(Debug)]
enum RequestType {
    /// A put request, with a content length
    Put(usize),
}

#[derive(Debug)]
pub struct RequestMetric {
    path: Path,
    elapsed: Duration,
    request_type: RequestType,
}

#[derive(Debug)]
pub struct MetricsStore<T: ObjectStore> {
    inner: T,
    requests: Mutex<Vec<RequestMetric>>,
}

impl<T: ObjectStore> std::fmt::Display for MetricsStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricsObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl<T: ObjectStore> ObjectStore for MetricsStore<T> {
    async fn put(&self, location: &Path, payload: PutPayload) -> object_store::Result<PutResult> {
        let span = info_span!("put");
        self.inner.put(location, payload).instrument(span).await

        // let now = Instant::now();
        // let payload_size = payload.content_length();
        // let result = self.inner.put(location, payload).await;
        // let elapsed = now.elapsed();
        // let mut requests = self.requests.lock().await;
        // requests.push(RequestMetric {
        //     path: location.clone(),
        //     elapsed,
        //     request_type: RequestType::Put(payload_size),
        // });
        // result
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(&location, payload, opts).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart(&location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(&location, opts).await
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        self.inner.get(&location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        self.inner.get_range(&location, range).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(&location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<usize>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.inner.get_ranges(&location, ranges).await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.inner.head(&location).await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(&location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix).boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.rename_if_not_exists(from, to).await
    }
}
