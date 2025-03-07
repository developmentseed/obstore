use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use super::iox_time::{Time, TimeProvider};
use async_trait::async_trait;
use futures::{channel::oneshot::Sender, FutureExt};
use object_store::{MultipartUpload, PutPayload, PutResult, UploadPart};

/// Able to perform metrics tracking bytes written
/// with object_store::put_multipart.
#[derive(Debug)]
pub(crate) struct MultipartUploadWrapper {
    /// current inner [`MultipartUpload`]
    inner: Box<dyn MultipartUpload>,

    /// calculate end time
    time_provider: Arc<dyn TimeProvider>,

    /// current subtotaled size, regardless of success or failure
    bytes_attempted_so_far: Arc<AtomicU64>,
    /// current outcome
    outcome: Arc<AtomicBool>,
    /// action upon complete
    tx: Option<Sender<(bool, Option<u64>, Time)>>,
}

impl MultipartUploadWrapper {
    pub(crate) fn new(
        inner: Box<dyn MultipartUpload>,
        time_provider: Arc<dyn TimeProvider>,
        tx: Sender<(bool, Option<u64>, Time)>,
    ) -> Self {
        Self {
            inner,
            time_provider,
            bytes_attempted_so_far: Arc::new(AtomicU64::new(0)),
            outcome: Arc::new(AtomicBool::new(true)),
            tx: Some(tx),
        }
    }
}

#[async_trait]
impl MultipartUpload for MultipartUploadWrapper {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let attempted_size = data.content_length();
        let res = self.inner.as_mut().put_part(data);
        let bytes_attempted_so_far = self.bytes_attempted_so_far.clone();
        let outcome = self.outcome.clone();

        async move {
            match res.await {
                Ok(_) => {
                    bytes_attempted_so_far.fetch_add(attempted_size as u64, Ordering::AcqRel);
                    Ok(())
                }
                Err(e) => {
                    outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
                    bytes_attempted_so_far.fetch_add(attempted_size as u64, Ordering::AcqRel);
                    Err(e)
                }
            }
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let res = self.inner.complete().await;
        if res.is_err() {
            self.outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
        }
        res
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.outcome.fetch_and(false, Ordering::AcqRel); // mark result failure
        self.inner.abort().await
    }
}

impl Drop for MultipartUploadWrapper {
    fn drop(&mut self) {
        let outcome = self.outcome.load(Ordering::Acquire);
        let bytes = self.bytes_attempted_so_far.load(Ordering::Acquire);

        if let Some(tx) = self.tx.take() {
            tx.send((outcome, Some(bytes), self.time_provider.now()))
                .expect("should send object store metrics back from MultipartUploadWrapper");
        }
    }
}
