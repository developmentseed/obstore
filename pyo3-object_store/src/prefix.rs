//! An object store wrapper handling a constant path prefix
//! This was originally vendored from https://github.com/apache/arrow-rs/blob/3bf29a2c7474e59722d885cd11fafd0dca13a28e/object_store/src/prefix.rs#L4 so that we can access the raw `T` underlying the MaybePrefixedStore.
//! It was further edited to use an `Option<Path>` internally so that we can apply a
//! `MaybePrefixedStore` to all store classes.

use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::aws::AmazonS3;
use object_store::azure::MicrosoftAzure;
use object_store::gcp::GoogleCloudStorage;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use std::borrow::Cow;
use std::ops::Range;
use std::sync::OnceLock;

use object_store::path::Path;
// Remove when updating to object_store 0.13
#[allow(deprecated)]
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};

static DEFAULT_PATH: OnceLock<Path> = OnceLock::new();

/// Store wrapper that applies a constant prefix to all paths handled by the store.
#[derive(Debug, Clone)]
pub struct MaybePrefixedStore<T: ObjectStore> {
    prefix: Option<Path>,
    inner: T,
}

impl<T: ObjectStore> std::fmt::Display for MaybePrefixedStore<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(prefix) = self.prefix.as_ref() {
            write!(f, "PrefixObjectStore({prefix})")
        } else {
            write!(f, "ObjectStore")
        }
    }
}

impl<T: ObjectStore> MaybePrefixedStore<T> {
    /// Create a new instance of [`MaybePrefixedStore`]
    pub fn new(store: T, prefix: Option<impl Into<Path>>) -> Self {
        Self {
            prefix: prefix.map(|x| x.into()),
            inner: store,
        }
    }

    /// Access the underlying T under the MaybePrefixedStore
    pub fn inner(&self) -> &T {
        &self.inner
    }

    /// Create the full path from a path relative to prefix
    fn full_path<'a>(&'a self, location: &'a Path) -> Cow<'a, Path> {
        if let Some(prefix) = &self.prefix {
            Cow::Owned(prefix.parts().chain(location.parts()).collect())
        } else {
            Cow::Borrowed(location)
        }
    }
}

/// Strip the constant prefix from a given path
fn strip_prefix(prefix: Option<&Path>, path: Path) -> Path {
    if let Some(prefix) = prefix {
        // Note cannot use match because of borrow checker
        if let Some(suffix) = path.prefix_match(prefix) {
            return suffix.collect();
        }
        path
    } else {
        path
    }
}

/// Strip the constant prefix from a given ObjectMeta
fn strip_meta(prefix: Option<&Path>, meta: ObjectMeta) -> ObjectMeta {
    ObjectMeta {
        last_modified: meta.last_modified,
        size: meta.size,
        location: strip_prefix(prefix, meta.location),
        e_tag: meta.e_tag,
        version: None,
    }
}

fn strip_list_result(prefix: Option<&Path>, lst: ListResult) -> ListResult {
    ListResult {
        common_prefixes: lst
            .common_prefixes
            .into_iter()
            .map(|p| strip_prefix(prefix, p))
            .collect(),
        objects: lst
            .objects
            .into_iter()
            .map(|meta| strip_meta(prefix, meta))
            .collect(),
    }
}

#[async_trait::async_trait]
impl<T: ObjectStore> ObjectStore for MaybePrefixedStore<T> {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let full_path = self.full_path(location);
        self.inner.put(&full_path, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let full_path = self.full_path(location);
        self.inner.put_opts(&full_path, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let full_path = self.full_path(location);
        self.inner.put_multipart(&full_path).await
    }

    // Remove when updating to object_store 0.13
    #[allow(deprecated)]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        let full_path = self.full_path(location);
        self.inner.put_multipart_opts(&full_path, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let full_path = self.full_path(location);
        self.inner.get(&full_path).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let full_path = self.full_path(location);
        self.inner.get_range(&full_path, range).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let full_path = self.full_path(location);
        self.inner.get_opts(&full_path, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let full_path = self.full_path(location);
        self.inner.get_ranges(&full_path, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let full_path = self.full_path(location);
        let meta = self.inner.head(&full_path).await?;
        Ok(strip_meta(self.prefix.as_ref(), meta))
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let full_path = self.full_path(location);
        self.inner.delete(&full_path).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = self.full_path(prefix.unwrap_or(DEFAULT_PATH.get_or_init(Path::default)));
        let s = self.inner.list(Some(&prefix));
        let slf_prefix = self.prefix.clone();
        s.map_ok(move |meta| strip_meta(slf_prefix.as_ref(), meta))
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let offset = self.full_path(offset);
        let prefix = self.full_path(prefix.unwrap_or(DEFAULT_PATH.get_or_init(Path::default)));
        let s = self.inner.list_with_offset(Some(&prefix), &offset);
        let slf_prefix = self.prefix.clone();
        s.map_ok(move |meta| strip_meta(slf_prefix.as_ref(), meta))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = self.full_path(prefix.unwrap_or(DEFAULT_PATH.get_or_init(Path::default)));
        self.inner
            .list_with_delimiter(Some(&prefix))
            .await
            .map(|lst| strip_list_result(self.prefix.as_ref(), lst))
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.inner.copy(&full_from, &full_to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.inner.rename(&full_from, &full_to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.inner.copy_if_not_exists(&full_from, &full_to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let full_from = self.full_path(from);
        let full_to = self.full_path(to);
        self.inner.rename_if_not_exists(&full_from, &full_to).await
    }
}

fn create_paginated_list_prefix<'a>(
    store_prefix: Option<&'a Path>,
    list_prefix: Option<&'a str>,
    delimiter: Option<&Cow<'static, str>>,
) -> Option<Cow<'a, str>> {
    match (store_prefix, list_prefix) {
        (None, None) => None,
        (Some(store_prefix), None) => Some(Cow::Borrowed(store_prefix.as_ref())),
        (None, Some(list_prefix)) => Some(Cow::Borrowed(list_prefix)),
        (Some(store_prefix), Some(list_prefix)) => {
            let delimiter = delimiter.map(|x| x.as_ref()).unwrap_or("/");
            let combined = format!("{}{delimiter}{list_prefix}", store_prefix.as_ref());
            Some(Cow::Owned(combined))
        }
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for MaybePrefixedStore<AmazonS3> {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let store_prefix = self.prefix.as_ref();
        let list_prefix =
            create_paginated_list_prefix(store_prefix, prefix, opts.delimiter.as_ref());
        let lst = self
            .inner
            .list_paginated(list_prefix.as_deref(), opts)
            .await?;
        Ok(PaginatedListResult {
            result: strip_list_result(store_prefix, lst.result),
            page_token: lst.page_token,
        })
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for MaybePrefixedStore<GoogleCloudStorage> {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let store_prefix = self.prefix.as_ref();
        let list_prefix =
            create_paginated_list_prefix(store_prefix, prefix, opts.delimiter.as_ref());
        let lst = self
            .inner
            .list_paginated(list_prefix.as_deref(), opts)
            .await?;
        Ok(PaginatedListResult {
            result: strip_list_result(store_prefix, lst.result),
            page_token: lst.page_token,
        })
    }
}

#[async_trait::async_trait]
impl PaginatedListStore for MaybePrefixedStore<MicrosoftAzure> {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        opts: PaginatedListOptions,
    ) -> Result<PaginatedListResult> {
        let store_prefix = self.prefix.as_ref();
        let list_prefix =
            create_paginated_list_prefix(store_prefix, prefix, opts.delimiter.as_ref());
        let lst = self
            .inner
            .list_paginated(list_prefix.as_deref(), opts)
            .await?;
        Ok(PaginatedListResult {
            result: strip_list_result(store_prefix, lst.result),
            page_token: lst.page_token,
        })
    }
}
