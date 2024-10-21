import boto3
import object_store_rs as obs
import pyarrow.parquet as pq
from object_store_rs.fsspec import AsyncFsspecStore

# session = boto3.Session()

store = obs.store.HTTPStore.from_url("https://github.com")
fs = AsyncFsspecStore(store)
url = "opengeospatial/geoparquet/raw/refs/heads/main/examples/example.parquet"
test = pq.read_metadata(url, filesystem=fs)
