# ruff: noqa
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

import obstore as obs
from obstore.store import HTTPStore, S3Store

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/example.parquet")
async def download_example() -> StreamingResponse:
    store = HTTPStore.from_url("https://raw.githubusercontent.com")
    path = "opengeospatial/geoparquet/refs/heads/main/examples/example.parquet"

    # Make the request. This only begins the download; it does not wait for the download
    # to finish.
    resp = await obs.get_async(store, path)

    # Passing `GetResult` directly to `StreamingResponse` calls `GetResult.stream()`
    # under the hood and thus  uses the default chunking behavior of
    # `GetResult.stream()`.
    return StreamingResponse(resp)


@app.get("/large.parquet")
async def large_example() -> StreamingResponse:
    # Example large Parquet file hosted in AWS open data
    store = S3Store("ookla-open-data", region="us-west-2", skip_signature=True)
    path = "parquet/performance/type=fixed/year=2024/quarter=1/2024-01-01_performance_fixed_tiles.parquet"

    # Make the request
    # Note: for large file downloads you may need to increase the timeout in the client
    # configuration
    resp = await obs.get_async(store, path)

    # Example: Ensure the stream returns at least 10MB of data in each chunk.
    return StreamingResponse(resp.stream(min_chunk_size=10 * 1024 * 1024))
