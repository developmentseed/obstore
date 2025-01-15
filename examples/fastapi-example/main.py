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

    # Make the request. This only begins the download.
    resp = await obs.get_async(store, path)

    # Example: Ensure the stream returns at least 1MB of data in each chunk.
    return StreamingResponse(resp.stream(min_chunk_size=1 * 1024 * 1024))


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
