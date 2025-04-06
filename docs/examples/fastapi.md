# FastAPI

[FastAPI](https://fastapi.tiangolo.com/) is a modern, high-performance, web framework for building APIs with Python based on standard Python type hints.

It's easy to integrate obstore with FastAPI routes, where you want to download a file from an object store and return it to the user.

FastAPI has a [`StreamingResponse`](https://fastapi.tiangolo.com/advanced/custom-response/#streamingresponse), which neatly integrates with [`BytesStream`][obstore.BytesStream] to stream the response to the user.

!!! note

    This example is also [available on Github](https://github.com/developmentseed/obstore/blob/main/examples/fastapi/README.md) if you'd like to test it out locally.

First, import `fastapi` and `obstore` and create the FastAPI application.

```py
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

import obstore as obs
from obstore.store import HTTPStore, S3Store

app = FastAPI()
```

Next, we can add our route. Here, we create a simple route that fetches a small
Parquet file from an HTTP url and returns it to the user.

Passing `resp` directly to `StreamingResponse` calls
[`GetResult.stream()`][obstore.GetResult.stream] under the hood and thus uses
the default chunking behavior of `GetResult.stream()`.

```py
@app.get("/example.parquet")
async def download_example() -> StreamingResponse:
    store = HTTPStore.from_url("https://raw.githubusercontent.com")
    path = "opengeospatial/geoparquet/refs/heads/main/examples/example.parquet"

    # Make the request. This only begins the download; it does not wait for the
    # download to finish.
    resp = await obs.get_async(store, path)
    return StreamingResponse(resp)
```

You may also want to customize the chunking behavior of the async stream. To do
this, call [`GetResult.stream()`][obstore.GetResult.stream] before passing to
`StreamingResponse`.

```py
@app.get("/large.parquet")
async def large_example() -> StreamingResponse:
    # Example large Parquet file hosted in AWS open data
    store = S3Store("ookla-open-data", region="us-west-2", skip_signature=True)
    path = "parquet/performance/type=fixed/year=2024/quarter=1/2024-01-01_performance_fixed_tiles.parquet"

    # Note: for large file downloads you may need to increase the timeout in
    # the client configuration
    resp = await obs.get_async(store, path)

    # Example: Ensure the stream returns at least 5MB of data in each chunk.
    return StreamingResponse(resp.stream(min_chunk_size=5 * 1024 * 1024))
```

Note that here FastAPI wraps
[`starlette.responses.StreamingResponse`](https://www.starlette.io/responses/#streamingresponse).
So any web server that uses [Starlette](https://www.starlette.io/) for responses
can use this same code.
