# tqdm (Progress Bar)

[tqdm](https://tqdm.github.io/) provides an interactive progress bar for Python.

![](../assets/example.gif)

It's easy to wrap obstore downloads with a tqdm progress bar:

```py
from obstore.store import HTTPStore
from tqdm import tqdm

store = HTTPStore.from_url("https://ookla-open-data.s3.us-west-2.amazonaws.com")
path = "parquet/performance/type=fixed/year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet"
response = obs.get(store, path)
file_size = response.meta["size"]
with tqdm(total=file_size) as pbar:
    for bytes_chunk in response:
        # Do something with buffer
        pbar.update(len(bytes_chunk))
```

Or, if you're using the async API:

```py
from obstore.store import HTTPStore
from tqdm import tqdm

store = HTTPStore.from_url("https://ookla-open-data.s3.us-west-2.amazonaws.com")
path = "parquet/performance/type=fixed/year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet"
response = await obs.get_async(store, path)
file_size = response.meta["size"]
with tqdm(total=file_size) as pbar:
    async for bytes_chunk in response:
        # Do something with buffer
        pbar.update(len(bytes_chunk))
```

There's a [full example](https://github.com/developmentseed/obstore/tree/main/examples/progress-bar) in the obstore repository.
