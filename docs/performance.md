# Performance

> Last edited 2025-02-05.

Performance is a primary goal of Obstore. Benchmarking is still ongoing, so this document is a mix of what we've learned so far and our untested expectations.

**tl;dr**: Obstore can't magically make your networking hardware faster, but it can reduce overhead, and in cases where that overhead is the limiting factor it can better utilize your available hardware and improve performance.

## Non-performance benefits

Before we get into the weeds of performance, keep in mind that performance is not the _only_ feature of Obstore. There's a strong focus on developer experience as well:

- Simple to install with no required Python dependencies.
- Works the same across AWS S3, Google Cloud Storage, and Azure Storage.
- Full type hinting, including all store configuration and operations.
- Downloads that automatically act as iterators and uploads that automatically accept iterators.
- Automatic pagination of `list` calls behind the scenes

So you might enjoy using Obstore even in a case where it only marginally improves your performance.

## Defining performance

"Fast" can have several definitions in a networking context.

- **Download latency**: the time until the first byte of a download is received.
- **Single-request throughput**: the download or upload bytes per second of a single request.
- **Many-request throughput**: the combined download or upload bytes per second of multiple concurrent requests.

Furthermore, performance can be different when using obstore's synchronous or asynchronous API.

Let's consider the areas where we expect improved, possibly-improved, and equal performance.

## Improved performance

**Many-request throughput with the asynchronous API** is the primary place where we expect significantly improved performance. Especially when making many requests of relatively small files, we find that obstore can provide significantly higher throughput.

For example, preliminary results indicate roughly [9x higher throughput than fsspec](https://github.com/geospatial-jeff/pyasyncio-benchmark/blob/fe8f290cb3282dcc3bc96cae06ed5f90ad326eff/test_results/cog_header_results.csv) and [2.8x higher throughput than aioboto3](https://github.com/geospatial-jeff/pyasyncio-benchmark/blob/40e67509a248c5102a6b1608bcb9773295691213/test_results/20250218_results/ec2_m5/aggregated_results.csv). That specific benchmark considered fetching the first 16KB of a file many times from an async context.

## Possibly improved performance

**Using the synchronous API**. We haven't benchmarked the synchronous API. However, we do release the Python [Global Interpreter Lock (GIL)](https://en.wikipedia.org/wiki/Global_interpreter_lock) for all synchronous operations, so it may perform better in a thread pool than other Python request libraries.

## Equal performance

- Single-request throughput: if you're making _one request_, the limiting factor is likely network conditions, not Python overhead, so it's unlikely that obstore will be faster.

    Keep in mind, however, that what looks like a single request may actually be multiple requests under the hood. [`obstore.put`][obstore.put] will use multipart uploads by default, meaning that various parts of a file will be uploaded concurrently, and there may be efficiency gains here.
- Latency: this is primarily driven by hardware and network conditions, and we expect Obstore to have similar latency as other Python request libraries.

## Future research

In the future, we'd like to benchmark:

- Alternate Python event loops, e.g. [`uvloop`](https://github.com/MagicStack/uvloop)
- The obstore synchronous API

If you have any interest in collaborating on this, [open an issue](https://github.com/developmentseed/obstore/issues/new/choose).
