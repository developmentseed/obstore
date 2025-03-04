# Troubleshooting Amazon S3

## Region required

All requests to S3 must include the region. An error will occur on requests when you don't pass the correct region.

For example, trying to list the [`sentinel-cogs`](https://registry.opendata.aws/sentinel-2-l2a-cogs/) open bucket without passing a region will fail:

```py
import obstore as obs
from obstore.store import S3Store

store = S3Store("sentinel-cogs", skip_signature=True)
next(obs.list(store))
```

raises

```
GenericError: Generic S3 error: Error performing list request:
Received redirect without LOCATION, this normally indicates an incorrectly
configured region
```

We can fix this by passing the correct region:

```py
import obstore as obs
from obstore.store import S3Store

store = S3Store("sentinel-cogs", skip_signature=True, region="us-west-2")
next(obs.list(store))
```

this prints:

```py
[{'path': 'sentinel-s2-l2a-cogs/1/C/CV/2018/10/S2B_1CCV_20181004_0_L2A/AOT.tif',
  'last_modified': datetime.datetime(2020, 9, 30, 20, 25, 56, tzinfo=datetime.timezone.utc),
  'size': 50510,
  'e_tag': '"2e24c2ee324ea478f2f272dbd3f5ce69"',
  'version': None},
...
```

### Inferring the bucket region

Note that it's possible to infer the S3 bucket region from an arbitrary `HEAD` request.

Here, we show an example of using `requests` to find the bucket region, but you can use any HTTP client:

```py
import requests

def find_bucket_region(bucket_name: str) -> str:
    resp = requests.head(f"https://{bucket_name}.s3.amazonaws.com")
    return resp.headers["x-amz-bucket-region"]
```

Applying this to our previous example, we can use this to find the region of the `sentinel-cogs` bucket:

```py
find_bucket_region("sentinel-cogs")
# 'us-west-2'
```

Or we can pass this directly into the region:

```py
bucket_name = "sentinel-cogs"
store = S3Store(
    bucket_name, skip_signature=True, region=find_bucket_region(bucket_name)
)
```

Finding the bucket region in this way works **both for public and non-public buckets**.

This `HEAD` request can also tell you if the bucket is public or not by checking the [HTTP response code](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) (accessible in `requests` via [`resp.status_code`](https://requests.readthedocs.io/en/latest/api/#requests.Response.status_code)):

- [`200`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/200): public bucket.
- [`403`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/403): private bucket.
