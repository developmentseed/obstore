# Minio

[MinIO](https://github.com/minio/minio) is a high-performance, S3 compatible object store, open sourced under GNU AGPLv3 license. It's often used for testing or self-hosting S3-compatible storage.

We can run minio locally using docker:

```shell
docker run -p 9000:9000 -p 9001:9001 \
    quay.io/minio/minio server /data --console-address ":9001"
```

`obstore` isn't able to create a bucket, so we need to do that manually. We can do that through the minio web UI. After running the above docker command, go to <http://localhost:9001>. Then log in with the credentials `minioadmin`, `minioadmin` for username and password. Then click "Create a Bucket" and create a bucket with the name `"test-bucket"`.

Now we can create an `S3Store` to interact with minio:

```py
import obstore as obs
from obstore.store import S3Store

store = S3Store(
    "test-bucket",
    endpoint="http://localhost:9000",
    access_key_id="minioadmin",
    secret_access_key="minioadmin",
    virtual_hosted_style_request=False,
    client_options={"allow_http": True},
)

# Add files
obs.put(store, "a.txt", b"foo")
obs.put(store, "b.txt", b"bar")
obs.put(store, "c/d.txt", b"baz")

# List files
files = obs.list(store).collect()
print(files)

# Download a file
resp = obs.get(store, "a.txt")
print(resp.bytes())

# Delete a file
obs.delete(store, "a.txt")
```

There's a [full example](https://github.com/developmentseed/obstore/tree/main/examples/minio) in the obstore repository.
