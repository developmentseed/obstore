# Minio example

[MinIO](https://github.com/minio/minio) is a high-performance, S3 compatible object store, open sourced under GNU AGPLv3 license. It's often used for testing or self-hosting S3-compatible storage.

We can run minio locally using docker:

```shell
docker run -p 9000:9000 -p 9001:9001 \
    quay.io/minio/minio server /data --console-address ":9001"
```

`obstore` isn't able to create a bucket, so we need to do that manually. We can do that through the minio web UI. After running the above docker command, go to <http://localhost:9001>.

Then log in with the credentials `minioadmin`, `minioadmin` for username and password.

Then click "Create a Bucket" and create a bucket with the name `"test-bucket"`.

Now, run the Python script:

```
uv run python main.py
```
