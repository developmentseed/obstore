# ruff: noqa
import asyncio

import obstore as obs
from obstore.store import S3Store


async def main():
    store = S3Store(
        "test-bucket",
        endpoint="http://localhost:9000",
        access_key_id="minioadmin",
        secret_access_key="minioadmin",
        virtual_hosted_style_request=False,
        client_options={"allow_http": True},
    )

    print("Put file:")
    await obs.put_async(store, "a.txt", b"foo")
    await obs.put_async(store, "b.txt", b"bar")
    await obs.put_async(store, "c/d.txt", b"baz")

    print("\nList files:")
    files = await obs.list(store).collect_async()
    print(files)

    print("\nFetch a.txt")
    resp = await obs.get_async(store, "a.txt")
    print(await resp.bytes_async())

    print("\nDelete a.txt")
    await obs.delete_async(store, "a.txt")


if __name__ == "__main__":
    asyncio.run(main())
