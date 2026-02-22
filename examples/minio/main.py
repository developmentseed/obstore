# ruff: noqa
import asyncio

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
    await store.put_async("a.txt", b"foo")
    await store.put_async("b.txt", b"bar")
    await store.put_async("c/d.txt", b"baz")

    print("\nList files:")
    files = await store.list().collect_async()
    print(files)

    print("\nFetch a.txt")
    resp = await store.get_async("a.txt")
    print(await resp.bytes_async())

    print("\nDelete a.txt")
    await store.delete_async("a.txt")


if __name__ == "__main__":
    asyncio.run(main())
