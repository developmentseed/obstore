# ruff: noqa
import asyncio
import sys
from urllib.parse import urlsplit

from tqdm import tqdm

import obstore as obs
from obstore.store import HTTPStore

# https://registry.opendata.aws/speedtest-global-performance/
DEFAULT_URL = "https://ookla-open-data.s3.us-west-2.amazonaws.com/parquet/performance/type=fixed/year=2019/quarter=1/2019-01-01_performance_fixed_tiles.parquet"


def sync_download_progress_bar(url: str):
    store, path = parse_url(url)
    resp = obs.get(store, path)
    file_size = resp.meta["size"]
    with tqdm(total=file_size) as pbar:
        for bytes_chunk in resp:
            # Do something with buffer
            pbar.update(len(bytes_chunk))


async def async_download_progress_bar(url: str):
    store, path = parse_url(url)
    resp = await obs.get_async(store, path)
    file_size = resp.meta["size"]
    with tqdm(total=file_size) as pbar:
        async for bytes_chunk in resp:
            # Do something with buffer
            pbar.update(len(bytes_chunk))


def parse_url(url: str) -> tuple[HTTPStore, str]:
    parsed = urlsplit(url)
    if parsed.query or parsed.fragment:
        raise ValueError("Invalid URL: query or fragment not supported in HTTPStore")

    base = f"{parsed.scheme}://{parsed.netloc}"
    store = HTTPStore.from_url(base)
    return store, parsed.path.lstrip("/")


def main():
    if len(sys.argv) >= 2:
        url = sys.argv[1]
    else:
        url = DEFAULT_URL

    print("Synchronous download:")
    sync_download_progress_bar(url)
    print("Asynchronous download:")
    asyncio.run(async_download_progress_bar(url))


if __name__ == "__main__":
    main()
