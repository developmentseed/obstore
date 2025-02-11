from datetime import timedelta

from obstore.store import HTTPStore


def test_config_timedelta():
    HTTPStore.from_url(
        "https://example.com",
        client_options={"timeout": timedelta(seconds=30)},
    )
