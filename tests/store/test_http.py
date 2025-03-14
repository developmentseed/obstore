import pickle

from obstore.store import HTTPStore


def test_pickle():
    store = HTTPStore.from_url("https://example.com")
    new_store: HTTPStore = pickle.loads(pickle.dumps(store))
    assert store.url == new_store.url


def test_eq():
    store = HTTPStore.from_url("https://example.com", client_options={"timeout": "10s"})
    store2 = HTTPStore.from_url(
        "https://example.com",
        client_options={"timeout": "10s"},
    )
    store3 = HTTPStore.from_url("https://example2.com")
    assert store == store  # noqa: PLR0124
    assert store == store2
    assert store != store3
