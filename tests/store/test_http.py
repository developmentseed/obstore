import pickle

from obstore.store import HTTPStore


def test_pickle():
    store = HTTPStore.from_url("https://example.com")
    new_store: HTTPStore = pickle.loads(pickle.dumps(store))
    assert store.url == new_store.url
