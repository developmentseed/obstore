from pathlib import Path

import obstore as obs
from obstore.store import LocalStore


def test_local_store():
    here = Path(".")
    store = LocalStore(here)
    list_result = obs.list(store).collect()
    assert any("test_local.py" in x["path"] for x in list_result)


def test_repr():
    here = Path(".")
    store = LocalStore(here)
    assert repr(store).startswith("LocalStore")
