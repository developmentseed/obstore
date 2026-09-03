"""Tests for `Store.replace_prefix` across each store backend."""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from obstore.store import AzureStore, GCSStore, LocalStore, S3Store

if TYPE_CHECKING:
    from obstore.store import ClientConfig, S3Config


def remote_stores() -> list[S3Store | AzureStore | GCSStore]:
    """One store per remote backend, each configured with a prefix."""
    return [
        S3Store(
            "bucket",
            prefix="data/2024",
            region="us-east-1",
            skip_signature=True,
            client_options={"timeout": "10s"},
            retry_config={"max_retries": 5},
        ),
        AzureStore(
            "container",
            prefix="data/2024",
            account_name="account",
            skip_signature=True,
            client_options={"timeout": "10s"},
            retry_config={"max_retries": 5},
        ),
        GCSStore(
            "bucket",
            prefix="data/2024",
            skip_signature=True,
            client_options={"timeout": "10s"},
            retry_config={"max_retries": 5},
        ),
    ]


@pytest.fixture(params=remote_stores(), ids=lambda store: type(store).__name__)
def remote_store(request: pytest.FixtureRequest) -> S3Store | AzureStore | GCSStore:
    return request.param


def test_replaces_prefix(remote_store: S3Store | AzureStore | GCSStore):
    assert remote_store.replace_prefix("data/2025").prefix == "data/2025"


def test_replaces_prefix_rather_than_appending(
    remote_store: S3Store | AzureStore | GCSStore,
):
    twice = remote_store.replace_prefix("data/2025").replace_prefix("data/2026")
    assert twice.prefix == "data/2026"


def test_none_clears_prefix(remote_store: S3Store | AzureStore | GCSStore):
    assert remote_store.replace_prefix(None).prefix is None


def test_original_store_is_unchanged(remote_store: S3Store | AzureStore | GCSStore):
    remote_store.replace_prefix("data/2025")
    assert remote_store.prefix == "data/2024"


def test_other_config_is_inherited(remote_store: S3Store | AzureStore | GCSStore):
    new_store = remote_store.replace_prefix("data/2025")
    assert new_store.config == remote_store.config
    assert new_store.client_options == remote_store.client_options
    assert new_store.retry_config == remote_store.retry_config


def test_eq_matches_a_directly_constructed_store(
    remote_store: S3Store | AzureStore | GCSStore,
):
    new_store = remote_store.replace_prefix("data/2025")
    directly = type(remote_store)(
        prefix="data/2025",
        config=remote_store.config,  # type: ignore[arg-type]
        client_options=remote_store.client_options,
        retry_config=remote_store.retry_config,
    )
    assert new_store == directly


def test_pickle_round_trip(remote_store: S3Store | AzureStore | GCSStore):
    """The pickling config must stay in sync with the underlying store's prefix."""
    new_store = remote_store.replace_prefix("data/2025")
    restored = pickle.loads(pickle.dumps(new_store))
    assert restored.prefix == "data/2025"
    assert restored == new_store


def test_preserves_subclass(remote_store: S3Store | AzureStore | GCSStore):
    init_calls = []

    class Subclass(type(remote_store)):  # type: ignore[misc]
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            init_calls.append(1)

    store = Subclass(
        prefix="data/2024",
        config=remote_store.config,  # type: ignore[arg-type]
        client_options=remote_store.client_options,
        retry_config=remote_store.retry_config,
    )
    assert len(init_calls) == 1

    new_store = store.replace_prefix("data/2025")
    assert type(new_store) is Subclass
    assert new_store.prefix == "data/2025"
    assert new_store.config == store.config
    # `__init__` is not re-run, just as unpickling a store does not call it.
    assert len(init_calls) == 1


def test_local_replaces_prefix(tmp_path: Path):
    (tmp_path / "2024").mkdir()
    (tmp_path / "2025").mkdir()

    store = LocalStore(tmp_path / "2024", automatic_cleanup=True)
    new_store = store.replace_prefix(tmp_path / "2025")

    assert new_store.prefix == tmp_path / "2025"
    assert isinstance(new_store.prefix, Path)
    # The original is untouched
    assert store.prefix == tmp_path / "2024"
    # And the rest of the config is inherited
    assert new_store == LocalStore(tmp_path / "2025", automatic_cleanup=True)


def test_local_none_clears_prefix(tmp_path: Path):
    assert LocalStore(tmp_path).replace_prefix(None).prefix is None


def test_local_mkdir_is_inherited(tmp_path: Path):
    store = LocalStore(tmp_path / "2024", mkdir=True)
    new_dir = tmp_path / "2025"
    assert not new_dir.exists()

    store.replace_prefix(new_dir)
    assert new_dir.exists()


def test_local_writes_to_the_new_prefix(tmp_path: Path):
    store = LocalStore(tmp_path / "2024", mkdir=True)
    new_store = store.replace_prefix(tmp_path / "2025")
    new_store.put("afile.txt", b"hello world")

    assert (tmp_path / "2025" / "afile.txt").read_bytes() == b"hello world"
    assert not (tmp_path / "2024" / "afile.txt").exists()


def test_local_pickle_round_trip(tmp_path: Path):
    store = LocalStore(tmp_path / "2024", mkdir=True)
    new_store = store.replace_prefix(tmp_path / "2025")
    restored: LocalStore = pickle.loads(pickle.dumps(new_store))
    assert restored.prefix == tmp_path / "2025"
    assert restored == new_store


def test_local_preserves_subclass(tmp_path: Path):
    init_calls = []

    class MyLocalStore(LocalStore):
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            init_calls.append(1)

    store = MyLocalStore(tmp_path / "2024", mkdir=True)
    assert len(init_calls) == 1

    new_store = store.replace_prefix(tmp_path / "2025")
    assert type(new_store) is MyLocalStore
    assert new_store.prefix == tmp_path / "2025"
    # `__init__` is not re-run, just as unpickling a store does not call it.
    assert len(init_calls) == 1


def test_writes_to_the_new_prefix(minio_bucket: tuple[S3Config, ClientConfig]):
    """End-to-end: the underlying store, not just the config, is re-prefixed."""
    config, client_options = minio_bucket
    store = S3Store(prefix="data/2024", config=config, client_options=client_options)
    new_store = store.replace_prefix("data/2025")

    store.put("afile.txt", b"2024")
    new_store.put("afile.txt", b"2025")

    unprefixed = S3Store(config=config, client_options=client_options)
    assert unprefixed.get("data/2024/afile.txt").bytes() == b"2024"
    assert unprefixed.get("data/2025/afile.txt").bytes() == b"2025"

    # Listing through the new store only sees the new prefix
    assert [obj["path"] for obj in new_store.list().collect()] == ["afile.txt"]
