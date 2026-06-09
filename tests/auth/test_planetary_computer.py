from __future__ import annotations

import json
from pathlib import Path

import pystac
import pytest

from obstore.auth.planetary_computer import (
    PlanetaryComputerAsyncCredentialProvider,
    PlanetaryComputerCredentialProvider,
)
from obstore.store import AzureStore

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "planetary_computer"


def load_collection(collection_id: str) -> pystac.Collection:
    """Load a recorded Planetary Computer STAC Collection from a fixture.

    The assets exercised by `from_asset` are static published metadata, so we
    record them once rather than hitting the live STAC API on every test run
    (which is sporadically flaky). To refresh, re-fetch with
    `pystac_client.Client.open(...).get_collection(collection_id).to_dict()`.
    """
    with (FIXTURES_DIR / f"{collection_id}.json").open() as f:
        return pystac.Collection.from_dict(json.load(f))


@pytest.mark.parametrize(
    "cls",
    [PlanetaryComputerCredentialProvider, PlanetaryComputerAsyncCredentialProvider],
)
@pytest.mark.asyncio
async def test_from_asset(
    cls: type[
        PlanetaryComputerCredentialProvider | PlanetaryComputerAsyncCredentialProvider
    ],
):
    collection = load_collection("daymet-daily-hi")

    abfs_asset = collection.assets["zarr-abfs"]
    cls.from_asset(abfs_asset)

    cls.from_asset(abfs_asset.__dict__)

    blob_asset = collection.assets["zarr-https"]
    cls.from_asset(blob_asset)

    cls.from_asset(blob_asset.__dict__)

    collection = load_collection("landsat-c2-l2")
    gpq_asset = collection.assets["geoparquet-items"]
    cls.from_asset(gpq_asset)

    cls.from_asset(gpq_asset.__dict__)


@pytest.mark.parametrize(
    "cls",
    [PlanetaryComputerCredentialProvider, PlanetaryComputerAsyncCredentialProvider],
)
@pytest.mark.asyncio
async def test_pass_config_to_store(
    cls: type[
        PlanetaryComputerCredentialProvider | PlanetaryComputerAsyncCredentialProvider
    ],
):
    url = "https://naipeuwest.blob.core.windows.net/naip/v002/mt/2023/mt_060cm_2023/"
    store = AzureStore(credential_provider=cls(url))
    assert store.config == {"account_name": "naipeuwest", "container_name": "naip"}
    assert store.prefix == "v002/mt/2023/mt_060cm_2023"


@pytest.mark.parametrize(
    "cls",
    [PlanetaryComputerCredentialProvider, PlanetaryComputerAsyncCredentialProvider],
)
@pytest.mark.asyncio
async def test_url_account_container_params(
    cls: type[
        PlanetaryComputerCredentialProvider | PlanetaryComputerAsyncCredentialProvider
    ],
):
    url = "https://naipeuwest.blob.core.windows.net/naip/v002/mt/2023/mt_060cm_2023/"
    account_name = "naipeuwest"
    container_name = "naip"

    cls(url)

    with pytest.raises(ValueError, match="Cannot pass container_name"):
        cls(url, container_name=container_name)

    with pytest.raises(ValueError, match="Cannot pass account_name"):
        cls(url, account_name=account_name)

    cls(
        account_name=account_name,
        container_name=container_name,
    )
