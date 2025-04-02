from __future__ import annotations

import pystac_client
import pytest

from obstore.auth.planetary_computer import (
    PlanetaryComputerAsyncCredentialProvider,
    PlanetaryComputerCredentialProvider,
)
from obstore.store import AzureStore

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
)


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
    collection = catalog.get_collection("daymet-daily-hi")

    abfs_asset = collection.assets["zarr-abfs"]
    cls.from_asset(abfs_asset)

    cls.from_asset(abfs_asset.__dict__)

    blob_asset = collection.assets["zarr-https"]
    cls.from_asset(blob_asset)

    cls.from_asset(blob_asset.__dict__)

    collection = catalog.get_collection("landsat-c2-l2")
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
