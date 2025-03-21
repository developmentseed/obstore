import pystac_client

from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
)

collection = catalog.get_collection("daymet-daily-hi")
asset = collection.assets["zarr-abfs"]


def test_from_asset():
    abfs_asset = collection.assets["zarr-abfs"]
    PlanetaryComputerCredentialProvider.from_asset(abfs_asset)

    PlanetaryComputerCredentialProvider.from_asset(abfs_asset.__dict__)

    blob_asset = collection.assets["zarr-https"]
    PlanetaryComputerCredentialProvider.from_asset(blob_asset)

    PlanetaryComputerCredentialProvider.from_asset(blob_asset.__dict__)
