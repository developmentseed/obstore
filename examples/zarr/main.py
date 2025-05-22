"""Example using Zarr with the Obstore backend."""

import matplotlib.pyplot as plt
import pystac_client
import xarray as xr
from zarr.storage import ObjectStore

from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider
from obstore.store import AzureStore

catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
)
collection = catalog.get_collection("daymet-daily-hi")
asset = collection.assets["zarr-abfs"]

azure_store = AzureStore(
    credential_provider=PlanetaryComputerCredentialProvider.from_asset(asset),
)

zarr_store = ObjectStore(azure_store, read_only=True)
ds = xr.open_dataset(zarr_store, consolidated=True, engine="zarr")

fig, ax = plt.subplots(figsize=(12, 12))
ds.sel(time="2009")["tmax"].mean(dim="time").plot.imshow(ax=ax, cmap="inferno")
fig.savefig("zarr-example.png")
