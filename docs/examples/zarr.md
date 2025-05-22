# Zarr

[Zarr-Python](https://zarr.readthedocs.io/en/stable/index.html) is a Python library for reading and writing the [Zarr file format](https://zarr.dev/) for N-dimensional arrays. Zarr-Python is often used in conjunction with [Xarray](https://xarray.dev/).

Zarr datasets are often very large and thus stored in object storage for cost effectiveness. As of Zarr-Python version 3.0.7 and later, you can [use Obstore as a backend](https://zarr.readthedocs.io/en/stable/user-guide/storage.html#object-store) for Zarr-Python. For large queries this [can be significantly faster](https://github.com/maxrjones/zarr-obstore-performance) than the default fsspec-based backend.

## Example

!!! note

    This example is also [available on Github](https://github.com/developmentseed/obstore/blob/main/examples/zarr/README.md) if you'd like to test it out locally.

```py
import matplotlib.pyplot as plt
import pystac_client
import xarray as xr
from zarr.storage import ObjectStore

from obstore.auth.planetary_computer import PlanetaryComputerCredentialProvider
from obstore.store import AzureStore

# These first lines are specific to Zarr stored in the Microsoft Planetary
# Computer. We use pystac-client to find the metadata for this specific Zarr
# store.
catalog = pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1/",
)
collection = catalog.get_collection("daymet-daily-hi")
asset = collection.assets["zarr-abfs"]

# We construct an AzureStore because this Zarr dataset is stored in Azure
# storage
azure_store = AzureStore(
    credential_provider=PlanetaryComputerCredentialProvider.from_asset(asset),
)

# Next we use the Zarr ObjectStorage adapter and pass it to xarray.
zarr_store = ObjectStore(azure_store, read_only=True)
ds = xr.open_dataset(zarr_store, consolidated=True, engine="zarr")

# And plot with matplotlib
fig, ax = plt.subplots(figsize=(12, 12))
ds.sel(time="2009")["tmax"].mean(dim="time").plot.imshow(ax=ax, cmap="inferno")
fig.savefig("zarr-example.png")
```

This plots:

![](../assets/zarr-example.png)
