import pystac_client
import pyarrow.dataset as ds
import obstore
import obstore.auth.planetary_computer
import os

# Optional: Set environment variable if needed for Azure access
# os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "..." # Usually picked up from asset

# URL for the Microsoft Planetary Computer STAC API
STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
COLLECTION_ID = "landsat-c2-l2"
ASSET_KEY = "geoparquet-items" # The key for the GeoParquet asset

print(f"Opening STAC catalog: {STAC_URL}")
catalog = pystac_client.Client.open(STAC_URL)

print(f"Getting collection: {COLLECTION_ID}")
collection = catalog.get_collection(COLLECTION_ID)
if not collection:
    print(f"Error: Collection '{COLLECTION_ID}' not found.")
    exit()

print(f"Getting asset: {ASSET_KEY}")
if ASSET_KEY not in collection.assets:
    print(f"Error: Asset '{ASSET_KEY}' not found in collection '{COLLECTION_ID}'.")
    print(f"Available assets: {list(collection.assets.keys())}")
    exit()

asset = collection.assets[ASSET_KEY]
print(f"Asset Href: {asset.href}")

# Extract storage options if they exist
storage_options = asset.extra_fields.get("table:storage_options", {})
account_name = storage_options.get("account_name")
container_name = storage_options.get("container_name") # Sometimes needed

if not account_name:
    print("Warning: 'account_name' not found in asset's table:storage_options. Trying to proceed without it.")
    # Attempt to infer account name from href if possible, although PC usually provides it
    # This is less robust
    try:
        from urllib.parse import urlparse
        parsed_href = urlparse(asset.href)
        if parsed_href.netloc.endswith(".blob.core.windows.net"):
            account_name = parsed_href.netloc.split('.')[0]
            print(f"Inferred account_name: {account_name}")
        else:
             print("Could not infer account_name from href.")
    except Exception as e:
        print(f"Error parsing href to infer account_name: {e}")


print("Creating Planetary Computer credential provider...")
# Use the from_asset classmethod if available and robust
# Otherwise, manually create with href and account_name
try:
    # This is the preferred way if the asset structure is consistent
    credential_provider = obstore.auth.planetary_computer.PlanetaryComputerCredentialProvider.from_asset(asset)
    print("Created provider using from_asset.")
except Exception as e:
    print(f"Could not use from_asset ({e}), falling back to manual creation.")
    if not account_name:
         print("Error: Cannot create provider manually without 'account_name'.")
         exit()
    credential_provider = obstore.auth.planetary_computer.PlanetaryComputerCredentialProvider(
        asset.href,
        account_name=account_name,
    )
    print("Created provider manually.")


print("Initializing Obstore AzureStore...")
# Use the credential provider with AzureStore
# Need to provide the base URL for the storage account
# If container_name is available, use it, otherwise obstore might infer
if account_name:
    store_url = f"az://{container_name}" if container_name else f"az://" # obstore needs container in path or url
    azure_store = obstore.AzureStore(
        store_url, # Provide a base URL or let it infer
        credential_provider=credential_provider,
        account_name=account_name # Explicitly pass account_name
    )
    print(f"AzureStore initialized for account: {account_name}, URL base: {store_url}")
else:
    print("Error: Cannot initialize AzureStore without account_name.")
    exit()


# Create an fsspec filesystem interface from the obstore instance
print("Creating fsspec filesystem from obstore...")
fs = obstore.fsspec.ObstoreFileSystem(azure_store)
print("fsspec filesystem created.")

# The asset href usually points to the specific file or a base directory.
# We need the path relative to the root of the container for pyarrow dataset.
# Example: abfs://items/path/to/data.parquet -> items/path/to/data.parquet
try:
    from urllib.parse import urlparse
    parsed_href = urlparse(asset.href)
    # Path usually starts with the container name, remove it for relative path
    relative_path = parsed_href.path.lstrip('/')
    if container_name and relative_path.startswith(container_name + '/'):
        relative_path = relative_path[len(container_name) + 1:]

    print(f"Using relative path for PyArrow dataset: {relative_path}")

    # Load the dataset using pyarrow.dataset
    print("Loading dataset with pyarrow.dataset...")
    # Ensure format is specified if needed (usually inferred from extension)
    dataset = ds.dataset(relative_path, filesystem=fs, format="parquet")
    print("Dataset loaded successfully.")

    # Print schema or perform other operations
    print("\nDataset Schema:")
    print(dataset.schema)

    print("\nTaking first 5 rows:")
    print(dataset.head(5))

except ImportError:
    print("Error: urlparse requires 'urllib.parse'. This should be standard.")
except Exception as e:
    print(f"Error loading dataset: {e}")
    print("Ensure the relative path within the container is correct and accessible.")
    print(f"Asset href: {asset.href}")
    print(f"Derived relative path: {relative_path if 'relative_path' in locals() else 'N/A'}")

print("\nExample finished.") 