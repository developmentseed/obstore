# Overridden Defaults

In general, we wish to follow the upstream `object_store` as closely as possible, which should reduce the maintenance overhead here.

However, there are occasionally places where we want to diverge from the upstream decision making, and we document those here.

## Azure CLI

We always check for Azure CLI authentication as a fallback.

If we stuck with the upstream `object_store` default, you would need to pass `use_azure_cli=True` to check for Azure CLI credentials.

The Azure CLI is the [second-to-last Azure authentication method checked](https://github.com/apache/arrow-rs/blob/9c92a50b6d190ca9d0c74c3ccc69e348393d9246/object_store/src/azure/builder.rs#L1015-L1016) checked. So this only changes the default behavior for people relying on instance authentication. For those people, they can still pass `use_azure_cli=False`.
