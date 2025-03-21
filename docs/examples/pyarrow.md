# PyArrow

[PyArrow](https://arrow.apache.org/docs/python/index.html) is the canonical Python implementation for the Apache Arrow project.

PyArrow also supports reading and writing various file formats, including Parquet, CSV, JSON, and Arrow IPC.

PyArrow integration is supported [via its fsspec integration](https://arrow.apache.org/docs/python/filesystems.html#using-fsspec-compatible-filesystems-with-arrow), since Obstore [exposes an fsspec-compatible API](../integrations/fsspec.md).

```py
import pyarrow.parquet as pq

from obstore.fsspec import FsspecStore

fs = FsspecStore("s3", skip_signature=True, region="us-west-2")

url = "s3://overturemaps-us-west-2/release/2025-02-19.0/theme=addresses/type=address/part-00010-e084a2d7-fea9-41e5-a56f-e638a3307547-c000.zstd.parquet"
parquet_file = pq.ParquetFile(url, filesystem=fs)
print(parquet_file.schema_arrow)
```
prints:
```
id: string
geometry: binary
bbox: struct<xmin: float, xmax: float, ymin: float, ymax: float> not null
  child 0, xmin: float
  child 1, xmax: float
  child 2, ymin: float
  child 3, ymax: float
country: string
postcode: string
street: string
number: string
unit: string
address_levels: list<element: struct<value: string>>
  child 0, element: struct<value: string>
      child 0, value: string
postal_city: string
version: int32 not null
sources: list<element: struct<property: string, dataset: string, record_id: string, update_time: string, confidence: double>>
  child 0, element: struct<property: string, dataset: string, record_id: string, update_time: string, confidence: double>
      child 0, property: string
      child 1, dataset: string
      child 2, record_id: string
      child 3, update_time: string
      child 4, confidence: double
-- schema metadata --
geo: '{"version":"1.1.0","primary_column":"geometry","columns":{"geometry' + 230
org.apache.spark.legacyINT96: ''
org.apache.spark.version: '3.4.1'
org.apache.spark.sql.parquet.row.metadata: '{"type":"struct","fields":[{"' + 1586
org.apache.spark.legacyDateTime: ''
```
