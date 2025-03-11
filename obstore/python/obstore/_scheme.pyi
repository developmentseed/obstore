from typing import Literal

def parse_scheme(
    url: str,
) -> Literal["s3", "gcs", "http", "local", "memory", "azure"]: ...
