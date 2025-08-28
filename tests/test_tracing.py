import json
from pathlib import Path
from tempfile import TemporaryDirectory

from obstore import init_log
from obstore.store import S3Store


def test_tracing(s3_store: S3Store):
    # Create a temp directory for logs
    with TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir) / "logs"
        log_file = "test_trace.log"

        init_log(log_dir, log_file, rotation="never", level="trace")

        _items = s3_store.list().collect()

        with (log_dir / log_file).open() as f:
            lines = [json.loads(line) for line in f if line.strip()]

        assert lines[0]["level"] == "TRACE"

        print(log_dir / log_file)  # noqa: T201
