from pathlib import Path
from tempfile import TemporaryDirectory

from obstore import init_log
from obstore.store import S3Store


def test_tracing(s3_store: S3Store):
    # Create a temp directory for logs
    with TemporaryDirectory(delete=False) as temp_dir:
        log_dir = Path(temp_dir) / "logs"
        log_file = "test_trace.log"

        stderr_config = {
            "format": "json",
            "show_ansi": False,
            "show_target": False,
            "show_level": False,
            "level": "trace",
        }
        stdout_config = {
            "format": "pretty",
            "show_ansi": True,
            "show_target": True,
            "show_level": True,
            "level": "debug",
        }
        init_log(stderr=stderr_config, stdout=stdout_config, level="trace")

        _items = s3_store.list().collect()

        # with (log_dir / log_file).open() as f:
        #     lines = [json.loads(line) for line in f if line.strip()]

        # assert lines[0]["level"] == "TRACE"

        print(log_dir / log_file)  # noqa: T201
