from pathlib import Path
from typing import Literal, Self

class Logger:
    def __init__(
        self,
        dir: Path | str,  # noqa: A002
        prefix: str,
        *,
        suffix: str | None = None,
        max_log_files: int | None = None,
        rotation: Literal["minutely", "hourly", "daily", "never"] = "never",
        level: Literal["trace", "debug", "info", "warn", "error", "off"]
        | str
        | None = None,
    ) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: object,
        exc_value: object,
        traceback: object,
    ) -> None: ...
    def flush(self) -> None: ...

def init_log(
    dir: Path | str,  # noqa: A002
    prefix: str,
    *,
    suffix: str | None = None,
    max_log_files: int | None = None,
    rotation: Literal["minutely", "hourly", "daily", "never"] = "never",
    level: Literal["trace", "debug", "info", "warn", "error", "off"]
    | str
    | None = None,
) -> None:
    """Initialize Rust-level tracing to log to files in the specified directory.

    This function can only be called once per process. If tracing has already been initialized, this function will error without making any changes.

    Args:
        dir: Directory to write log files to.
        prefix: The prefix for log filenames. The prefix is output before the timestamp in the file name, and if it is non-empty, it is followed by a dot (`.`).

    Keyword Args:
        suffix: Sets the suffix for log filenames. The suffix is output after the timestamp in the file name, and if it is non-empty, it is preceded by a dot (.).
        max_log_files: Keeps the last n log files on disk.

            When a new log file is created, if there are `n` or more existing log files in the directory, the oldest will be deleted. If no value is supplied, no files will be removed.

            Files are considered candidates for deletion based on the following criteria:

            - The file must not be a directory or symbolic link.
            - If `prefix` is passed, the file name must start with that prefix.
            - If `suffix` is passed, the file name must end with that suffix.
            - If neither `prefix` nor `suffix` are passed, then the file name must parse as a valid date based on the date format.

            Files matching these criteria may be deleted if the maximum number of log files in the directory has been reached.
        rotation: How often to rotate log files. Options are:
            - "minutely": Create a new log file every minute.
            - "hourly": Create a new log file every hour.
            - "daily": Create a new log file every day.
            - "never": Do not rotate log files (all logs go to a single file).
        level: Minimum log level to record. Options include, from lowest to highest importance:
            - `"trace"`
            - `"debug"`
            - `"info"`
            - `"warn"`
            - `"error"`
            - `"off"`

            If not set explicitly, this will also check the `"RUST_LOG"` environment variable, falling back to `"info"` if neither is set.

    """
