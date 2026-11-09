"""Entrypoint from the repository root: uv run python3 -m consumer.banking_sink."""
import os
import sys

from loguru import logger

from .config import Settings
from .runner import KafkaS3Sink
from .storage import ParquetS3Writer


def main() -> int:
    logger.remove()
    logger.add(
        sys.stderr,
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
    settings = Settings.from_env()
    KafkaS3Sink(settings, ParquetS3Writer(settings)).run()
    return 0

if __name__ == "__main__":
    sys.exit(main())
