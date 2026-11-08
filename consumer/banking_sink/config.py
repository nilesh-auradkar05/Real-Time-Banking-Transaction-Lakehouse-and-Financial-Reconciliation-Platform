"""Environment configuration for the streaming CDC landing service"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

def _int_env(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value

@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    consumer_group_id: str
    subscriptions: tuple[str, ...]
    auto_offset_reset: str
    s3_bucket: str
    s3_prefix: str
    s3_endpoint_url: str | None
    aws_region: str
    batch_size: int
    flush_interval_seconds: int
    dlq_prefix: str = "landing/dead_letter"
    malformed_event_policy: str = "quarantine"

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        explicit = tuple(
            item.strip() for item in os.getenv("KAFKA_TOPICS", "").split(",")
            if item.strip()
        )
        pattern = os.getenv("KAFKA_TOPIC_PATTERN", "").strip()
        subscriptions = explicit or ((pattern,) if pattern else ())
        if not subscriptions:
            raise ValueError("Set KAFKA_TOPICS or KAFKA_TOPIC_PATTERN")

        auto_offset_reset = os.getenv("AUTO_OFFSET_RESET", "earliest").lower()
        if auto_offset_reset not in {"earliest", "latest", "error"}:
            raise ValueError("AUTO_OFFSET_RESET must be earliest, latest or error")

        bucket = os.getenv("S3_BUCKET", "").strip()
        if not bucket:
            raise ValueError("S3_BUCKET must be set")
        policy = os.getenv("MALFORMED_EVENT_POLICY", "quarantine").lower()
        if policy not in {"quarantine", "fail"}:
            raise ValueError("MALFORMED_EVENT_POLICY must be quarantine or fail")
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            consumer_group_id=os.getenv("KAFKA_CONSUMER_GROUP", "banking-cdc-s3-v1"),
            subscriptions=subscriptions,
            auto_offset_reset=auto_offset_reset,
            s3_bucket=bucket,
            s3_prefix=os.getenv("S3_PREFIX", "landing/cdc").strip("/"),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", "").strip() or None,
            aws_region=os.getenv("AWS_REGION", "us-east-1"),
            batch_size=_int_env("BATCH_SIZE", 50000),
            flush_interval_seconds=_int_env("FLUSH_INTERVAL_SECONDS", 15),
            dlq_prefix=os.getenv("DLQ_PREFIX", "landing/dead_letter").strip("/"),
            malformed_event_policy=policy,
        )
