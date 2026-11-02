"""Write one partition's kafka CDC batch to S3-compatible Parquet"""
from __future__ import annotations

import hashlib
import io
import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Sequence

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger

from .config import Settings
from .events import CDCEvent

# A stable, nullable warehouse-friendly envelope, NOT a hardcoded banking table schema.
SCHEMA = pa.schema([
    pa.field("event_id", pa.string()),
    pa.field("topic", pa.string()),
    pa.field("kafka_partition", pa.int32()),
    pa.field("kafka_offset", pa.int64()),
    pa.field("kafka_timestamp_ms", pa.int64()),
    pa.field("source_database", pa.string()),
    pa.field("source_schema", pa.string()),
    pa.field("source_table", pa.string()),
    pa.field("op", pa.string()),
    pa.field("event_ts_ms", pa.int64()),
    pa.field("debezium_ts_ms", pa.int64()),
    pa.field("source_lsn", pa.string()),
    pa.field("source_txid", pa.string()),
    pa.field("snapshot", pa.string()),
    pa.field("key_json", pa.string()),
    pa.field("before_json", pa.string()),
    pa.field("after_json", pa.string()),
    pa.field("source_json", pa.string()),
    pa.field("payload_json", pa.string()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])

def _safe_segment(value: str) -> str:
    """Prevent accidental path traversal through dynamic table/topic names"""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", value).replace("..", "__")

class ParquetS3Writer:
    def __init__(self, settings: Settings, client=None):
        self.settings = settings
        self.s3 = client or boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.aws_region,
            config=Config(retries={"mode": "standard", "max_attempts": 5}),
        )

    def check_bucket(self) -> None:
        self.s3.head_bucket(Bucket=self.settings.s3_bucket)

    def put_batch(self, events: Sequence[CDCEvent]) -> str:
        if not events:
            raise ValueError("Cannot upload empty batch")
        topic, partition = events[0].topic, events[0].kafka_partition
        if any(e.topic != topic or e.kafka_partition != partition for e in events):
            raise ValueError("A parquet batch must contain exactly one topic partition")

        # offsets must strictly increase within a partition
        offsets = [e.kafka_offset for e in events]
        if offsets != sorted(offsets):
            raise ValueError("Offsets must be strictly increasing within a batch")

        table = _safe_segment(events[0].source_table)
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start, end = offsets[0], offsets[-1]
        path = (
            f"table={table}/ingest_date={day}/"
            f"topic={_safe_segment(topic)}/partition={partition}/"
            f"offsets={start:020d}-{end:020d}.parquet"
        )
        key = f"{self.settings.s3_prefix}/{path}" if self.settings.s3_prefix else path
        event_hash = hashlib.sha256(
            "\n".join(e.event_id for e in events).encode("utf-8")
        ).hexdigest()

        table_obj = pa.Table.from_pylist([asdict(e) for e in events], schema=SCHEMA)
        buffer = io.BytesIO()
        pq.write_table(table_obj, buffer, compression="snappy", version="2.6")
        body = buffer.getvalue()
        metadata = {
            "record-count": str(len(events)),
            "first-offset": str(start),
            "last-offset": str(end),
            "event-hash": event_hash,
        }
        try:
            # Conditional PUT avoids overwriting a previously persisted offset range.
            self.s3.put_object(
                Bucket=self.settings.s3_bucket,
                Key=key,
                Body=body,
                ContentType="application/octet-stream",
                Metadata=metadata,
                IfNoneMatch="*",
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", "")
            if status != 412 and code not in {"PreconditionFailed", "412"}:
                raise
            # Existing batch is only a success when it has the same kafka event ids.
            head = self.s3.head_object(Bucket=self.settings.s3_bucket, Key=key)
            existing = head.get("Metadata", {})
            if existing.get("event-hash") != event_hash:
                raise RuntimeError(f"S3 key collision with different events: {key}") from exc
            logger.info(f"Already landed identical batch: s3://{self.settings.s3_bucket}/{key}")
            return key
        logger.info(
            f"Landed {len(events)} events ({topic} [{partition}] offsets {start}..{end}, {len(body)}) at s3://{self.settings.s3_bucket}/{key})"
        )
        return key