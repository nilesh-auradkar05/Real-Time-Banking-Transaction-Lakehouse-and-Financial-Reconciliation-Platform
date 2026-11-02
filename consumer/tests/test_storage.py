import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError
from banking_sink.events import parse_event

pa = pytest.importorskip("pyarrow")
from consumer.banking_sink.config import Settings
from consumer.banking_sink.storage import ParquetS3Writer


class FakeS3:
    def __init__(self):
        self.objects = {}
    def put_object(self, **kw):
        key = kw["Key"]
        if key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"},
                               "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")
        self.objects[key] = kw
    def head_object(self, **kw):
        return {"Metadata": self.objects[kw["Key"]]["Metadata"]}
    def head_bucket(self, **kw):
        return {}


def settings():
    return Settings("localhost:9092", "g1", ("^banking\\.public\\..*$",), "earliest",
                    "bucket", "landing/cdc", None, "us-east-1", 100, 15)


def event(offset):
    return parse_event(topic="banking.public.trans", partition=0, offset=offset,
                       kafka_timestamp_ms=None, key=b'{"trans_id": 1}',
                       value=b'{"op":"r","source":{"table":"trans"},"after":{"id":1}}')


def test_parquet_upload_schema_and_idempotent_object():
    s3 = FakeS3()
    sink = ParquetS3Writer(settings(), client=s3)
    e = [event(4), event(5)]
    key = sink.put_batch(e)
    assert "table=trans/" in key
    assert "offsets=00000000000000000004-00000000000000000005.parquet" in key
    import io
    parquet = pa.parquet.read_table(io.BytesIO(s3.objects[key]["Body"]))
    assert parquet.num_rows == 2
    assert parquet.column("op").to_pylist() == ["r", "r"]
    assert sink.put_batch(e) == key
    assert len(s3.objects) == 1


def test_reject_mixed_partition_batches():
    sink = ParquetS3Writer(settings(), client=FakeS3())
    with pytest.raises(ValueError, match="one topic partition"):
        sink.put_batch([event(4), replace(event(5), kafka_partition=1)])
