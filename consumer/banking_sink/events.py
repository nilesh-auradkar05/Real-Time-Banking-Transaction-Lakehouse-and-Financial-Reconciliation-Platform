"""Parse JSON-serialized Debezium records without assuming row schemas"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

ALLOWED_OPERATIONS = {"r", "c", "u", "d", "t", "m"}

def _json(obj: Any) -> str | None:
    if obj is None:
        return None
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

def _int_or_none(obj: Any) -> int | None:
    return None if obj is None or obj == "" else int(obj)

def _parse_json(data: bytes | None, label: str) -> Any:
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Malformed JSON {label}") from exc

def _unwrap(obj: Any) -> Any:
    # Kafka Connect JsonConverter with schemas.enable=True wraps the payload.
    if isinstance(obj, dict) and "schema" in obj and "payload" in obj:
        return obj["payload"]
    return obj


@dataclass(frozen=True)
class CDCEvent:
    event_id: str
    topic: str
    kafka_partition: int
    kafka_offset: int
    kafka_timestamp_ms: int | None
    source_database: str | None
    source_schema: str | None
    source_table: str
    op: str
    event_ts_ms: int | None
    debezium_ts_ms: int | None
    source_lsn: str | None
    source_txid: str | None
    snapshot: str | None
    key_json: str | None
    before_json: str | None
    after_json: str | None
    source_json: str | None
    payload_json: str | None
    ingested_at: datetime
    record_pk: int | None = None
    replay_event_id: int | None = None
    data_origin: str | None = None
    synthetic_batch_id: int | None = None

def parse_event(
    *, topic: str, partition: int, offset: int,
    kafka_timestamp_ms: int | None, key: bytes | None, value: bytes | None,
) -> CDCEvent:
    """Return one immutable landing record, including null-valued tombstones.

    Supports Debezium JSON with and without kafka connect schema wrappers.
    Avro/Protobuf or an ExtractNewRecordState SMT is NOT supported.
    """

    key_obj = _unwrap(_parse_json(key, "key"))
    raw_value = _parse_json(value, "value")
    payload = _unwrap(raw_value)
    if value is not None and not isinstance(payload, dict):
        raise ValueError("Debezium value must be a JSON object or tombstone")

    if value is None:
        source: dict[str, Any] = {}
        op = "tombstone"
    else:
        if "op" not in payload or "source" not in payload:
            raise ValueError("Expected Debezium envelope containing 'op' and 'source'")
        op = payload["op"]
        if op not in ALLOWED_OPERATIONS:
            raise ValueError(f"Unsupported CDC operation: {op!r}")
        source = payload["source"] or {}
        if not isinstance(source, dict):
            raise ValueError("Debezium source must be an object")

    source_table = str(source.get("table") or topic.rsplit(".", 1)[1])
    row = ((payload.get('after') or payload.get("before")) if value is not None else None) or {}
    if not isinstance(row, dict):
        raise ValueError("Debezium row image must be a JSON object")
    pk_map = {"fin_account": "account_id", "fin_client": "client_id", "fin_disp": "disp_id",
              "fin_card": "card_id", "fin_district": "district_id", "fin_loan": "loan_id",
              "fin_order": "order_id", "fin_trans": "trans_id", "replay_event": "replay_event_id"}
    pk_field = pk_map.get(source_table)
    record_pk = (row.get(pk_field) if pk_field else None)
    if record_pk is None and isinstance(key_obj, dict) and pk_field:
        record_pk = key_obj.get(pk_field)
    
    snapshot = source.get("snapshot")
    if snapshot is not None:
        snapshot = str(snapshot).lower()
    return CDCEvent(
        event_id=f"{topic}:{partition}:{offset}",
        topic=topic,
        kafka_partition=partition,
        kafka_offset=offset,
        kafka_timestamp_ms=kafka_timestamp_ms if kafka_timestamp_ms is not None and kafka_timestamp_ms >= 0 else None,
        source_database=source.get("db"),
        source_schema=source.get("schema"),
        source_table=source_table,
        op=op,
        event_ts_ms=_int_or_none(source.get("ts_ms")),
        debezium_ts_ms=_int_or_none(payload.get("ts_ms")) if value is not None else None,
        source_lsn=str(source["lsn"]) if source.get("lsn") is not None else None,
        source_txid=str(source["txId"]) if source.get("txId") is not None else None,
        snapshot=snapshot,
        key_json=_json(key_obj),
        before_json=_json(payload.get("before")) if value is not None else None,
        after_json=_json(payload.get("after")) if value is not None else None,
        source_json=_json(source) if value is not None else None,
        payload_json=_json(payload) if value is not None else None,
        ingested_at=datetime.now(timezone.utc),
        record_pk=_int_or_none(record_pk),
        replay_event_id=_int_or_none(row.get("last_replay_event_id") or 
        (row.get("replay_event_id") if source_table == "replay_event" else None)),
        data_origin=str(row["data_origin"]) if row.get("data_origin") is not None else None,
        synthetic_batch_id=_int_or_none(row.get("synthetic_batch_id")),
    )