import json
import pytest
from banking_sink.events import parse_event


def parse(value, key=None):
    return parse_event(
        topic="banking.public.trans", partition=0, offset=42,
        kafka_timestamp_ms=1700000001234,
        key=json.dumps(key).encode() if key is not None else None,
        value=json.dumps(value).encode() if value is not None else None,
    )


def test_create_schema_less():
    event = parse({
        "before": None, "after": {"trans_id": 5, "amount": 100.25}, "op": "c",
        "source": {"db": "bank", "schema": "public", "table": "trans", "lsn": 128, "ts_ms": 12},
        "ts_ms": 15,
    }, {"trans_id": 5})
    assert event.event_id == "banking.public.trans:0:42"
    assert event.op == "c"
    assert json.loads(event.after_json)["amount"] == 100.25
    assert event.event_ts_ms == 12
    assert event.source_lsn == "128"


def test_schema_wrapped_update():
    event = parse({"schema": {}, "payload": {
        "before": {"account_id": 5}, "after": {"account_id": 5, "balance": 9},
        "source": {"table": "account"}, "op": "u",
    }}, {"schema": {}, "payload": {"account_id": 5}})
    assert event.op == "u"
    assert event.source_table == "account"
    assert json.loads(event.key_json) == {"account_id": 5}


def test_delete_and_tombstone_preserved():
    deleted = parse({"before": {"id": 1}, "after": None, "source": {}, "op": "d"})
    tombstone = parse(None, {"id": 1})
    assert deleted.op == "d"
    assert deleted.after_json is None
    assert tombstone.op == "tombstone"
    assert tombstone.payload_json is None
    assert json.loads(tombstone.key_json) == {"id": 1}


def test_snapshot_read():
    event = parse({"before": None, "after": {"id": 1}, "source": {"snapshot": "true"}, "op": "r"})
    assert event.op == "r"
    assert event.snapshot == "true"


def test_fail_closed_on_unrecognized_or_malformed_event():
    with pytest.raises(ValueError, match="Expected Debezium envelope"):
        parse({"id": 1})
    with pytest.raises(ValueError, match="Malformed JSON"):
        parse_event(topic="banking.public.trans", partition=0, offset=1,
                    kafka_timestamp_ms=None, key=None, value=b"not-json")
