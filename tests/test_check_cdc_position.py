import json

from scripts.check_cdc_position import find_problems, latest_lsn

KEY = json.dumps(["postgres-connector", {"server": "banking_server"}]).encode()


def _offset(lsn: int) -> bytes:
    return json.dumps({"lsn": lsn, "lsn_proc": lsn, "txId": 1}).encode()


def test_latest_record_wins_not_highest_lsn():
    records = [(KEY, _offset(900)), (KEY, None), (KEY, _offset(100))]  # reset, then new offsets
    assert latest_lsn(records, "postgres-connector") == 100


def test_reset_with_no_new_offset_means_none():
    assert latest_lsn([(KEY, _offset(900)), (KEY, None)], "postgres-connector") is None


def test_other_connectors_are_ignored():
    other = json.dumps(["other-connector", {"server": "x"}]).encode()
    assert latest_lsn([(other, _offset(900)), (KEY, _offset(5))], "postgres-connector") == 5


def test_2026_11_23_reinitialised_postgres_is_reported():
    saved, wal = 0x48C8C88, 0x2C9A538  # 0/48C8C88 saved, 0/2C9A538 current WAL
    problems = find_problems(saved, wal, (True, wal))
    assert len(problems) == 1
    assert "saved LSN 0/48C8C88 is ahead of the current WAL 0/2C9A538" in problems[0]


def test_healthy_position_has_no_problems():
    assert find_problems(100, 200, (True, 150)) == []
    assert find_problems(None, 200, (True, 150)) == []
