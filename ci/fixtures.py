"""Deterministic Debezium-shaped CDC fixtures for the emulator dbt build.

Each scenario exists to prove one documented rule (docs/data_model.md); EXPECTED lists the
outcome ci/run_dbt_emulator.py asserts after `dbt build`.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

T0 = datetime(2026, 11, 1, tzinfo=timezone.utc)          # snapshot commit time
LOADED_AT = T0 + timedelta(hours=1)                      # warehouse COPY time
EPOCH = date(1970, 1, 1)


def _ms(minutes: float) -> int:
    return int((T0 + timedelta(minutes=minutes)).timestamp() * 1000)


def _days(d: date) -> int:
    """Debezium sends PostgreSQL DATE as epoch days when converter schemas are disabled."""
    return (d - EPOCH).days


def _ts(minutes: float) -> str:
    return (T0 + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


PK = {"fin_district": "district_id", "fin_client": "client_id", "fin_account": "account_id",
      "fin_disp": "disp_id", "fin_card": "card_id", "fin_loan": "loan_id",
      "fin_order": "order_id", "fin_trans": "trans_id"}


class Stream:
    """Builds RAW.CDC_EVENTS rows with monotonically increasing offsets and LSNs."""

    def __init__(self) -> None:
        self.rows: list[dict] = []
        self._offset: dict[str, int] = {}
        self._lsn = 1_000

    def emit(self, table: str, op: str, minutes: float, after: dict | None = None,
             before: dict | None = None, file_no: int = 1) -> dict:
        topic = f"banking_server.public.{table}"
        offset = self._offset.get(topic, 0)
        self._offset[topic] = offset + 1
        self._lsn += 8
        row_img = after or before or {}
        pk = PK[table]
        row = {
            "event_id": f"{topic}:0:{offset}", "topic": topic, "kafka_partition": 0,
            "kafka_offset": offset, "kafka_timestamp_ms": _ms(minutes) + 50,
            "source_database": "banking", "source_schema": "public", "source_table": table,
            "op": op, "record_pk": row_img.get(pk),
            "replay_event_id": row_img.get("last_replay_event_id"),
            "data_origin": row_img.get("data_origin"),
            "synthetic_batch_id": row_img.get("synthetic_batch_id"),
            "event_ts_ms": _ms(minutes), "debezium_ts_ms": _ms(minutes) + 20,
            "source_lsn": str(self._lsn), "source_txid": str(self._lsn // 8),
            "snapshot": "true" if op == "r" else "false",
            "key_json": json.dumps({pk: row_img.get(pk)}),
            "before_json": json.dumps(before) if before is not None else None,
            "after_json": json.dumps(after) if after is not None else None,
            "landing_ingested_at": LOADED_AT.isoformat(),
            "src_filename": f"landing/cdc/table={table}/offsets={file_no}.parquet",
            "src_file_row_number": offset,
            "raw_loaded_at": LOADED_AT,
        }
        self.rows.append(row)
        return row


CZ, FK = "czech_original", "faker_synthetic"
D1, D2, D3 = date(2013, 1, 5), date(2013, 1, 6), date(2013, 1, 7)


def _trans(tid, acct, d, amount, balance, ttype, op_code=None, category=None,
           bank=None, other=None, origin=CZ, batch=None, minutes=10.0):
    return {"trans_id": tid, "account_id": acct, "trans_date": _days(d),
            "amount": f"{amount:.2f}", "balance": f"{balance:.2f}", "trans_type": ttype,
            "operation": op_code, "category": category, "other_bank_id": bank,
            "other_account_id": other, "recorded_at": _ts(minutes),
            "synthetic_batch_id": batch, "data_origin": origin,
            "last_replay_event_id": None, "updated_at": _ts(minutes)}


def build() -> list[dict]:
    s = Stream()
    snap = 0.0
    # ---- Baseline snapshot (op r) -------------------------------------------------------
    for did, name, region in [(1, "Hl.m. Praha", "Prague"), (2, "Benesov", "central Bohemia")]:
        s.emit("fin_district", "r", snap, after={
            "district_id": did, "district_name": name, "region": region,
            "num_inhabitants": 100000, "num_municipalities_gt499": 0,
            "num_municipalities_500to1999": 1, "num_municipalities_2000to9999": 2,
            "num_municipalities_gt10000": 1, "num_cities": 1, "ratio_urban": "100.00",
            "average_salary": "12541.00", "unemployment_rate95": "0.29",
            "unemployment_rate96": "0.43", "num_entrep_per1000": 167,
            "num_crimes95": 85677, "num_crimes96": 99107})
    clients = [(1, 1, CZ, None), (2, 2, CZ, None), (900000001, 1, FK, 1)]
    for cid, did, origin, batch in clients:
        s.emit("fin_client", "r", snap, after={
            "client_id": cid, "birth_date": _days(date(1970, 12, 13)), "gender": "F",
            "district_id": did, "synthetic_batch_id": batch, "data_origin": origin,
            "last_replay_event_id": None, "updated_at": _ts(snap)})
    accounts = [(10, 1, CZ, None), (20, 2, CZ, None), (900000010, 1, FK, 1)]
    for aid, did, origin, batch in accounts:
        s.emit("fin_account", "r", snap, after={
            "account_id": aid, "district_id": did, "create_date": _days(date(2012, 3, 24)),
            "frequency": "M", "synthetic_batch_id": batch, "data_origin": origin,
            "last_replay_event_id": None, "updated_at": _ts(snap)})
    for disp, cid, aid, origin, batch in [(100, 1, 10, CZ, None), (101, 2, 20, CZ, None),
                                          (900000100, 900000001, 900000010, FK, 1)]:
        s.emit("fin_disp", "r", snap, after={
            "disp_id": disp, "client_id": cid, "account_id": aid, "disp_type": "O",
            "synthetic_batch_id": batch, "data_origin": origin,
            "last_replay_event_id": None, "updated_at": _ts(snap)})
    s.emit("fin_card", "r", snap, after={
        "card_id": 500, "disp_id": 100, "card_type": "C", "issued_date": _days(date(2012, 6, 1)),
        "data_origin": CZ, "last_replay_event_id": None, "updated_at": _ts(snap)})
    loan = {"loan_id": 600, "account_id": 20, "granted_date": _days(date(2012, 7, 1)),
            "amount": "1200.00", "duration": 12, "payments": "100.00", "status": "C",
            "data_origin": CZ, "last_replay_event_id": None, "updated_at": _ts(snap)}
    s.emit("fin_loan", "r", snap, after=loan)
    order = {"order_id": 700, "account_id": 10, "bank_to": "AB", "account_to": 12345,
             "amount": "50.00", "category": "SIPO", "data_origin": CZ,
             "last_replay_event_id": None, "updated_at": _ts(snap)}
    s.emit("fin_order", "r", snap, after=order)

    # ---- History replay (op c) and simulated changes (op u / d) ------------------------
    # Account 10: three clean days. ROB matches order 700, CCW matches card 500.
    s.emit("fin_trans", "c", 10, after=_trans(1, 10, D1, 1000, 1000, "C", "VKLAD", minutes=10))
    s.emit("fin_trans", "c", 11, after=_trans(2, 10, D1, -50, 950, "D", "ROB", "SIPO", "AB",
                                               12345, minutes=11))
    s.emit("fin_trans", "c", 12, after=_trans(3, 10, D2, -100, 850, "D", "CCW", minutes=12))
    # Simulated update: account 10 statement frequency (drives dim_account SCD2).
    s.emit("fin_account", "u", 30,
           before={"account_id": 10, "district_id": 1, "create_date": _days(date(2012, 3, 24)),
                   "frequency": "M", "synthetic_batch_id": None, "data_origin": CZ,
                   "last_replay_event_id": None, "updated_at": _ts(snap)},
           after={"account_id": 10, "district_id": 1, "create_date": _days(date(2012, 3, 24)),
                  "frequency": "W", "synthetic_batch_id": None, "data_origin": CZ,
                  "last_replay_event_id": 9001, "updated_at": _ts(30)})
    s.emit("fin_trans", "c", 40, after=_trans(4, 10, D3, 200, 1050, "C", "VKLAD", minutes=40))
    # Unmatched ROB: no partner details, so order_key must be -2.
    s.emit("fin_trans", "c", 41, after=_trans(5, 10, D3, -20, 1030, "D", "ROB", minutes=41))

    # Account 20: loan repayments either side of a loan-status update; day 3 stated balance
    # is deliberately wrong by 5.00, which must surface as VARIANCE.
    s.emit("fin_trans", "c", 15, after=_trans(6, 20, D1, 500, 500, "C", "VKLAD", minutes=15))
    s.emit("fin_trans", "c", 20, after=_trans(7, 20, D2, -100, 400, "D", "PREVOD", "LO",
                                               "CD", 777, minutes=20))
    s.emit("fin_loan", "u", 35, before=loan,
           after={**loan, "status": "D", "last_replay_event_id": 9002, "updated_at": _ts(35)})
    s.emit("fin_trans", "c", 50, after=_trans(8, 20, D3, -100, 305, "D", "PREVOD", "LO",
                                               "CD", 777, minutes=50))

    # Faker account: insert, then a simulated delete of a synthetic transaction.
    fk1 = _trans(900000001, 900000010, D1, 300, 300, "C", "VKLAD", origin=FK, batch=1, minutes=60)
    fk2 = _trans(900000002, 900000010, D2, -30, 270, "D", "VYBER", origin=FK, batch=1, minutes=61)
    s.emit("fin_trans", "c", 60, after=fk1)
    s.emit("fin_trans", "c", 61, after=fk2)
    s.emit("fin_trans", "d", 70, before=fk2)

    # Duplicate transport delivery: same Kafka coordinates landed again in another file.
    dup = dict(s.rows[-3]); dup["src_filename"] = dup["src_filename"].replace("offsets=1", "offsets=2")
    s.rows.append(dup)
    return s.rows


# Assertions run after `dbt build`. Each query must return exactly the expected rows.
EXPECTED: dict[str, tuple[str, list[tuple]]] = {
    "transport dedup keeps one row per Kafka coordinate": (
        "select count(*), count(distinct event_id) from SILVER.STG_CDC_EVENTS", [(27, 27)]),
    "delete is kept as is_deleted, not dropped": (
        "select transaction_id, is_deleted from GOLD.FACT_TRANSACTION "
        "where transaction_id = 900000002", [(900000002, True)]),
    "every fact row carries its real origin": (
        "select data_origin, count(*) from GOLD.FACT_TRANSACTION group by 1 order by 1",
        [("czech_original", 8), ("faker_synthetic", 2)]),
    "account SCD2: two versions, transaction picks the version in force at commit": (
        "select t.transaction_id, a.statement_frequency from GOLD.FACT_TRANSACTION t "
        "join GOLD.DIM_ACCOUNT a on a.account_key = t.account_key "
        "where t.transaction_id in (1, 4) order by 1", [(1, "M"), (4, "W")]),
    "loan SCD2: LO repayment points at the loan status in force at commit": (
        "select t.transaction_id, l.status_code from GOLD.FACT_TRANSACTION t "
        "join GOLD.DIM_LOAN l on l.loan_key = t.loan_key "
        "where t.transaction_id in (7, 8) order by 1", [(7, "C"), (8, "D")]),
    "conditional keys: matched, not applicable (-1) and unmatched (-2)": (
        "select transaction_id, order_key, card_key from GOLD.FACT_TRANSACTION "
        "where transaction_id in (1, 2, 3, 5) order by 1",
        [(1, -1, -1), (2, 700, -1), (3, -1, 500), (5, -2, -1)]),
    "daily balance reconciliation detects the injected 5.00 variance": (
        "select account_id, business_date, reconciliation_status, balance_variance "
        "from GOLD.FACT_ACCOUNT_BALANCE_DAILY where account_id in (10, 20) order by 1, 2",
        [(10, D1, "NO_COMPARABLE_PREVIOUS_DAY", None), (10, D2, "RECONCILED", 0),
         (10, D3, "RECONCILED", 0), (20, D1, "NO_COMPARABLE_PREVIOUS_DAY", None),
         (20, D2, "RECONCILED", 0), (20, D3, "VARIANCE", 5)]),
    "balance chain survives the account SCD2 change (one row per account-day)": (
        "select count(*) from GOLD.FACT_ACCOUNT_BALANCE_DAILY where account_id = 10", [(3,)]),
    "loan repayment progress": (
        "select payments_made, amount_repaid, amount_outstanding from GOLD.FACT_LOAN",
        [(2, 200, 1000)]),
}
