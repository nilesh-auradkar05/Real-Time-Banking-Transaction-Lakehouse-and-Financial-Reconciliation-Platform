#!/usr/bin/env python3
"""Data generator for the banking CDC platform.

The Czech PKDD'99 data is static history. This script loads it into PostgreSQL and then makes
real inserts, updates and deletes there, which Debezium captures from the write-ahead log.

    uv run python data-generator/generator.py load
    uv run python data-generator/generator.py faker  --seed 42 --clients 100
    uv run python data-generator/generator.py replay --limit 5000 --speed-factor 0

Connection: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB (from .env or the environment),
POSTGRES_HOST (default localhost) and POSTGRES_PORT (default 5434, the compose mapping).
Authority for tables and columns: docs/data_models/*.erd.json. DDL: postgres/schema.sql.
"""
import argparse
import heapq
import os
import random
import re
import signal
import sys
import threading
import time
from collections import deque
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from faker import VERSION as FAKER_VERSION, Faker
from loguru import logger
from psycopg2.extras import execute_values

GENERATOR_VERSION = "1.0.0"
REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = REPO_ROOT / "postgres" / "schema.sql"
DEFAULT_DATASET = REPO_ROOT / "dataset" / "1999_Czech_financial_dataset_Teradata"

ORIGINAL = "czech_original"
SYNTHETIC = "faker_synthetic"
FAKER_ID_BASE = 900_000_000
FAKER_LOCALE = "cs_CZ"
INT_MAX = 2_147_483_647

# (table, tsv file, primary key, tsv columns, has data_origin). FK order.
DISTRICT_COLS = ["district_id", "district_name", "region", "num_inhabitants",
                 "num_municipalities_gt499", "num_municipalities_500to1999",
                 "num_municipalities_2000to9999", "num_municipalities_gt10000", "num_cities",
                 "ratio_urban", "average_salary", "unemployment_rate95", "unemployment_rate96",
                 "num_entrep_per1000", "num_crimes95", "num_crimes96"]
LOAD_TABLES = [
    ("fin_district", "fin_district.tsv", "district_id", DISTRICT_COLS, False),
    ("fin_account", "fin_account.tsv", "account_id",
     ["account_id", "district_id", "create_date", "frequency"], True),
    ("fin_client", "fin_client.tsv", "client_id",
     ["client_id", "birth_date", "gender", "district_id"], True),
    ("fin_disp", "fin_disp.tsv", "disp_id",
     ["disp_id", "client_id", "account_id", "disp_type"], True),
    ("fin_card", "fin_card.tsv", "card_id",
     ["card_id", "disp_id", "card_type", "issued_date"], True),
    ("fin_loan", "fin_loan.tsv", "loan_id",
     ["loan_id", "account_id", "granted_date", "amount", "duration", "payments", "status"], True),
    ("fin_order", "fin_order.tsv", "order_id",
     ["order_id", "account_id", "bank_to", "account_to", "amount", "category"], True),
]
TRANS_COLS = ["trans_id", "account_id", "trans_date", "amount", "balance", "trans_type",
              "operation", "category", "other_bank_id", "other_account_id"]

# Columns that must exist, so an older table of a different shape fails loudly.
_META = ["data_origin", "last_replay_event_id", "updated_at"]
EXPECTED_COLUMNS = {
    "replay_run": ["replay_run_id", "scenario", "random_seed", "speed_factor", "started_at",
                   "ended_at", "status"],
    "replay_event": ["replay_event_id", "replay_run_id", "source_table", "record_pk", "operation",
                     "data_origin", "business_date", "amount", "intended_commit_at",
                     "actual_commit_at", "injected_delay_seconds", "injected_fault"],
    "synthetic_batch": ["synthetic_batch_id", "generator_version", "faker_version", "random_seed",
                        "locale", "clients_created", "accounts_created", "transactions_created",
                        "business_date_from", "business_date_to", "created_at"],
    "synthetic_client_profile": ["client_id", "full_name", "email", "phone", "synthetic_batch_id"],
    "fin_district": DISTRICT_COLS,
    "fin_client": LOAD_TABLES[2][3] + ["synthetic_batch_id"] + _META,
    "fin_account": LOAD_TABLES[1][3] + ["synthetic_batch_id"] + _META,
    "fin_disp": LOAD_TABLES[3][3] + ["synthetic_batch_id"] + _META,
    "fin_card": LOAD_TABLES[4][3] + _META,
    "fin_loan": LOAD_TABLES[5][3] + _META,
    "fin_order": LOAD_TABLES[6][3] + _META,
    "fin_trans": TRANS_COLS + ["recorded_at", "synthetic_batch_id"] + _META,
}


class InvariantError(RuntimeError):
    """A requested change would break a documented rule."""


# ---------------------------------------------------------------------------------------------
# Connection and schema
# ---------------------------------------------------------------------------------------------
def connect():
    """Connect from environment variables. Never prints connection values."""
    load_dotenv()  # does not override variables already set in the environment
    missing = [k for k in ("POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB") if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)} (set them in .env)")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5434"))
    try:
        return psycopg2.connect(host=host, port=port, user=os.environ["POSTGRES_USER"],
                                password=os.environ["POSTGRES_PASSWORD"],
                                dbname=os.environ["POSTGRES_DB"],
                                application_name="banking-data-generator")
    except psycopg2.OperationalError:
        sys.exit(f"Could not connect to PostgreSQL at {host}:{port}. Is it running, and do "
                 "POSTGRES_USER, POSTGRES_PASSWORD and POSTGRES_DB match it?")


def check_schema(cur):
    """Fail loudly if a table is missing or lacks a column this script writes."""
    cur.execute("SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema()")
    have = {}
    for table, column in cur.fetchall():
        have.setdefault(table, set()).add(column)
    problems = []
    for table, cols in EXPECTED_COLUMNS.items():
        if table not in have:
            problems.append(f"table {table} is missing")
        elif set(cols) - have[table]:
            problems.append(f"{table} lacks columns {sorted(set(cols) - have[table])}")
    if problems:
        sys.exit("Schema problem (run `generator.py load` on an empty database, or drop the "
                 "old tables): " + "; ".join(problems))


def next_id(cur, table, column):
    """Next Faker id: one above the highest id in the table, never below 900,000,000."""
    cur.execute(f"SELECT COALESCE(MAX({column}), 0) FROM {table}")
    return max(cur.fetchone()[0] + 1, FAKER_ID_BASE)


# ---------------------------------------------------------------------------------------------
# load: schema + COPY of the Czech TSVs
# ---------------------------------------------------------------------------------------------
def load_table(cur, dataset_dir, table, tsv, pk, cols, has_origin):
    """COPY one TSV into a staging table, then insert the rows that are not there yet.

    The dataset's author already shifted dates by 20 years, divided amounts by 10 and
    translated the codes, so nothing is transformed here except: blank field -> NULL.
    Codes keep their padding (Silver trims them). Rows the replay has deleted are not
    brought back by a re-run.
    """
    col_list = ", ".join(cols)
    cur.execute(f"CREATE TEMP TABLE stg ON COMMIT DROP AS SELECT {col_list} FROM {table} WITH NO DATA")
    with open(dataset_dir / tsv, encoding="utf-8", newline="") as f:
        cur.copy_expert(f"COPY stg ({col_list}) FROM STDIN WITH (FORMAT text, NULL '')", f)
    cur.execute("SELECT count(*) FROM stg")
    in_file = cur.fetchone()[0]
    target_cols = col_list + (", data_origin" if has_origin else "")
    select_cols = ", ".join(f"s.{c}" for c in cols) + (", %s" if has_origin else "")
    params = ([ORIGINAL] if has_origin else []) + [table]
    cur.execute(
        f"INSERT INTO {table} ({target_cols}) SELECT {select_cols} FROM stg s "
        "WHERE NOT EXISTS (SELECT 1 FROM replay_event e WHERE e.source_table = %s "
        f"AND e.record_pk = s.{pk} AND e.operation = 'd' AND e.actual_commit_at IS NOT NULL) "
        "ON CONFLICT DO NOTHING", params)
    inserted = cur.rowcount
    cur.execute("DROP TABLE stg")
    return in_file, inserted


def cmd_load(conn, args):
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    conn.commit()
    check_schema(cur)
    for table, tsv, pk, cols, has_origin in LOAD_TABLES:
        in_file, inserted = load_table(cur, Path(args.dataset_dir), table, tsv, pk, cols, has_origin)
        conn.commit()
        logger.info(f"{table:13} file rows {in_file:>6}  inserted {inserted:>6}  skipped (present or deleted) {in_file - inserted:>6}")
    logger.info("fin_trans is not bulk-loaded: `replay` inserts it in business-date order.")
    return 0


# ---------------------------------------------------------------------------------------------
# faker: deterministic synthetic clients, accounts and transactions
# ---------------------------------------------------------------------------------------------
BANKS = ["AB", "CD", "EF", "GH", "IJ", "KL", "MN", "OP", "QR", "ST", "UV", "WX", "YZ"]
# (weight, type, operation, category, has partner, sign, min cents, max cents).
# No CCW, ROB or LO: Faker creates no cards, standing orders or loans, so those rows would
# show up as reconciliation exceptions.
TEMPLATES = [
    (30, "C", "CIC", "  ", False, 1, 1000, 500000),
    (20, "C", "COB", "  ", True, 1, 29000, 700000),
    (8, "C", "COB", "PE", True, 1, 40000, 70000),
    (12, "C", "   ", "IC", False, 1, 50, 5500),
    (35, "D", "WIC", "  ", False, -1, 1000, 300000),
    (12, "D", "WIC", "ST", False, -1, 150, 1000),
]
CREDIT_TEMPLATE = TEMPLATES[0]


def account_transactions(rng, account_id, create_date, date_to, n_tx):
    """n_tx transactions on distinct days: an opening credit, then a mix. Balance never < 0."""
    span = (date_to - create_date).days
    offsets = [0] + sorted(rng.sample(range(1, span + 1), n_tx - 1))
    balance_cents = 0
    rows = []
    for k, offset in enumerate(offsets):
        tpl = CREDIT_TEMPLATE if k == 0 else rng.choices(TEMPLATES, weights=[t[0] for t in TEMPLATES])[0]
        _, ttype, op, cat, partner, sign, lo, hi = tpl
        if k == 0:
            lo, hi = 10000, 500000  # opening deposit 100.00 to 5000.00
        if sign < 0 and min(hi, balance_cents) < lo:  # cannot afford a debit: take a credit
            _, ttype, op, cat, partner, sign, lo, hi = CREDIT_TEMPLATE
        if sign < 0:
            hi = min(hi, balance_cents)
        cents = rng.randint(lo, hi)
        balance_cents += sign * cents
        bank = rng.choice(BANKS) if partner else None
        other_account = rng.randrange(10_000_000, 100_000_000) if partner else None
        rows.append((create_date + timedelta(days=offset), account_id, ttype, op, cat, bank,
                     other_account, Decimal(sign * cents).scaleb(-2), Decimal(balance_cents).scaleb(-2)))
    return rows


def generate_faker_batch(seed, clients, tx_per_account, date_from, date_to, district_ids, bases):
    """Pure function: the same arguments always give the same rows. No clock, no global random.

    bases = first id for each table: client, account, disp, trans.
    """
    span = (date_to - date_from).days
    if clients < 1 or tx_per_account < 2 or span < tx_per_account:
        raise ValueError("need clients >= 1, tx_per_account >= 2 and a date range of at least "
                         "tx_per_account days")
    rng = random.Random(seed)
    fake = Faker(FAKER_LOCALE)
    fake.seed_instance(seed)
    n_accounts = max(1, min(clients, round(clients * 0.84)))  # the Czech data: 4500 accounts, 5369 clients

    client_rows, profile_rows = [], []
    for i in range(clients):
        client_id = bases["client"] + i
        gender = rng.choice("MF")
        name = fake.name_male() if gender == "M" else fake.name_female()
        slug = re.sub(r"[^a-z0-9]", "", fake.user_name().lower()) or "client"
        birth = date(1940, 1, 1) + timedelta(days=rng.randrange(0, 56 * 365))
        client_rows.append((client_id, birth, gender, rng.choice(district_ids)))
        profile_rows.append((client_id, name, f"{slug}.{client_id}@example.com", fake.phone_number()))

    account_rows, trans = [], []
    for i in range(n_accounts):
        account_id = bases["account"] + i
        create_offset = rng.randint(0, max(0, min(span // 2, span - tx_per_account)))
        create_date = date_from + timedelta(days=create_offset)
        frequency = rng.choices("MWT", weights=[92, 5, 3])[0]
        account_rows.append((account_id, rng.choice(district_ids), create_date, frequency))
        trans.extend(account_transactions(rng, account_id, create_date, date_to, tx_per_account))

    # Account i is owned by client i. The remaining clients are disponents on random accounts.
    disp_rows = []
    for i in range(clients):
        if i < n_accounts:
            account_id, disp_type = bases["account"] + i, "O"
        else:
            account_id, disp_type = bases["account"] + rng.randrange(n_accounts), "D"
        disp_rows.append((bases["disp"] + i, bases["client"] + i, account_id, disp_type))

    # Ids follow posting order; one transaction per account and day, so order is unambiguous.
    trans.sort(key=lambda t: (t[0], t[1]))
    trans_rows = [(bases["trans"] + n, t[1], t[0], t[7], t[8], t[2], t[3], t[4], t[5], t[6])
                  for n, t in enumerate(trans)]
    if bases["trans"] + len(trans_rows) > INT_MAX:
        raise ValueError("Faker transaction ids would overflow int")
    return {"clients": client_rows, "profiles": profile_rows, "accounts": account_rows,
            "disps": disp_rows, "trans": trans_rows}


def regenerate_batch(cur, batch_id):
    """Rebuild a stored batch from its synthetic_batch row alone (used to prove reproducibility)."""
    cur.execute("SELECT random_seed, clients_created, accounts_created, transactions_created, "
                "business_date_from, business_date_to FROM synthetic_batch WHERE synthetic_batch_id = %s",
                (batch_id,))
    seed, clients, accounts, transactions, d_from, d_to = cur.fetchone()
    cur.execute("SELECT district_id FROM fin_district ORDER BY district_id")
    districts = [r[0] for r in cur.fetchall()]
    bases = {}
    for key, table, col in [("client", "fin_client", "client_id"), ("account", "fin_account", "account_id"),
                            ("disp", "fin_disp", "disp_id"), ("trans", "fin_trans", "trans_id")]:
        cur.execute(f"SELECT MIN({col}) FROM {table} WHERE synthetic_batch_id = %s", (batch_id,))
        bases[key] = cur.fetchone()[0]
    return generate_faker_batch(seed, clients, transactions // accounts, d_from, d_to, districts, bases)


def insert_faker_batch(cur, batch_id, data):
    def add_batch(rows):
        return [r + (batch_id,) for r in rows]

    execute_values(cur, "INSERT INTO fin_client (client_id, birth_date, gender, district_id, "
                        "data_origin, synthetic_batch_id) VALUES %s", add_batch(data["clients"]),
                   template=f"(%s,%s,%s,%s,'{SYNTHETIC}',%s)", page_size=1000)
    execute_values(cur, "INSERT INTO fin_account (account_id, district_id, create_date, frequency, "
                        "data_origin, synthetic_batch_id) VALUES %s", add_batch(data["accounts"]),
                   template=f"(%s,%s,%s,%s,'{SYNTHETIC}',%s)", page_size=1000)
    execute_values(cur, "INSERT INTO fin_disp (disp_id, client_id, account_id, disp_type, "
                        "data_origin, synthetic_batch_id) VALUES %s", add_batch(data["disps"]),
                   template=f"(%s,%s,%s,%s,'{SYNTHETIC}',%s)", page_size=1000)
    execute_values(cur, "INSERT INTO synthetic_client_profile (client_id, full_name, email, phone, "
                        "synthetic_batch_id) VALUES %s", add_batch(data["profiles"]), page_size=1000)
    execute_values(cur, "INSERT INTO fin_trans (trans_id, account_id, trans_date, amount, balance, "
                        "trans_type, operation, category, other_bank_id, other_account_id, "
                        "data_origin, synthetic_batch_id) VALUES %s", add_batch(data["trans"]),
                   template=f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{SYNTHETIC}',%s)", page_size=1000)


def cmd_faker(conn, args):
    cur = conn.cursor()
    check_schema(cur)
    d_from, d_to = date.fromisoformat(args.date_from), date.fromisoformat(args.date_to)
    cur.execute("SELECT synthetic_batch_id FROM synthetic_batch WHERE random_seed = %s", (args.seed,))
    existing = cur.fetchone()
    if existing:
        logger.info(f"Batch {existing[0]} already exists for seed {args.seed}; nothing to do. "
                    "Use another --seed for a new batch.")
        return 0
    cur.execute("SELECT district_id FROM fin_district ORDER BY district_id")
    districts = [r[0] for r in cur.fetchall()]
    if not districts:
        sys.exit("fin_district is empty: run `generator.py load` first.")
    bases = {"client": next_id(cur, "fin_client", "client_id"), "account": next_id(cur, "fin_account", "account_id"),
             "disp": next_id(cur, "fin_disp", "disp_id"), "trans": next_id(cur, "fin_trans", "trans_id")}
    try:
        data = generate_faker_batch(args.seed, args.clients, args.tx_per_account, d_from, d_to, districts, bases)
    except ValueError as exc:
        sys.exit(str(exc))
    # One transaction: either the whole batch exists, or none of it.
    cur.execute("INSERT INTO synthetic_batch (generator_version, faker_version, random_seed, locale, "
                "clients_created, accounts_created, transactions_created, business_date_from, "
                "business_date_to) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING synthetic_batch_id",
                (GENERATOR_VERSION, FAKER_VERSION, args.seed, FAKER_LOCALE, len(data["clients"]),
                 len(data["accounts"]), len(data["trans"]), d_from, d_to))
    batch_id = cur.fetchone()[0]
    insert_faker_batch(cur, batch_id, data)
    conn.commit()
    logger.info(f"synthetic_batch {batch_id}: {len(data['clients'])} clients, {len(data['accounts'])} accounts, "
                f"{len(data['trans'])} transactions (seed {args.seed}, Faker {FAKER_VERSION}, {FAKER_LOCALE})")
    return 0


# ---------------------------------------------------------------------------------------------
# replay: history inserts, simulated updates and deletes
# ---------------------------------------------------------------------------------------------
# The only changes the simulator may make: (table, Debezium operation letter).
ALLOWED_CHANGES = {
    ("fin_trans", "c"), ("fin_trans", "d"),
    ("fin_client", "u"), ("fin_account", "u"), ("fin_loan", "u"), ("fin_order", "u"),
    ("fin_order", "d"), ("fin_card", "d"),
}
UPDATE_SQL = {
    "fin_client": "UPDATE fin_client SET district_id = %s, last_replay_event_id = %s, updated_at = %s WHERE client_id = %s",
    "fin_account": "UPDATE fin_account SET frequency = %s, last_replay_event_id = %s, updated_at = %s WHERE account_id = %s",
    "fin_loan": "UPDATE fin_loan SET status = %s, last_replay_event_id = %s, updated_at = %s WHERE loan_id = %s",
    "fin_order": "UPDATE fin_order SET amount = %s, last_replay_event_id = %s, updated_at = %s WHERE order_id = %s",
}
DELETE_SQL = {
    "fin_order": "DELETE FROM fin_order WHERE order_id = %s",
    "fin_card": "DELETE FROM fin_card WHERE card_id = %s",
}
LOAN_TRANSITIONS = {"C": "AD", "D": "BC"}  # running ok -> finished or in debt; in debt -> finished unpaid or ok
TRANS_INSERT = ("INSERT INTO fin_trans (trans_id, account_id, trans_date, amount, balance, trans_type, "
                "operation, category, other_bank_id, other_account_id, recorded_at, synthetic_batch_id, "
                "data_origin, last_replay_event_id, updated_at) VALUES %s RETURNING trans_id")
TRANS_TEMPLATE = f"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL,'{ORIGINAL}',%s,%s)"


def check_change_allowed(table, op, origin):
    """The one rule book. Original transactions are never updated or deleted."""
    if (table, op) not in ALLOWED_CHANGES:
        raise InvariantError(f"replay may not apply operation {op!r} to {table}")
    if table == "fin_trans":
        if op == "c" and origin != ORIGINAL:
            raise InvariantError("replay inserts only original transactions; Faker ones come from `faker`")
        if op != "c" and origin != SYNTHETIC:
            raise InvariantError("original (czech_original) transactions are never updated or deleted")


def delete_faker_transaction(cur, trans_id):
    """The only code path that deletes from fin_trans (there is no UPDATE path at all)."""
    if trans_id < FAKER_ID_BASE:
        raise InvariantError(f"trans_id {trans_id} is below {FAKER_ID_BASE}: refusing to delete")
    cur.execute("DELETE FROM fin_trans WHERE trans_id = %s AND data_origin = %s AND trans_id >= %s "
                "AND synthetic_batch_id IS NOT NULL", (trans_id, SYNTHETIC, FAKER_ID_BASE))
    if cur.rowcount != 1:
        raise InvariantError(f"expected to delete exactly one Faker transaction {trans_id}, got {cur.rowcount}")


def read_history(dataset_dir, done_ids, limit):
    """Raw fin_trans lines not yet in the database, earliest business date first (then trans_id)."""
    def lines():
        with open(dataset_dir / "fin_trans.tsv", encoding="utf-8", newline="") as f:
            for line in f:
                if int(line.split("\t", 1)[0]) not in done_ids:
                    yield line

    def key(line):
        parts = line.split("\t", 3)
        return parts[2], int(parts[0])

    return heapq.nsmallest(limit, lines(), key=key) if limit else sorted(lines(), key=key)


def history_item(line):
    f = line.rstrip("\n").split("\t")
    row = (int(f[0]), int(f[1]), f[2], Decimal(f[3]), Decimal(f[4]), f[5], f[6],
           f[7] or None, f[8] or None, int(f[9]) if f[9] else None)  # blank -> NULL, padding kept
    return {"table": "fin_trans", "pk": row[0], "op": "c", "origin": ORIGINAL, "amount": row[3],
            "date": date.fromisoformat(f[2]), "row": row}


def accounts_with_operation(dataset_dir, operation):
    accounts = set()
    with open(dataset_dir / "fin_trans.tsv", encoding="utf-8", newline="") as f:
        for line in f:
            parts = line.split("\t", 7)
            if parts[6] == operation:
                accounts.add(int(parts[1]))
    return accounts


def _fetch(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def plan_changes(cur, rng, n_updates, n_deletes, dataset_dir):
    """Choose simulated updates and deletes from the current database state.

    Round-robin over the kinds, so even a small run covers every kind. Each record is touched
    at most once per run. Deletes prefer records whose historical transactions stay consistent:
    leasing (LE) orders have no transactions, cards on accounts with no card withdrawals, and
    only the LAST transaction of a Faker account (so its running balance stays valid).
    """
    used, changes = set(), []

    def pool(rows, preferred=lambda r: False):
        shuffled = rng.sample(rows, len(rows))
        return deque([r for r in shuffled if preferred(r)] + [r for r in shuffled if not preferred(r)])

    def take(source, table):
        while source:
            row = source.popleft()
            if (table, row[0]) not in used:
                used.add((table, row[0]))
                return row
        return None

    districts = [r[0] for r in _fetch(cur, "SELECT district_id FROM fin_district ORDER BY 1")]
    upd_pools = {
        "fin_client": pool(_fetch(cur, "SELECT client_id, district_id, data_origin FROM fin_client ORDER BY 1")),
        "fin_account": pool(_fetch(cur, "SELECT account_id, frequency, data_origin FROM fin_account ORDER BY 1")),
        "fin_loan": pool(_fetch(cur, "SELECT loan_id, status, data_origin FROM fin_loan "
                                     "WHERE status IN ('C','D') ORDER BY 1")),
        "fin_order": pool(_fetch(cur, "SELECT order_id, amount, data_origin FROM fin_order ORDER BY 1")),
    }
    kinds = list(upd_pools)
    for i in range(n_updates):
        table = kinds[i % len(kinds)]
        row = take(upd_pools[table], table)
        if row is None:
            continue
        pk, current, origin = row
        amount = None
        if table == "fin_client":
            new = rng.choice([d for d in districts if d != current])
        elif table == "fin_account":
            new = rng.choice([f for f in "MWT" if f != current.strip()])
        elif table == "fin_loan":
            new = rng.choice(LOAN_TRANSITIONS[current])
        else:
            new = (current * Decimal(rng.randint(80, 125)) / 100).quantize(Decimal("0.01"), ROUND_HALF_UP)
            if new <= 0 or new == current:
                new = current + Decimal("1.00")
            amount = new
        changes.append({"table": table, "pk": pk, "op": "u", "origin": origin, "amount": amount, "value": new})

    orders = _fetch(cur, "SELECT order_id, amount, data_origin, category FROM fin_order ORDER BY 1")
    ccw = accounts_with_operation(dataset_dir, "CCW") if n_deletes and (dataset_dir / "fin_trans.tsv").exists() else set()
    cards = _fetch(cur, "SELECT c.card_id, d.account_id, c.data_origin FROM fin_card c "
                        "JOIN fin_disp d USING (disp_id) ORDER BY 1")
    tails = _fetch(cur, "SELECT DISTINCT ON (account_id) trans_id, amount, data_origin FROM fin_trans "
                        "WHERE data_origin = 'faker_synthetic' ORDER BY account_id, trans_id DESC")
    del_pools = {
        "fin_order": pool(orders, lambda r: (r[3] or "").strip() == "LE"),
        "fin_card": pool(cards, lambda r: r[1] not in ccw),
        "fin_trans": pool(tails),
    }
    kinds = list(del_pools)
    for i in range(n_deletes):
        table = kinds[i % len(kinds)]
        row = take(del_pools[table], table)
        if row is None:
            logger.warning(f"note: no candidate left for a {table} delete")
            continue
        if table == "fin_order":
            pk, amount, origin = row[0], row[1], row[2]
        elif table == "fin_card":
            pk, amount, origin = row[0], None, row[2]
        else:
            pk, amount, origin = row[0], row[1], row[2]
        changes.append({"table": table, "pk": pk, "op": "d", "origin": origin, "amount": amount})
    for ch in changes:
        check_change_allowed(ch["table"], ch["op"], ch["origin"])
    return changes


def write_events(conn, run_id, items, intended, delay, fault):
    """Write and COMMIT the replay_event rows for items, before the changes themselves."""
    for it in items:
        check_change_allowed(it["table"], it["op"], it["origin"])
    rows = [(run_id, it["table"], it["pk"], it["op"], it["origin"], it["date"], it["amount"], at,
             delay, fault) for it, at in zip(items, intended)]
    cur = conn.cursor()
    out = execute_values(cur, "INSERT INTO replay_event (replay_run_id, source_table, record_pk, operation, "
                              "data_origin, business_date, amount, intended_commit_at, "
                              "injected_delay_seconds, injected_fault) VALUES %s "
                              "RETURNING source_table, record_pk, operation, replay_event_id",
                         rows, page_size=1000, fetch=True)
    ids = {(t, pk, op): eid for t, pk, op, eid in out}
    for it in items:
        it["event_id"] = ids[(it["table"], it["pk"], it["op"])]
    conn.commit()


def apply_items(conn, items):
    """Apply already-recorded changes in one transaction and stamp actual_commit_at in it."""
    cur = conn.cursor()
    cur.execute("SELECT clock_timestamp()")
    ts = cur.fetchone()[0]
    inserts = [it for it in items if it["table"] == "fin_trans" and it["op"] == "c"]
    if inserts:
        rows = [it["row"] + (ts, it["event_id"], ts) for it in inserts]
        got = execute_values(cur, TRANS_INSERT, rows, template=TRANS_TEMPLATE, page_size=1000, fetch=True)
        if len(got) != len(inserts):
            raise RuntimeError("fin_trans insert did not return every row")
    for it in items:
        if it["op"] == "u":
            cur.execute(UPDATE_SQL[it["table"]], (it["value"], it["event_id"], ts, it["pk"]))
        elif it["op"] == "d" and it["table"] == "fin_trans":
            delete_faker_transaction(cur, it["pk"])
            continue
        elif it["op"] == "d":
            cur.execute(DELETE_SQL[it["table"]], (it["pk"],))
        else:
            continue
        if cur.rowcount != 1:
            raise RuntimeError(f"{it['table']} {it['pk']}: expected 1 row changed, got {cur.rowcount}")
    cur.execute("UPDATE replay_event SET actual_commit_at = %s WHERE replay_event_id = ANY(%s)",
                (ts, [it["event_id"] for it in items]))
    conn.commit()


def stream_items(history, changes, rng, fallback_date):
    """History in business-date order with each simulated change placed at a random point."""
    by_slot = {}
    for ch in changes:
        by_slot.setdefault(rng.randrange(max(1, len(history))), []).append(ch)
    if not history:
        for ch in by_slot.get(0, []):
            ch["date"] = fallback_date
            yield ch
        return
    for i, line in enumerate(history):
        item = history_item(line)
        yield item
        for ch in by_slot.get(i, []):
            ch["date"] = item["date"]  # a simulated change happens "on" the date replay has reached
            yield ch


def run_replay(conn, args):
    cur = conn.cursor()
    check_schema(cur)
    dataset_dir = Path(args.dataset_dir)
    cur.execute("SELECT count(*) FROM fin_account")
    if cur.fetchone()[0] == 0:
        sys.exit("fin_account is empty: run `generator.py load` first.")
    rng = random.Random(args.seed)
    cur.execute("SELECT trans_id FROM fin_trans WHERE data_origin = %s", (ORIGINAL,))
    done = {r[0] for r in cur.fetchall()}
    history = read_history(dataset_dir, done, args.limit)
    changes = plan_changes(cur, rng, args.updates, args.deletes, dataset_dir)
    cur.execute("SELECT COALESCE(max(trans_date), DATE '2018-12-31') FROM fin_trans")
    fallback_date = cur.fetchone()[0]
    logger.info(f"replaying {len(history)} history inserts ({len(done)} already in the database) "
                f"plus {len(changes)} simulated changes")

    cur.execute("INSERT INTO replay_run (scenario, random_seed, speed_factor, started_at, status) "
                "VALUES (%s, %s, %s, now(), 'running') RETURNING replay_run_id",
                (args.scenario, args.seed, Decimal(str(args.speed_factor)), ))
    run_id = cur.fetchone()[0]
    conn.commit()

    stop = threading.Event()

    def on_signal(signum, frame):
        if stop.is_set():
            raise KeyboardInterrupt  # second signal: stop right now (the open transaction rolls back)
        stop.set()
        logger.warning("stopping after the change in flight (press again to force)...")

    old_handlers = {s: signal.signal(s, on_signal) for s in (signal.SIGINT, signal.SIGTERM)}
    status = "failed"
    try:
        replay_loop(conn, args, run_id, rng, history, changes, fallback_date, stop)
        status = "interrupted" if stop.is_set() else "completed"
    except KeyboardInterrupt:
        conn.rollback()
        status = "interrupted"
    except BaseException:
        conn.rollback()
        raise
    finally:
        for s, h in old_handlers.items():
            signal.signal(s, h)
        cur = conn.cursor()
        cur.execute("UPDATE replay_run SET ended_at = now(), status = %s WHERE replay_run_id = %s", (status, run_id))
        conn.commit()
    cur.execute("SELECT source_table, operation, count(*), count(actual_commit_at) FROM replay_event "
                "WHERE replay_run_id = %s GROUP BY 1, 2 ORDER BY 1, 2", (run_id,))
    logger.info(f"replay_run {run_id} {status}")
    for table, op, planned, delivered in cur.fetchall():
        logger.info(f"  {table:12} {op}  planned {planned:>7}  delivered {delivered:>7}")
    return 130 if status == "interrupted" else 0


def replay_loop(conn, args, run_id, rng, history, changes, fallback_date, stop):
    speed = args.speed_factor
    start_mono, start_dt = time.monotonic(), datetime.now(timezone.utc)
    first_date = []
    held = deque()  # late changes: (release time, item); event already committed
    progress = {"applied": 0, "batches": 0}

    def schedule(item):
        """(intended commit time, monotonic due time). Unthrottled runs are due immediately."""
        if speed <= 0:
            return datetime.now(timezone.utc), time.monotonic()
        if not first_date:
            first_date.append(item["date"])
        offset = (item["date"] - first_date[0]).days * 86400 / speed
        return start_dt + timedelta(seconds=offset), start_mono + offset

    def wait_until(when):
        pause = when - time.monotonic()
        if pause > 0 and not stop.is_set():
            stop.wait(pause)

    def apply(items):
        apply_items(conn, items)
        progress["applied"] += len(items)
        progress["batches"] += 1
        if progress["batches"] % 25 == 0:
            logger.info(f"  applied {progress['applied']} changes (business date {items[-1]['date']})")

    def flush(items):
        if not items:
            return
        stamps = [schedule(it) for it in items]
        write_events(conn, run_id, items, [s[0] for s in stamps], 0, None)
        wait_until(stamps[-1][1])
        apply(items)

    def release_due():
        while held and held[0][0] <= time.monotonic():
            apply([held.popleft()[1]])

    pending = []
    for item in stream_items(history, changes, rng, fallback_date):
        if stop.is_set():
            break
        release_due()
        if args.late_fraction > 0 and rng.random() < args.late_fraction:
            intended, due = schedule(item)
            write_events(conn, run_id, [item], [intended], args.late_delay_seconds, "late_arrival")
            held.append((due + args.late_delay_seconds, item))
            continue
        if pending and (len(pending) >= args.batch_size or pending[0]["date"] != item["date"]):
            flush(pending)
            pending = []
        pending.append(item)
    if not stop.is_set():
        flush(pending)
    # Items without an event yet (pending) are dropped on a stop; late items already have an
    # event, so they are always delivered, immediately if we are stopping.
    while held:
        release, item = held.popleft()
        wait_until(release)
        apply([item])
    logger.info(f"  applied {progress['applied']} changes in {progress['batches']} transactions")


def cmd_replay(conn, args):
    return run_replay(conn, args)


# ---------------------------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------------------------
def non_negative_speed(text):
    value = float(text)
    if not 0 <= value < 1e8:
        raise argparse.ArgumentTypeError("must be between 0 and 99,999,999 (0 = no pacing)")
    return value


def fraction(text):
    value = float(text)
    if not 0 <= value <= 1:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return value


def build_parser():
    p = argparse.ArgumentParser(prog="generator.py", description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="command", required=True)

    load = sub.add_parser("load", help="create the schema and COPY the Czech TSVs (safe to re-run)")
    load.add_argument("--dataset-dir", default=str(DEFAULT_DATASET))
    load.set_defaults(func=cmd_load)

    fk = sub.add_parser("faker", help="insert one batch of synthetic clients, accounts and transactions")
    fk.add_argument("--seed", type=int, default=42, help="same seed, same data; one batch per seed")
    fk.add_argument("--clients", type=int, default=100)
    fk.add_argument("--tx-per-account", type=int, default=30, help="exact number per account, >= 2")
    fk.add_argument("--date-from", default="2013-01-01")
    fk.add_argument("--date-to", default="2018-12-31")
    fk.set_defaults(func=cmd_faker)

    rp = sub.add_parser("replay", help="replay fin_trans history plus simulated updates and deletes")
    rp.add_argument("--dataset-dir", default=str(DEFAULT_DATASET))
    rp.add_argument("--seed", type=int, default=42)
    rp.add_argument("--limit", type=int, default=None, help="replay at most N history transactions")
    rp.add_argument("--speed-factor", type=non_negative_speed, default=86400.0,
                    help="business seconds per wall second; 86400 = one business day per second; 0 = no pacing")
    rp.add_argument("--updates", type=int, default=50, help="simulated updates, spread over the 4 allowed kinds")
    rp.add_argument("--deletes", type=int, default=20, help="simulated deletes, spread over the 3 allowed kinds")
    rp.add_argument("--late-fraction", type=fraction, default=0.0, help="share of changes delivered late")
    rp.add_argument("--late-delay-seconds", type=int, default=30, help="wall-clock delay of a late change")
    rp.add_argument("--batch-size", type=int, default=200, help="changes per transaction (one business date at most)")
    rp.add_argument("--scenario", default="history_replay")
    rp.set_defaults(func=cmd_replay)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    conn = connect()
    try:
        return args.func(conn, args)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
