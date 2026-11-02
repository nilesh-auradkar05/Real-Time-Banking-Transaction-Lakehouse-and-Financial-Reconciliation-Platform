#!/usr/bin/env python3
"""End-to-end check of generator.py with plain asserts. Needs an EMPTY throwaway PostgreSQL.

    docker run --rm -d --name gen-test-pg -p 127.0.0.1:55432:5432 \
        -e POSTGRES_USER=gentest -e POSTGRES_PASSWORD=<pw> -e POSTGRES_DB=gentest postgres:15
    POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=55432 POSTGRES_USER=gentest POSTGRES_PASSWORD=<pw> \
        POSTGRES_DB=gentest uv run --frozen python data-generator/test_generator.py

Refuses to run against port 5434 (the compose database) or against a database that already
has tables. Runs: load, load again, faker, faker again, replay, replay again, replay + Ctrl-C.
"""
import os
import signal
import subprocess
import sys
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import psycopg2
from loguru import logger

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import generator as g  # noqa: E402

DATASET = g.DEFAULT_DATASET
TOLERANCE = Decimal("0.02")


def tsv_rows(name):
    with open(DATASET / name, encoding="utf-8", newline="") as f:
        return [line.rstrip("\n").split("\t") for line in f]


def wait_for_db(conn_factory, seconds=60):
    deadline = time.time() + seconds
    while True:
        try:
            return conn_factory()
        except SystemExit:
            if time.time() > deadline:
                raise
            time.sleep(1)


def main():
    for key in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"):
        assert os.environ.get(key), f"set {key} explicitly so .env can never point this test at a real database"
    assert os.environ["POSTGRES_PORT"] != "5434", "refusing to run against the compose database port 5434"

    conn = wait_for_db(g.connect)
    conn.autocommit = True
    cur = conn.cursor()

    def q(sql, params=None):
        cur.execute(sql, params)
        return cur.fetchall()

    def one(sql, params=None):
        return q(sql, params)[0][0]

    assert one("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'") == 0, \
        "database is not empty: use a throwaway one"

    def run(*argv):
        assert g.main(list(argv)) in (0, 130)

    # ------------------------------------------------------------------ load
    run("load")
    expected = {"fin_district": 77, "fin_account": 4500, "fin_client": 5369, "fin_disp": 5369,
                "fin_card": 892, "fin_loan": 682, "fin_order": 6471}
    for table, n in expected.items():
        file_rows = len(tsv_rows(table + ".tsv"))
        assert file_rows == n, f"{table}: TSV has {file_rows}"
        assert one(f"SELECT count(*) FROM {table}") == n, f"{table} row count"
    assert one("SELECT count(*) FROM fin_trans") == 0, "load must not bulk-load fin_trans"
    for table in ("fin_client", "fin_account", "fin_disp", "fin_card", "fin_loan", "fin_order"):
        assert one(f"SELECT count(*) FROM {table} WHERE data_origin <> 'czech_original'") == 0
    for table in ("fin_client", "fin_account", "fin_disp"):
        assert one(f"SELECT count(*) FROM {table} WHERE synthetic_batch_id IS NOT NULL") == 0
    assert one("SELECT count(*) FROM fin_district WHERE unemployment_rate95 IS NULL") == 1  # district 69 only
    assert one("SELECT count(*) FROM fin_order WHERE category = '  '") == 1379, "padding must be kept"
    logger.info("load: row counts match the TSVs, origin/batch labels correct")

    run("load")
    for table, n in expected.items():
        assert one(f"SELECT count(*) FROM {table}") == n, f"{table} duplicated by a second load"
    logger.info("load again: no duplicates")

    # ------------------------------------------------------------------ faker
    run("faker", "--seed", "7", "--clients", "40", "--tx-per-account", "25")
    batch_id = one("SELECT synthetic_batch_id FROM synthetic_batch WHERE random_seed = 7")
    seed, faker_ver, gen_ver, locale, n_clients, n_accounts, n_trans = q(
        "SELECT random_seed, faker_version, generator_version, locale, clients_created, accounts_created, "
        "transactions_created FROM synthetic_batch WHERE synthetic_batch_id = %s", (batch_id,))[0]
    assert (seed, faker_ver, gen_ver, locale) == (7, g.FAKER_VERSION, g.GENERATOR_VERSION, "cs_CZ")
    assert (n_clients, n_accounts, n_trans) == (40, 34, 34 * 25)
    for table, col, n in [("fin_client", "client_id", n_clients), ("fin_account", "account_id", n_accounts),
                          ("fin_disp", "disp_id", n_clients), ("fin_trans", "trans_id", n_trans)]:
        assert one(f"SELECT count(*) FROM {table} WHERE data_origin = 'faker_synthetic'") == n, table
        assert one(f"SELECT count(*) FROM {table} WHERE data_origin = 'faker_synthetic' AND "
                   f"(synthetic_batch_id IS DISTINCT FROM %s OR {col} < 900000000)", (batch_id,)) == 0, table
        assert one(f"SELECT count(*) FROM {table} WHERE data_origin = 'czech_original' AND "
                   f"(synthetic_batch_id IS NOT NULL OR {col} >= 900000000)") == 0, table
        assert one(f"SELECT min({col}) FROM {table} WHERE data_origin = 'faker_synthetic'") == 900_000_000, table
    assert one("SELECT count(*) FROM synthetic_client_profile") == n_clients
    assert one("SELECT count(DISTINCT email) FROM synthetic_client_profile") == n_clients
    assert one("SELECT count(*) FROM fin_trans t JOIN fin_account a USING (account_id) "
               "WHERE t.data_origin = 'faker_synthetic' AND a.data_origin <> 'faker_synthetic'") == 0, \
        "Faker transaction on an original account"
    assert one("SELECT count(*) FROM fin_trans t JOIN fin_account a USING (account_id) "
               "WHERE t.data_origin = 'czech_original' AND a.data_origin <> 'czech_original'") == 0
    assert one("SELECT count(*) FROM fin_trans WHERE operation IN ('CCW','ROB') OR category = 'LO'") == 0
    assert one("SELECT count(*) FROM fin_account a WHERE a.data_origin = 'faker_synthetic' AND "
               "(SELECT count(*) FROM fin_disp d WHERE d.account_id = a.account_id AND d.disp_type = 'O') <> 1") == 0
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'faker_synthetic' AND balance < 0") == 0
    # the check must also fail the way it should: the constraint ties id range, origin and batch
    try:
        cur.execute("INSERT INTO fin_client (client_id, birth_date, gender, district_id, data_origin) "
                    "VALUES (5, DATE '1980-01-01', 'M', 1, 'faker_synthetic')")
        raise AssertionError("CHECK constraint did not reject a Faker row with a low id")
    except psycopg2.errors.CheckViolation:
        pass
    logger.info(f"faker: batch {batch_id} labelled and ranged correctly, Faker transactions only on Faker accounts")

    assert_faker_balances(q, "after faker")

    # reproducibility: the stored batch row alone regenerates exactly what is in the database
    again = g.regenerate_batch(cur, batch_id)
    db = {
        "clients": q("SELECT client_id, birth_date, gender, district_id FROM fin_client "
                     "WHERE synthetic_batch_id = %s ORDER BY 1", (batch_id,)),
        "accounts": q("SELECT account_id, district_id, create_date, frequency FROM fin_account "
                      "WHERE synthetic_batch_id = %s ORDER BY 1", (batch_id,)),
        "disps": q("SELECT disp_id, client_id, account_id, disp_type FROM fin_disp "
                   "WHERE synthetic_batch_id = %s ORDER BY 1", (batch_id,)),
        "profiles": q("SELECT client_id, full_name, email, phone FROM synthetic_client_profile "
                      "WHERE synthetic_batch_id = %s ORDER BY 1", (batch_id,)),
        "trans": q("SELECT trans_id, account_id, trans_date, amount, balance, trans_type, operation, category, "
                   "other_bank_id, other_account_id FROM fin_trans WHERE synthetic_batch_id = %s ORDER BY 1",
                   (batch_id,)),
    }
    for key, rows in db.items():
        assert [tuple(r) for r in rows] == [tuple(r) for r in sorted(again[key])], f"regenerated {key} differ"
    districts = [r[0] for r in q("SELECT district_id FROM fin_district ORDER BY 1")]
    bases = {"client": 1, "account": 1, "disp": 1, "trans": 1}
    args = (7, 40, 25, date(2013, 1, 1), date(2018, 12, 31), districts, bases)
    assert g.generate_faker_batch(*args) == g.generate_faker_batch(*args), "same seed must give same data"
    assert g.generate_faker_batch(8, *args[1:]) != g.generate_faker_batch(*args), "different seeds should differ"
    before = (one("SELECT count(*) FROM synthetic_batch"), one("SELECT count(*) FROM fin_trans"))
    run("faker", "--seed", "7", "--clients", "40", "--tx-per-account", "25")
    assert before == (one("SELECT count(*) FROM synthetic_batch"), one("SELECT count(*) FROM fin_trans"))
    logger.info("faker: same seed regenerates the stored batch exactly, and a repeat adds nothing")

    # ------------------------------------------------------------------ replay (first run)
    faker_trans_before = one("SELECT count(*) FROM fin_trans WHERE data_origin = 'faker_synthetic'")
    order_before, card_before = expected["fin_order"], expected["fin_card"]
    run("replay", "--seed", "5", "--limit", "300", "--updates", "8", "--deletes", "9", "--speed-factor", "0",
        "--late-fraction", "0.2", "--late-delay-seconds", "1", "--batch-size", "50")
    run_id = one("SELECT max(replay_run_id) FROM replay_run")
    status, speed, ended, seed_stored = q("SELECT status, speed_factor, ended_at, random_seed FROM replay_run "
                                          "WHERE replay_run_id = %s", (run_id,))[0]
    assert status == "completed" and ended is not None and speed == 0 and seed_stored == 5

    history = sorted_history()
    assert_original_trans_match_tsv(q, one)
    ids = {r[0] for r in q("SELECT trans_id FROM fin_trans WHERE data_origin = 'czech_original'")}
    assert ids == {int(h[0]) for h in history[:300]}, "replay did not take the 300 earliest business dates"

    # inserts: keep trans_date, separate recorded_at, linked to their event
    assert one("SELECT count(*) FROM fin_trans t JOIN replay_event e ON e.replay_event_id = t.last_replay_event_id "
               "WHERE t.data_origin = 'czech_original' AND e.source_table = 'fin_trans' AND e.record_pk = t.trans_id "
               "AND e.operation = 'c' AND e.actual_commit_at IS NOT NULL AND e.replay_run_id = %s",
               (run_id,)) == 300, "insert without its event"
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original' AND recorded_at < now() - interval '1 hour'") == 0
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original' AND trans_date >= DATE '2019-01-01'") == 0

    # every event delivered; updates and deletes covered; every changed row has its event
    assert one("SELECT count(*) FROM replay_event WHERE replay_run_id = %s AND actual_commit_at IS NULL", (run_id,)) == 0
    counts = {(t, o): n for t, o, n in q("SELECT source_table, operation, count(*) FROM replay_event "
                                         "WHERE replay_run_id = %s GROUP BY 1, 2", (run_id,))}
    for table in ("fin_client", "fin_account", "fin_loan", "fin_order"):
        assert counts[(table, "u")] == 2, f"updates on {table}: {counts.get((table, 'u'))}"
        changed = one(f"SELECT count(*) FROM {table} WHERE last_replay_event_id IS NOT NULL")
        linked = one(f"SELECT count(*) FROM {table} t JOIN replay_event e ON e.replay_event_id = t.last_replay_event_id "
                     f"WHERE e.source_table = %s AND e.operation = 'u' AND e.actual_commit_at IS NOT NULL", (table,))
        assert changed == linked == 2, f"{table}: {changed} changed rows, {linked} linked to events"
    for table in ("fin_order", "fin_card", "fin_trans"):
        assert counts[(table, "d")] == 3, f"deletes on {table}"
    assert counts[("fin_trans", "c")] == 300 and len(counts) == 8
    assert one("SELECT count(*) FROM fin_order") == order_before - 3
    assert one("SELECT count(*) FROM fin_card") == card_before - 3
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'faker_synthetic'") == faker_trans_before - 3
    for table, pk in (("fin_order", "order_id"), ("fin_card", "card_id"), ("fin_trans", "trans_id")):
        assert one(f"SELECT count(*) FROM replay_event e JOIN {table} t ON t.{pk} = e.record_pk "
                   f"WHERE e.operation = 'd' AND e.source_table = %s", (table,)) == 0, "deleted row still there"
    assert one("SELECT count(*) FROM replay_event WHERE operation = 'd' AND source_table = 'fin_trans' "
               "AND data_origin <> 'faker_synthetic'") == 0
    # simulated updates only touch what the docs allow, and never relabel a row
    assert one("SELECT count(*) FROM fin_loan WHERE status NOT IN ('A','B','C','D')") == 0
    assert one("SELECT count(*) FROM fin_order WHERE amount <= 0") == 0
    logger.info("replay: history, updates, deletes all have delivered events; original transactions only inserted")

    # late arrivals are really late
    late = q("SELECT injected_delay_seconds, injected_fault, extract(epoch FROM actual_commit_at - intended_commit_at) "
             "FROM replay_event WHERE replay_run_id = %s AND injected_fault IS NOT NULL", (run_id,))
    assert late and all(d == 1 and f == "late_arrival" and lag >= 0.9 for d, f, lag in late), late
    assert one("SELECT count(*) FROM replay_event WHERE replay_run_id = %s AND injected_fault IS NULL "
               "AND injected_delay_seconds = 0", (run_id,)) > 0
    out_of_order = one("SELECT count(*) FROM replay_event a JOIN replay_event b ON a.replay_run_id = b.replay_run_id "
                       "WHERE a.replay_run_id = %s AND a.injected_fault IS NOT NULL AND b.injected_fault IS NULL "
                       "AND b.business_date > a.business_date AND b.actual_commit_at < a.actual_commit_at", (run_id,))
    assert out_of_order > 0 or len({r[0] for r in q("SELECT business_date FROM replay_event")}) == 1, \
        "late events did not arrive after later business dates"
    logger.info(f"replay: {len(late)} late events, all delayed >= 1 second")

    assert_faker_balances(q, "after replay")

    # ------------------------------------------------------------------ hard invariant
    orig_id = one("SELECT min(trans_id) FROM fin_trans WHERE data_origin = 'czech_original'")
    n_orig = one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original'")
    for call in (lambda: g.check_change_allowed("fin_trans", "u", g.ORIGINAL),
                 lambda: g.check_change_allowed("fin_trans", "d", g.ORIGINAL),
                 lambda: g.check_change_allowed("fin_client", "d", g.ORIGINAL),
                 lambda: g.check_change_allowed("fin_loan", "d", g.ORIGINAL),
                 lambda: g.delete_faker_transaction(cur, orig_id)):
        try:
            call()
            raise AssertionError("invariant not enforced in code")
        except g.InvariantError:
            pass
    for sql in ("UPDATE fin_trans SET amount = amount + 1 WHERE trans_id = %s", "DELETE FROM fin_trans WHERE trans_id = %s"):
        try:
            cur.execute(sql, (orig_id,))
            raise AssertionError("the database trigger let an original transaction change")
        except psycopg2.errors.RaiseException:
            pass
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original'") == n_orig
    logger.info("invariant: refused in code (4 cases) and by the trigger (update, delete)")

    # ------------------------------------------------------------------ replay again (resume)
    run("replay", "--seed", "6", "--limit", "200", "--updates", "0", "--deletes", "0", "--speed-factor", "0")
    ids = {r[0] for r in q("SELECT trans_id FROM fin_trans WHERE data_origin = 'czech_original'")}
    assert ids == {int(h[0]) for h in history[:500]}, "second run did not continue where the first stopped"
    assert one("SELECT count(*) FROM replay_event WHERE source_table = 'fin_trans' AND operation = 'c'") == 500
    logger.info("replay again: continued with the next 200, no duplicate events or rows")

    # ------------------------------------------------------------------ Ctrl-C
    env = dict(os.environ)
    proc = subprocess.Popen([sys.executable, str(HERE / "generator.py"), "replay", "--seed", "9", "--limit", "4000",
                             "--updates", "4", "--deletes", "0", "--speed-factor", "86400"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    stop_run = None
    deadline = time.time() + 90
    while time.time() < deadline and proc.poll() is None:
        rows = q("SELECT r.replay_run_id FROM replay_run r WHERE r.status = 'running' AND EXISTS "
                 "(SELECT 1 FROM replay_event e WHERE e.replay_run_id = r.replay_run_id AND e.actual_commit_at IS NOT NULL)")
        if rows:
            stop_run = rows[0][0]
            break
        time.sleep(0.2)
    assert stop_run is not None, "paced replay never started"
    time.sleep(1.5)
    proc.send_signal(signal.SIGINT)
    output, _ = proc.communicate(timeout=60)
    assert proc.returncode == 130, (proc.returncode, output[-500:])
    assert one("SELECT status FROM replay_run WHERE replay_run_id = %s", (stop_run,)) == "interrupted"
    assert one("SELECT ended_at IS NOT NULL FROM replay_run WHERE replay_run_id = %s", (stop_run,))
    assert one("SELECT count(*) FROM replay_event WHERE replay_run_id = %s AND actual_commit_at IS NULL", (stop_run,)) == 0
    done_now = one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original'")
    assert 500 <= done_now < 4500, done_now
    ids = {r[0] for r in q("SELECT trans_id FROM fin_trans WHERE data_origin = 'czech_original'")}
    assert ids == {int(h[0]) for h in history[:done_now]}, "interrupted run left a gap in business-date order"
    assert one("SELECT count(*) FROM fin_trans t LEFT JOIN replay_event e ON e.replay_event_id = t.last_replay_event_id "
               "WHERE t.data_origin = 'czech_original' AND e.replay_event_id IS NULL") == 0
    run("replay", "--seed", "10", "--limit", "100", "--updates", "0", "--deletes", "0", "--speed-factor", "0")
    assert one("SELECT count(*) FROM fin_trans WHERE data_origin = 'czech_original'") == done_now + 100
    logger.info(f"Ctrl-C: run {stop_run} interrupted cleanly after {done_now - 500} of 4000, resumed with no gap")

    assert_original_trans_match_tsv(q, one)
    assert_faker_balances(q, "at the end")

    # ------------------------------------------------------------------ event before change
    # Make the change step fail: the events must already be committed, and no change may exist.
    real_apply = g.apply_items

    def failing_apply(conn_, items):
        raise RuntimeError("injected failure")

    g.apply_items = failing_apply
    before_rows = one("SELECT count(*) FROM fin_trans")
    try:
        g.main(["replay", "--seed", "11", "--limit", "5", "--updates", "0", "--deletes", "0", "--speed-factor", "0"])
        raise AssertionError("injected failure did not surface")
    except RuntimeError:
        pass
    finally:
        g.apply_items = real_apply
    bad_run = one("SELECT max(replay_run_id) FROM replay_run")
    assert one("SELECT status FROM replay_run WHERE replay_run_id = %s", (bad_run,)) == "failed"
    assert q("SELECT count(*), count(actual_commit_at) FROM replay_event WHERE replay_run_id = %s", (bad_run,)) == [(5, 0)]
    assert one("SELECT count(*) FROM fin_trans") == before_rows
    logger.info("event first: with the change step failing, 5 events were already committed and no row changed")
    logger.success("ALL CHECKS PASSED")


def sorted_history():
    return sorted(tsv_rows("fin_trans.tsv"), key=lambda r: (r[2], int(r[0])))


def assert_original_trans_match_tsv(q, one):
    """Every original transaction is byte-for-byte what the TSV says: never updated."""
    want = {int(r[0]): r for r in tsv_rows("fin_trans.tsv")}
    for row in q("SELECT trans_id, account_id, trans_date, amount, balance, trans_type, operation, category, "
                 "other_bank_id, other_account_id, recorded_at = updated_at FROM fin_trans "
                 "WHERE data_origin = 'czech_original'"):
        f = want[row[0]]
        text = [str(row[0]), str(row[1]), row[2].isoformat(), row[3], row[4], row[5], row[6],
                row[7] if row[7] is not None else "", row[8] if row[8] is not None else "",
                str(row[9]) if row[9] is not None else ""]
        want_row = f[:3] + [Decimal(f[3]), Decimal(f[4])] + f[5:]  # the TSV writes .08, PostgreSQL 0.08
        assert text == want_row, (text, f)
        assert row[10], "original transaction was updated after its insert"


def assert_faker_balances(q, when):
    """Docs rule: closing = opening + net per account-day; opening = prior day's stated closing."""
    rows = q("""
        WITH t AS (SELECT account_id, trans_date, trans_id, amount, balance FROM fin_trans
                   WHERE data_origin = 'faker_synthetic'),
        marked AS (SELECT a.*, NOT EXISTS (SELECT 1 FROM t b WHERE b.account_id = a.account_id
                       AND b.trans_date = a.trans_date AND b.trans_id <> a.trans_id
                       AND abs(b.balance - b.amount - a.balance) <= 0.02) AS is_closing FROM t a),
        days AS (SELECT account_id, trans_date, sum(amount) AS net,
                        count(*) FILTER (WHERE is_closing) AS closers,
                        max(balance) FILTER (WHERE is_closing) AS closing FROM marked GROUP BY 1, 2),
        chk AS (SELECT *, COALESCE(lag(closing) OVER (PARTITION BY account_id ORDER BY trans_date), 0) AS opening
                FROM days)
        SELECT count(*), count(*) FILTER (WHERE closers <> 1),
               count(*) FILTER (WHERE abs(opening + net - closing) > %s) FROM chk""", (TOLERANCE,))
    days, ambiguous, bad = rows[0]
    assert days > 0 and ambiguous == 0 and bad == 0, f"Faker daily balances {when}: {days} days, {ambiguous} ambiguous, {bad} bad"
    per_row = q("""SELECT count(*) FROM (SELECT amount, balance,
                       COALESCE(lag(balance) OVER (PARTITION BY account_id ORDER BY trans_id), 0) AS prev
                   FROM fin_trans WHERE data_origin = 'faker_synthetic') x WHERE prev + amount <> balance""")[0][0]
    assert per_row == 0, f"Faker running balance broken in {per_row} rows {when}"
    logger.info(f"balances {when}: {days} Faker account-days reconcile, running balance exact")


if __name__ == "__main__":
    main()
