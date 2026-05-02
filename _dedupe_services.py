import argparse
import time
from typing import List, Tuple

import certifi
import pymysql

DB_CONFIG = {
    "host": "zidepeople-dev.mysql.database.azure.com",
    "user": "zide_admin",
    "password": "Suggestpassword56",
    "database": "zidepeople_db",
    "port": 3306,
    "ssl_ca": certifi.where(),
    "ssl_verify_cert": False,
    "ssl_verify_identity": False,
    "charset": "utf8mb4",
    "autocommit": True,
    "connect_timeout": 15,
    "read_timeout": 90,
    "write_timeout": 90,
}

RETRYABLE_CODES = {1205, 1213, 2006, 2013, 2055}


def connect():
    conn = pymysql.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute("SET SESSION innodb_lock_wait_timeout=5")
    return conn


def execute_retry(conn, sql: str, params=None, attempts: int = 7, pause: float = 0.5):
    last_exc = None
    for i in range(1, attempts + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else None
                return conn, cur.rowcount, rows
        except pymysql.err.OperationalError as exc:
            last_exc = exc
            code = exc.args[0] if exc.args else None
            if code not in RETRYABLE_CODES or i == attempts:
                raise
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(pause)
            conn = connect()
        except pymysql.err.InternalError as exc:
            last_exc = exc
            code = exc.args[0] if exc.args else None
            if code not in RETRYABLE_CODES or i == attempts:
                raise
            time.sleep(pause)
    raise last_exc


def fetch_groups(conn, limit_count: int):
    sql = """
        SELECT LOWER(TRIM(service)) AS norm_service, type
        FROM services
        GROUP BY LOWER(TRIM(service)), type
        HAVING COUNT(*) > 1
        ORDER BY MIN(id)
        LIMIT %s
    """
    conn, _, rows = execute_retry(conn, sql, (limit_count,))
    return conn, rows or []


def fetch_refs(conn) -> Tuple:
    sql = """
        SELECT TABLE_NAME, COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME = 'services'
          AND REFERENCED_COLUMN_NAME = 'id'
        ORDER BY TABLE_NAME
    """
    conn, _, rows = execute_retry(conn, sql)
    return conn, tuple(rows or [])


def count_remaining_groups(conn) -> int:
    sql = """
        SELECT COUNT(*)
        FROM (
            SELECT LOWER(TRIM(service)), type
            FROM services
            GROUP BY LOWER(TRIM(service)), type
            HAVING COUNT(*) > 1
        ) d
    """
    conn, _, rows = execute_retry(conn, sql)
    return rows[0][0]


def apply_indexes_if_clean(conn):
    remaining = count_remaining_groups(conn)
    if remaining != 0:
        print(f"Index migration skipped: {remaining} duplicate groups still remain.")
        return conn

    conn, _, rows = execute_retry(conn, "SHOW INDEX FROM services")
    idx_names = {r[2] for r in (rows or [])}

    if "service" in idx_names:
        conn, _, _ = execute_retry(conn, "ALTER TABLE services DROP INDEX service")
        print("Dropped index: service")

    conn, _, rows = execute_retry(conn, "SHOW INDEX FROM services")
    idx_names = {r[2] for r in (rows or [])}

    if "uq_services_service_type" not in idx_names:
        conn, _, _ = execute_retry(
            conn,
            "ALTER TABLE services ADD UNIQUE KEY uq_services_service_type (service, type)",
        )
        print("Added index: uq_services_service_type")

    conn, _, rows = execute_retry(
        conn,
        "SHOW INDEX FROM services WHERE Key_name IN ('service','uq_services_service_type')",
    )
    print("Final index rows:", rows)
    return conn


def process_drop_id(conn, keep_id: int, drop_id: int, refs: Tuple, chunk_size: int):
    for table_name, col_name in refs:
        while True:
            sql = (
                f"UPDATE IGNORE {table_name} "
                f"SET {col_name}=%s "
                f"WHERE {col_name}=%s "
                f"LIMIT {chunk_size}"
            )
            conn, moved, _ = execute_retry(conn, sql, (keep_id, drop_id))
            if moved <= 0:
                break
            if moved < chunk_size:
                break

        while True:
            sql = f"DELETE FROM {table_name} WHERE {col_name}=%s LIMIT {chunk_size}"
            conn, deleted, _ = execute_retry(conn, sql, (drop_id,))
            if deleted <= 0:
                break
            if deleted < chunk_size:
                break

    remaining_refs = 0
    for table_name, col_name in refs:
        sql = f"SELECT COUNT(*) FROM {table_name} WHERE {col_name}=%s"
        conn, _, rows = execute_retry(conn, sql, (drop_id,))
        remaining_refs += rows[0][0]

    if remaining_refs == 0:
        conn, deleted, _ = execute_retry(conn, "DELETE FROM services WHERE id=%s", (drop_id,))
        return conn, deleted == 1, remaining_refs
    return conn, False, remaining_refs


def run_batch(batch_size: int, chunk_size: int, migrate_if_clean: bool):
    conn = connect()
    try:
        conn, refs = fetch_refs(conn)
        conn, groups = fetch_groups(conn, batch_size)

        print(f"Ref tables: {len(refs)}")
        print(f"Batch groups: {len(groups)}")

        processed = 0
        for norm_service, service_type in groups:
            conn, _, rows = execute_retry(
                conn,
                """
                SELECT id
                FROM services
                WHERE LOWER(TRIM(service))=%s AND type=%s
                ORDER BY id
                """,
                (norm_service, service_type),
            )
            ids = [r[0] for r in (rows or [])]
            if len(ids) < 2:
                continue

            keep_id = ids[0]
            drop_ids = ids[1:]
            print(f"GROUP {processed + 1}: {norm_service} [{service_type}] keep={keep_id} drops={drop_ids}")

            for drop_id in drop_ids:
                conn, removed, remaining = process_drop_id(conn, keep_id, drop_id, refs, chunk_size)
                if not removed:
                    print(f"  WARN could not remove service {drop_id}; refs remaining={remaining}")

            processed += 1

        remaining = count_remaining_groups(conn)
        print(f"Processed groups: {processed}")
        print(f"Remaining duplicate groups: {remaining}")

        if migrate_if_clean:
            conn = apply_indexes_if_clean(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Deduplicate services by (service,type).")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--chunk-size", type=int, default=200)
    parser.add_argument("--migrate-if-clean", action="store_true")
    args = parser.parse_args()

    run_batch(
        batch_size=args.batch_size,
        chunk_size=args.chunk_size,
        migrate_if_clean=args.migrate_if_clean,
    )
