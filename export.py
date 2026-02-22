
#!/usr/bin/env python3
import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from threading import Lock
import psutil
import pymysql
import pymysql.cursors

HOST = "127.0.0.1"
PORT = 9306
USER = ""
PASSWORD = ""
DATABASE = ""
BACKUP_BASE = "/backup"
BATCH_SIZE = 50_000
RAM_THRESHOLD = 90
RAM_CHECK_INTERVAL = 2
RAM_WAIT_TIMEOUT = 20
RETRY_COUNT = 5
RETRY_DELAY = 15
QUERY_TIMEOUT = 600
SPARSE_MODE = "step"
SPARSE_STEP = 10_000_000
STATE_SUFFIX = ".state.json"

class DualLogger:
    def __init__(self, log_file: str):
        self.lock = Lock()
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
        self.logger = logging.getLogger('manticore_dumper')
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers = []
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    def info(self, msg):
        with self.lock:
            self.logger.info(msg)
    def warning(self, msg):
        with self.lock:
            self.logger.warning(msg)
    def error(self, msg):
        with self.lock:
            self.logger.error(msg)
    def debug(self, msg):
        with self.lock:
            self.logger.debug(msg)

class RAMMonitor:
    def __init__(self, logger: DualLogger, threshold=90, check_interval=2):
        self.logger = logger
        self.threshold = threshold
        self.check_interval = check_interval
    def get_ram_usage(self):
        return psutil.virtual_memory().percent
    def wait_if_needed(self, timeout=20):
        ram_usage = self.get_ram_usage()
        if ram_usage < self.threshold:
            return True
        self.logger.warning(f"RAM usage is high: {ram_usage:.1f}% (threshold: {self.threshold}%). Waiting…")
        start_time = time.time()
        low_ram_time = None
        while True:
            time.sleep(self.check_interval)
            ram_usage = self.get_ram_usage()
            if ram_usage >= self.threshold:
                low_ram_time = None
                elapsed = int(time.time() - start_time)
                if elapsed % 10 < self.check_interval:
                    self.logger.info(f" RAM: {ram_usage:.1f}%, waiting…")
            else:
                if low_ram_time is None:
                    low_ram_time = time.time()
                self.logger.info(f" RAM dropped to {ram_usage:.1f}%, checking stability…")
                if time.time() - low_ram_time >= timeout:
                    self.logger.info(f"✓ RAM is stable below {self.threshold}% ({ram_usage:.1f}%). Continuing.")
                    return True

def get_connection():
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        autocommit=True,
        connect_timeout=10,
        read_timeout=QUERY_TIMEOUT,
        write_timeout=QUERY_TIMEOUT,
        cursorclass=pymysql.cursors.DictCursor,
    )

def is_manticore_alive():
    try:
        conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, connect_timeout=5, cursorclass=pymysql.cursors.DictCursor)
        conn.close()
        return True
    except Exception:
        return False

def wait_for_manticore(logger: DualLogger, max_wait=600, poll_interval=10):
    if is_manticore_alive():
        return True
    logger.warning(f"Manticore is unavailable. Waiting for recovery (up to {max_wait // 60} min)…")
    start = time.time()
    while time.time() - start < max_wait:
        time.sleep(poll_interval)
        elapsed = int(time.time() - start)
        if is_manticore_alive():
            logger.info(f"✓ Manticore is available again after {elapsed} sec. Continuing.")
            time.sleep(3)
            return True
        logger.info(f" Manticore still unavailable… ({elapsed} sec elapsed)")
    logger.error(f"Manticore did not recover within {max_wait} sec. Stop waiting.")
    return False

def is_connection_error(e: Exception) -> bool:
    msg = str(e)
    patterns = [
        "Lost connection", "Can't connect", "Connection refused",
        "MySQL server has gone away", "2013", "2003", "2006",
        "Broken pipe", "timed out",
    ]
    return any(p in msg for p in patterns)

def run_query_with_retry(logger: DualLogger, query: str, retries=RETRY_COUNT):
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    return rows, True
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f" Query failed (attempt {attempt}/{retries}): {e}")
            if is_connection_error(e):
                if not wait_for_manticore(logger):
                    logger.error("Manticore did not recover, skipping query.")
                    return [], False
            elif attempt >= retries:
                logger.error(f" All {retries} attempts exhausted.")
                return [], False
            else:
                time.sleep(RETRY_DELAY)
    return [], False

def get_all_tables(logger: DualLogger):
    rows, ok = run_query_with_retry(logger, "SHOW TABLES;")
    if not ok:
        raise RuntimeError("Failed to get tables list")
    tables = []
    for r in rows:
        name = list(r.values())[0]
        tables.append(name)
    logger.info(f"Found {len(tables)} tables: {', '.join(tables[:10])}...")
    return tables

def get_id_range(logger: DualLogger, table: str):
    q = f"SELECT MIN(id) AS min_id, MAX(id) AS max_id FROM `{table}`;"
    rows, ok = run_query_with_retry(logger, q)
    if not ok or not rows:
        return None, None
    try:
        r = rows[0]
        min_id = int(r.get("min_id")) if r.get("min_id") is not None else None
        max_id = int(r.get("max_id")) if r.get("max_id") is not None else None
        return min_id, max_id
    except Exception as e:
        logger.error(f"Failed to parse id range for {table}: {e}")
        return None, None

def get_row_count(logger: DualLogger, table: str):
    rows, ok = run_query_with_retry(logger, f"SELECT COUNT(*) AS cnt FROM `{table}`;")
    if not ok or not rows:
        return None
    try:
        return int(list(rows[0].values())[0])
    except Exception as e:
        logger.error(f"Failed to parse row count for {table}: {e}")
        return None

def get_fieldnames(logger: DualLogger, table: str):
    rows, ok = run_query_with_retry(logger, f"DESCRIBE `{table}`;")
    if ok and rows:
        cols = []
        for r in rows:
            if "Field" in r:
                cols.append(r["Field"])
            else:
                cols.append(list(r.values())[0])
        if "id" in cols:
            cols = ["id"] + [c for c in cols if c != "id"]
        return cols
    return None

def fetch_next_batch_after_id(logger: DualLogger, table: str, last_id: int, limit: int):
    query = (
        f"SELECT * FROM `{table}` "
        f"WHERE id > {last_id} "
        f"ORDER BY id ASC "
        f"LIMIT {limit} "
        f"OPTION max_matches={limit};"
    )
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                    return rows, True
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f" Batch after id={last_id} failed (attempt {attempt}/{RETRY_COUNT}): {e}")
            if is_connection_error(e):
                if not wait_for_manticore(logger):
                    logger.error("Manticore did not recover.")
                    return [], False
            elif attempt >= RETRY_COUNT:
                logger.error(f" All {RETRY_COUNT} attempts exhausted for batch.")
                return [], False
            else:
                time.sleep(RETRY_DELAY)
    return [], False

def get_min_id_after(logger: DualLogger, table: str, greater_than_id: int, upper_bound: int | None = None):
    if upper_bound is not None:
        q = (
            f"SELECT MIN(id) AS next_id FROM `{table}` "
            f"WHERE id > {greater_than_id} AND id <= {upper_bound};"
        )
    else:
        q = f"SELECT MIN(id) AS next_id FROM `{table}` WHERE id > {greater_than_id};"
    rows, ok = run_query_with_retry(logger, q)
    if not ok or not rows:
        return None
    v = rows[0].get("next_id")
    return int(v) if v is not None else None

def check_local_file_status(logger: DualLogger, final_filename: str, backup_path: str, node: int):
    path = os.path.join(backup_path, final_filename)
    try:
        if not os.path.exists(path):
            return False, 0
        with open(path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f)
        data_rows = max(0, total_lines - 1)
        logger.info(f"[Node {node}] File {final_filename} exists: {data_rows:,} rows (+1 header)")
        return True, data_rows
    except Exception as e:
        logger.debug(f"[Node {node}] Error while checking file: {e}")
        return False, 0

def write_batch_to_local(logger: DualLogger, local_file: str, rows: list[dict], is_first: bool, batch_label: str, node: int, fieldnames: list[str]):
    if not rows:
        return True
    try:
        mode = 'w' if is_first else 'a'
        with open(local_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if is_first:
                writer.writeheader()
            for r in rows:
                out = {k: r.get(k, "") for k in fieldnames}
                writer.writerow(out)
        logger.debug(f"[Node {node}] Batch {batch_label} written ({len(rows):,} rows)")
        return True
    except Exception as e:
        logger.error(f"[Node {node}] Failed to write batch {batch_label}: {e}")
        return False

def state_path_for(backup_path: str, table: str) -> str:
    return os.path.join(backup_path, f"{table}{STATE_SUFFIX}")

def load_state(backup_path: str, table: str):
    path = state_path_for(backup_path, table)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def save_state(backup_path: str, table: str, data: dict):
    path = state_path_for(backup_path, table)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

def dump_table(logger: DualLogger, ram_monitor: RAMMonitor, table: str, node: int, backup_path: str, tables_remaining: int, resume: bool, batch_size: int, sparse_mode: str, sparse_step: int):
    logger.info(f"[Node {node}] {'='*60}")
    logger.info(f"[Node {node}] Start dumping table: {table} (tables remaining: {tables_remaining})")
    if not wait_for_manticore(logger):
        logger.error(f"[Node {node}] Manticore unavailable, skipping {table}")
        return False
    total_rows = get_row_count(logger, table)
    if total_rows is None:
        logger.error(f"[Node {node}] Could not get row count for {table}")
        return False
    if total_rows == 0:
        logger.warning(f"[Node {node}] Table {table} is empty, skipping")
        return True
    logger.info(f"[Node {node}] {table}: {total_rows:,} rows")
    min_id, max_id = get_id_range(logger, table)
    if min_id is None or max_id is None:
        logger.error(f"[Node {node}] Could not get id range for {table}")
        return False
    logger.info(f"[Node {node}] {table}: id from {min_id:,} to {max_id:,}")
    start_max_id = max_id
    final_filename = f"{table}_dump.csv"
    final_path = os.path.join(backup_path, final_filename)
    fieldnames = get_fieldnames(logger, table)
    st = load_state(backup_path, table) if resume else None
    if st is not None:
        if st.get("snapshot_max_id") == start_max_id:
            last_id = int(st.get("last_id", min_id - 1))
            already_done = int(st.get("rows_written", 0))
            logger.info(f"[Node {node}] Resuming {table}: last_id={last_id:,}, snapshot_max_id={start_max_id:,}, previously written: {already_done:,}")
        else:
            logger.warning(f"[Node {node}] Snapshot MAX(id) changed (was {st.get('snapshot_max_id')}, now {start_max_id}). Starting a new snapshot.")
            last_id = min_id - 1
            already_done = 0
    else:
        last_id = min_id - 1
        already_done = 0

    if not resume:
        file_exists, data_rows_in_file = check_local_file_status(logger, final_filename, backup_path, node)
        if file_exists:
            if data_rows_in_file == total_rows:
                logger.info(f"[Node {node}] ✓ Table {table} already fully dumped ({data_rows_in_file:,}). Skipping.")
                return True
            else:
                logger.warning(f"[Node {node}] File exists but row count differs: file {data_rows_in_file:,} vs table {total_rows:,}. Deleting and starting over.")
                try:
                    os.remove(final_path)
                    logger.info(f"[Node {node}] File {final_filename} deleted.")
                except Exception as e:
                    logger.error(f"[Node {node}] Failed to delete file: {e}")
                    return False

    total_written = already_done
    batch_num = 0 if already_done == 0 else (already_done // max(1, batch_size))
    is_first_write = not os.path.exists(final_path) or os.path.getsize(final_path) == 0
    current_sparse_window = sparse_step

    while last_id < start_max_id:
        ram_monitor.wait_if_needed(timeout=RAM_WAIT_TIMEOUT)
        rows, ok = fetch_next_batch_after_id(logger, table, last_id, batch_size)
        if not ok:
            logger.warning(f"[Node {node}] Failed to get batch after id={last_id}. Continuing…")
            continue
        if not rows:
            if last_id >= start_max_id:
                break
            if sparse_mode == "next":
                next_id = get_min_id_after(logger, table, last_id, upper_bound=start_max_id)
                if next_id is None:
                    logger.info(f"[Node {node}] No rows > last_id within snapshot. Done.")
                    break
                logger.debug(f"[Node {node}] sparse(next): jump to id={next_id:,}")
                last_id = next_id - 1
                current_sparse_window = sparse_step
                continue
            else:
                probe_upper = min(last_id + current_sparse_window, start_max_id)
                next_id = get_min_id_after(logger, table, last_id, upper_bound=probe_upper)
                if next_id is not None:
                    logger.debug(f"[Node {node}] sparse(step): window {current_sparse_window:,}, found next_id={next_id:,}")
                    last_id = next_id - 1
                    current_sparse_window = sparse_step
                    continue
                else:
                    if probe_upper >= start_max_id:
                        logger.info(f"[Node {node}] No rows > last_id within snapshot. Done.")
                        break
                    current_sparse_window = min(current_sparse_window + sparse_step, start_max_id - last_id)
                    logger.debug(f"[Node {node}] sparse(step): expanding window to {current_sparse_window:,} and retrying")
                    continue
        if fieldnames is None:
            fieldnames = list(rows[0].keys())
            if "id" in fieldnames:
                fieldnames = ["id"] + [c for c in fieldnames if c != "id"]
        success = write_batch_to_local(logger, final_path, rows, is_first=is_first_write, batch_label=str(batch_num + 1), node=node, fieldnames=fieldnames)
        if not success:
            logger.warning(f"[Node {node}] Failed to write batch {batch_num+1}, moving on")
        else:
            is_first_write = False
            total_written += len(rows)
            last_id = rows[-1].get('id', last_id)
            batch_num += 1
            save_state(backup_path, table, {
                "last_id": last_id,
                "snapshot_max_id": start_max_id,
                "rows_written": total_written,
                "updated_at": datetime.utcnow().isoformat() + "Z",
            })
            progress_pct = (last_id - min_id) / max(1, (start_max_id - min_id)) * 100.0
            logger.info(
                f"[Node {node}] {table}: batch {batch_num} in_batch: {len(rows):,} total: {total_written:,} last_id={last_id:,} progress: {progress_pct:.2f}% RAM: {ram_monitor.get_ram_usage():.1f}%"
            )

    final_count = get_row_count(logger, table)
    if final_count is not None and total_written != final_count:
        logger.warning(f"[Node {node}] Row count mismatch: written {total_written:,}, table {final_count:,}")
    logger.info(f"[Node {node}] ✓ Dump of table {table} completed. Rows written: {total_written:,}")
    return True


def main():
    parser = argparse.ArgumentParser(description='Manticore Search Dumper (seek-pagination) with sparse scan and resume')
    parser.add_argument('--node', type=int, required=True, choices=[1, 2, 3], help='Node number (1/2/3) used for /backup/node{x}')
    parser.add_argument('--backup-dir', type=str, default=BACKUP_BASE, help='Base backup directory (default: /backup)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE, help='Batch size for cursor pagination (default: 50_000)')
    parser.add_argument('--resume', action='store_true', help='Resume from the last saved state (*.state.json)')
    parser.add_argument('--sparse-mode', choices=['next', 'step'], default=SPARSE_MODE, help='Sparse scan mode: next | step')
    parser.add_argument('--sparse-step', type=int, default=SPARSE_STEP, help='Window growth step in step-mode (default: 10_000_000)')
    args = parser.parse_args()

    node = args.node
    backup_path = os.path.join(args.backup_dir, f"node{node}")
    if not os.path.exists(backup_path):
        print(f"ERROR: Directory {backup_path} does not exist!")
        sys.exit(1)

    log_file = f"manticore_dump_node{node}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = DualLogger(log_file)

    logger.info("=" * 80)
    logger.info("Manticore Search Dumper — started (seek-pagination)")
    logger.info(f" Node: {node}")
    logger.info(f" Backup directory: {backup_path}")
    logger.info(f" Batch size: {args.batch_size:,}")
    logger.info(f" RAM threshold: {RAM_THRESHOLD}%")
    logger.info(f" Retry attempts: {RETRY_COUNT}, delay: {RETRY_DELAY}s")
    logger.info(f" Query timeout: {QUERY_TIMEOUT}s")
    logger.info(f" Sparse mode: {args.sparse_mode}, step: {args.sparse_step:,}")
    logger.info(f" Log file: {log_file}")
    logger.info("=" * 80)

    logger.info("Checking connection to Manticore…")
    if not wait_for_manticore(logger):
        logger.error("Failed to connect to Manticore. Exiting.")
        sys.exit(1)
    logger.info("✓ Connection to Manticore established")

    ram_monitor = RAMMonitor(logger, threshold=RAM_THRESHOLD, check_interval=RAM_CHECK_INTERVAL)

    try:
        tables = get_all_tables(logger)
        if not tables:
            logger.error("No tables found to dump")
            return False
        results = {}
        total_tables = len(tables)
        for idx, table in enumerate(tables):
            tables_remaining = total_tables - idx
            try:
                success = dump_table(
                    logger, ram_monitor, table, node, backup_path, tables_remaining,
                    resume=args.resume, batch_size=args.batch_size,
                    sparse_mode=args.sparse_mode, sparse_step=args.sparse_step,
                )
                results[table] = success
            except Exception as e:
                logger.error(f"FATAL ERROR while dumping {table}: {e}")
                results[table] = False
            done = idx + 1
            remaining_after = total_tables - done
            logger.info(f"[Node {node}] Progress: {done}/{total_tables} tables processed, remaining: {remaining_after}")

        logger.info("=" * 80)
        logger.info("SUMMARY")
        logger.info("-" * 80)
        success_count = sum(1 for v in results.values() if v)
        total_count = len(results)
        for table, ok in results.items():
            status = "✓ OK" if ok else "✗ ERROR"
            logger.info(f" {table:40} {status}")
        logger.info("-" * 80)
        logger.info(f"Successful: {success_count}/{total_count}")
        logger.info("=" * 80)
        if success_count == total_count:
            logger.info("✓ ALL TABLES DUMPED SUCCESSFULLY!")
            return True
        else:
            logger.warning(f"{total_count - success_count} tables had errors")
            return False
    except KeyboardInterrupt:
        logger.warning("
Interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Critical error: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
