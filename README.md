
# Manticore Search → CSV Dumper (seek-pagination + sparse)

A robust utility to dump **all tables from Manticore Search to CSV** reliably, even with **very large datasets** and **sparse ID ranges**. It uses cursor-like pagination (`id > last_id ORDER BY id LIMIT N`), fixes a per-run snapshot boundary (`MAX(id)` at start), supports **resume**, **adaptive sparse scanning** (`next` and `step` modes), **RAM throttling**, retry logic, and dual logging (file + console).

## Features
- Dump **all tables** via `SHOW TABLES`
- Stable **seek-pagination**: `SELECT * WHERE id > last_id ORDER BY id LIMIT N`
- **Snapshot boundary**: freezes `MAX(id)` at start to avoid new inserts
- **Sparse scanning**:
  - `--sparse-mode next`: `SELECT MIN(id) WHERE id > last_id`
  - `--sparse-mode step`: expand window by `--sparse-step` across gaps
- **Resume** with `*.state.json`
- **RAM control**: pauses when memory is above threshold
- **Retry** and server recovery wait
- CSV header normalization (`id` first when present)

## Requirements
- Python 3.8+
- Access to Manticore SQL port (default `9306`)
- Packages from `requirements.txt`

## Install
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scriptsctivate
pip install -r requirements.txt
```

## Prepare
Create target folders and ensure permissions:
```bash
sudo mkdir -p /backup/node1 /backup/node2 /backup/node3
sudo chown -R "$USER":"$USER" /backup
```
Optionally adjust connection constants inside `export.py` (`HOST`, `PORT`, `USER`, `PASSWORD`, `DATABASE`).

## Usage
Basic run (dump to `/backup/node1`):
```bash
python export.py --node 1
```
More examples:
```bash
python export.py --node 1 --backup-dir /backup
python export.py --node 1 --batch-size 100000
python export.py --node 1 --resume
python export.py --node 1 --sparse-mode next
python export.py --node 1 --sparse-mode step --sparse-step 20000000
```

### Outputs
- CSV per table: `/<backup-dir>/node<node>/<table>_dump.csv`
- State per table: `/<backup-dir>/node<node>/<table>.state.json`
- Logs near the script, e.g. `manticore_dump_node1_YYYYMMDD_HHMMSS.log`

## CSV Format
- UTF-8, comma-separated
- Header from `DESCRIBE <table>`
- `id` column first when present

## CLI
- `--node {1,2,3}` (required)
- `--backup-dir /path` (default `/backup`)
- `--batch-size N` (default `50_000`)
- `--resume`
- `--sparse-mode {next,step}` (default `step`)
- `--sparse-step N` (default `10_000_000`)

## License
MIT
