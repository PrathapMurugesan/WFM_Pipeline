"""
wfm_metadata.py - zone / phase / cycle / UPC of a WFM survey, from the WFM Postgres tables.

The S3 folder name of an upload is the survey id:
    s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<survey id>/D1..D4
    <survey id> = wfm_survey_schedule.id           (+ related rows of wfm_report)

    python wfm_metadata.py 3433cb89-f583-438c-ab44-6e99455e1bf4      show both tables' columns + the rows + what was found
    run_pipeline.py calls lookup() itself when --zone/--phase/--cycle/--upc are not given.

Credentials (never in git, never printed) - the first that exists:
    1. environment variables DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SCHEMA TARGET_DBS (TARGET_DBS = database name)
    2. the file in NSV_PG_CREDS
    3. postgres_creds.txt in this folder, or silver/postgres_creds.txt
    file format: one 'KEY = value' per line (other lines ignored)
The database is only reachable inside the AWS VPC (run on the EC2 instance).
Needs: pip install psycopg2-binary
"""
import json
import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

ROOT = Path(__file__).resolve().parent
KEYS = ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_SCHEMA", "TARGET_DBS")
SCHEDULE_TABLE, REPORT_TABLE = "wfm_survey_schedule", "wfm_report"

# column names (lower case) that hold each field - first match wins; also searched inside JSON columns
FIELDS = {
    "upc":   ["upc", "upc_code", "upc_no", "upc_number", "project_upc", "project_code"],
    "zone":  ["zone", "zone_name", "zone_code"],
    "phase": ["phase", "phase_name", "phase_no", "survey_phase"],
    "cycle": ["cycle", "cycle_name", "cycle_no", "survey_cycle", "cycle_number"],
}
UPC = re.compile(r"\b([A-Z])[/_\- ]?(\d{5})[/_\- ]?(\d{5})[/_\- ]?([A-Z]{2})\b", re.IGNORECASE)


# =============================================================================
# Credentials + connection
# =============================================================================
def load_creds():
    creds = {k: os.environ[k] for k in KEYS if os.environ.get(k)}
    if {"DB_HOST", "DB_USER", "DB_PASSWORD"} <= set(creds):
        return creds
    files = [os.environ.get("NSV_PG_CREDS"), ROOT / "postgres_creds.txt", ROOT / "silver" / "postgres_creds.txt"]
    for f in files:
        if f and Path(f).is_file():
            for line in Path(f).read_text(encoding="utf-8").splitlines():
                m = re.match(r"\s*([A-Za-z_]+)\s*[:=]\s*(.*?)\s*$", line)
                if m and m.group(1).upper() in KEYS:
                    creds.setdefault(m.group(1).upper(), m.group(2).strip().strip("\"'"))
            break
    missing = [k for k in ("DB_HOST", "DB_USER", "DB_PASSWORD") if not creds.get(k)]
    if missing:
        raise RuntimeError(f"Postgres credentials missing {missing}: set the environment variables or put "
                           f"postgres_creds.txt next to this file (see the top of wfm_metadata.py)")
    return creds


def databases(creds):
    return [d for d in re.split(r"[,;\s]+", creds.get("TARGET_DBS", "")) if d] or ["postgres"]


def connect(creds, dbname):
    import psycopg2
    return psycopg2.connect(host=creds["DB_HOST"], port=int(creds.get("DB_PORT") or 5432), user=creds["DB_USER"],
                            password=creds["DB_PASSWORD"], dbname=dbname, connect_timeout=15)


# =============================================================================
# Queries
# =============================================================================
def _jsonable(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (Decimal, UUID)):
        return str(v)
    if isinstance(v, (bytes, memoryview)):
        return f"<{len(bytes(v))} bytes>"
    return v


def _rows(cur):
    cols = [d[0] for d in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, r)} for r in cur.fetchall()]


def find_table(cur, table, schema=None):
    """(schema, columns [(name, type)]) of the table; the configured schema first, then any schema."""
    cur.execute("""SELECT table_schema, column_name, data_type FROM information_schema.columns
                   WHERE table_name = %s ORDER BY (table_schema = %s) DESC, table_schema, ordinal_position""",
                (table, schema or ""))
    found = cur.fetchall()
    if not found:
        return None, []
    first = found[0][0]
    return first, [(c, t) for s, c, t in found if s == first]


def fetch(cur, schema, table, column, value):
    from psycopg2 import sql
    cur.execute(sql.SQL("SELECT * FROM {}.{} WHERE {}::text = %s").format(
        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(column)), (value,))
    return _rows(cur)


def lookup(survey_id, verbose=False):
    """
    {'survey_id', 'database', 'schedule': [rows], 'reports': [rows], 'report_link': column used,
     'columns': {table: [(name, type)]}, 'found': {'upc', 'zone', 'phase', 'cycle'}, 'found_in': {field: 'table.column'}}
    """
    creds = load_creds()
    out = {"survey_id": survey_id, "schedule": [], "reports": [], "columns": {}, "found": {}, "found_in": {}}
    for db in databases(creds):
        with connect(creds, db) as conn, conn.cursor() as cur:
            s_schema, s_cols = find_table(cur, SCHEDULE_TABLE, creds.get("DB_SCHEMA"))
            r_schema, r_cols = find_table(cur, REPORT_TABLE, creds.get("DB_SCHEMA"))
            if not s_schema and not r_schema:
                continue
            out["database"] = db
            out["columns"] = {f"{s_schema}.{SCHEDULE_TABLE}": s_cols, f"{r_schema}.{REPORT_TABLE}": r_cols}
            if s_schema:
                out["schedule"] = fetch(cur, s_schema, SCHEDULE_TABLE, "id", survey_id)
            if r_schema:
                # wfm_report -> the survey: a column that names the schedule / survey, else its own id
                links = [c for c, _ in r_cols if re.search(r"(survey|schedule)", c, re.I)] + ["id"]
                for col in links:
                    rows = fetch(cur, r_schema, REPORT_TABLE, col, survey_id)
                    if rows:
                        out["reports"], out["report_link"] = rows, col
                        break
            break
    out["found"], out["found_in"] = extract(out)
    return out


# =============================================================================
# zone / phase / cycle / UPC from the rows
# =============================================================================
def _flatten(prefix, value, into):
    if isinstance(value, str) and value.strip()[:1] in "{[":
        try:
            value = json.loads(value)
        except ValueError:
            pass
    if isinstance(value, dict):
        for k, v in value.items():
            _flatten(f"{prefix}.{k}", v, into)
    elif isinstance(value, list):
        for i, v in enumerate(value[:20]):
            _flatten(f"{prefix}[{i}]", v, into)
    else:
        into[prefix] = value


def _clean(field, value):
    text = str(value).strip() if value is not None else ""
    if not text:
        return None
    if field == "upc":
        m = UPC.search(text)
        return f"{m.group(1).upper()}/{m.group(2)}/{m.group(3)}/{m.group(4).upper()}" if m else None
    if field == "zone":
        m = re.search(r"\b(?:zone[\s_\-]*)?([A-Z])\b", text, re.I)      # 'Zone A', 'A', 'zone-a'
        return m.group(1).upper() if m else None
    m = re.search(r"(\d+)", text)                     # 'Phase 2', 'Cycle-2', 2 -> '2'
    return str(int(m.group(1))) if m else None


def extract(result):
    """First column (schedule first, then reports) whose name fits a field and whose value parses."""
    flat = []
    for table, rows in ((SCHEDULE_TABLE, result["schedule"]), (REPORT_TABLE, result["reports"])):
        for row in rows:
            cells = {}
            for k, v in row.items():
                _flatten(k, v, cells)
            flat += [(f"{table}.{k}", k.split(".")[-1].split("[")[0].lower(), v) for k, v in cells.items()]
    found, where = {}, {}
    for field, names in FIELDS.items():
        for name in names:
            hit = next(((path, _clean(field, v)) for path, col, v in flat if col == name and _clean(field, v)), None)
            if hit:
                where[field], found[field] = hit
                break
        if field == "upc" and "upc" not in found:          # a UPC written in any text column
            hit = next(((path, _clean("upc", v)) for path, _, v in flat if isinstance(v, str) and UPC.search(v)), None)
            if hit:
                where["upc"], found["upc"] = hit
    return found, where


# =============================================================================
# CLI
# =============================================================================
def main(args):
    if not args:
        print(__doc__)
        return 1
    survey_id = args[0].strip().strip("/").split("/")[-1]
    try:
        r = lookup(survey_id, verbose=True)
    except Exception as e:
        print(f"lookup failed: {type(e).__name__}: {e}")
        return 1
    print(f"survey id {survey_id}  (database {r.get('database', '-')})\n")
    for table, cols in r["columns"].items():
        print(f"{table}: {len(cols)} columns\n  " + ", ".join(f"{c} ({t})" for c, t in cols) + "\n")
    print(f"{SCHEDULE_TABLE} rows with id = {survey_id}: {len(r['schedule'])}")
    for row in r["schedule"]:
        print(json.dumps(row, indent=2, ensure_ascii=False, default=str))
    print(f"\n{REPORT_TABLE} rows (linked by '{r.get('report_link', '-')}'): {len(r['reports'])}")
    for row in r["reports"][:5]:
        print(json.dumps(row, indent=2, ensure_ascii=False, default=str))
    print("\nfound:")
    for f in FIELDS:
        print(f"  {f:6} = {r['found'].get(f) or '-':20} {('<- ' + r['found_in'][f]) if f in r['found_in'] else '(not found - add its column name to FIELDS)'}")
    target = ROOT / "output" / "metadata" / f"{survey_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(r, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nsaved: {target}")
    return 0 if r["schedule"] else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
