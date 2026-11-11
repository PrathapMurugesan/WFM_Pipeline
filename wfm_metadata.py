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
UNKNOWN_ZONE = "UNKNOWN"          # used only when the zone cannot be found (see enrich)

# ---- names behind the schedule's ids (foreign keys of wfm_survey_schedule, found with tools/analyse_zone.py) ----
#   nsv_zone_id -> master_zone.zone_code ('B') / zone_name ('ZONE-B (WEST)')     (source NSV)
#   no nsv_zone_id -> upc_project_zone (project_id -> nsv_zone_id) -> master_zone
#   ro_id -> master_region_zone.region_name, state_id -> master_state.state_name, piu_id -> master_piu.piu_name
LOOKUPS = {
    "zone":  ("master_zone", "id", ("zone_code", "zone_name", "source")),
    "ro":    ("master_region_zone", "id", ("region_name",)),
    "state": ("master_state", "id", ("state_name",)),
    "piu":   ("master_piu", "id", ("piu_name",)),
    "project_zone": ("upc_project_zone", "project_id", ("nsv_zone_id",)),
}

# ---- when is a survey processed (also used by wfm_trigger.py) ----
# THE rule: wfm_survey_schedule.current_status is one of these (case / spaces ignored)
FINAL_STATUSES = {"Survey Final Approved"}
# extra checks, all OFF (the status alone decides); switch on if needed
CHECK_REPORTS = False                                   # True = D1..D4 wfm_report rows must also be approved
REQUIRED_REPORTS = ["D1", "D2", "D3", "D4"]
APPROVED_STATUSES = {"APPROVED", "REPORT_APPROVED"}     # wfm_report.current_status meaning approved ...
APPROVED_IF_APPROVED_AT = True                          # ... or approved_at is filled
CHECK_SCHEDULE_LOCK = False                             # True = wfm_survey_schedule.is_locked must be false
CHECK_REPORT_LOCK = False                               # True = wfm_report.is_locked must be false

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
    """TARGET_DBS: 'dhms_staging', 'a, b' or a list '["dhms_staging"]' -> ['dhms_staging']"""
    return re.findall(r"[A-Za-z0-9_][A-Za-z0-9_$\-]*", creds.get("TARGET_DBS", "")) or ["postgres"]


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


def load_lookups(cur, schema):
    """{name: {key: {column: value}}} of the small master tables in LOOKUPS (a missing table -> {})."""
    from psycopg2 import sql
    out = {}
    for name, (table, key, cols) in LOOKUPS.items():
        try:
            cur.execute(sql.SQL("SELECT {}, {} FROM {}.{}").format(
                sql.Identifier(key), sql.SQL(", ").join(map(sql.Identifier, cols)),
                sql.Identifier(schema), sql.Identifier(table)))
            out[name] = {str(r[0]): dict(zip(cols, r[1:])) for r in cur.fetchall()}
        except Exception:
            cur.connection.rollback()
            out[name] = {}
    return out


def enrich(schedule, lookups):
    """Add _zone_code / _zone_name / _ro_name / _state_name / _piu_name to a schedule row (in place)."""
    zid = schedule.get("nsv_zone_id")
    if not zid and schedule.get("project_id"):
        zid = (lookups.get("project_zone", {}).get(str(schedule["project_id"])) or {}).get("nsv_zone_id")
    zone = lookups.get("zone", {}).get(str(zid)) if zid else None
    schedule["_zone_code"] = zone.get("zone_code") if zone else None
    schedule["_zone_name"] = zone.get("zone_name") if zone else None
    schedule["_zone_source"] = ("nsv_zone_id" if schedule.get("nsv_zone_id") else "upc_project_zone") if zone else None
    for name, col, idcol in (("ro", "region_name", "ro_id"), ("state", "state_name", "state_id"), ("piu", "piu_name", "piu_id")):
        row = lookups.get(name, {}).get(str(schedule.get(idcol))) if schedule.get(idcol) else None
        schedule[f"_{name}_name"] = row.get(col) if row else None
    return schedule


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
                if out["schedule"]:
                    enrich(out["schedule"][0], load_lookups(cur, s_schema))
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
    sched = out["schedule"][0] if out["schedule"] else {}
    out["eligible"], out["not_eligible_because"] = eligibility(sched, out["reports"])
    out["details"] = survey_details(sched, out["found"]) if sched else {}
    out["fingerprint"] = fingerprint(sched, out["reports"]) if sched else None
    return out


def list_surveys(source_system="NSV"):
    """Every active survey with its wfm_report rows: [{'schedule': row, 'reports': [rows]}] (for wfm_trigger.py)."""
    from psycopg2 import sql
    creds = load_creds()
    for db in databases(creds):
        with connect(creds, db) as conn, conn.cursor() as cur:
            s_schema, s_cols = find_table(cur, SCHEDULE_TABLE, creds.get("DB_SCHEMA"))
            r_schema, _ = find_table(cur, REPORT_TABLE, creds.get("DB_SCHEMA"))
            if not s_schema or not r_schema:
                continue
            names = {c for c, _ in s_cols}
            where = [sql.SQL("TRUE")]
            if "source_system" in names and source_system:
                where.append(sql.SQL("source_system = {}").format(sql.Literal(source_system)))
            if "deleted_at" in names:
                where.append(sql.SQL("deleted_at IS NULL"))
            cur.execute(sql.SQL("SELECT * FROM {}.{} WHERE ").format(sql.Identifier(s_schema), sql.Identifier(SCHEDULE_TABLE))
                        + sql.SQL(" AND ").join(where))
            schedules = _rows(cur)
            lookups = load_lookups(cur, s_schema)
            for row in schedules:
                enrich(row, lookups)
            cur.execute(sql.SQL("SELECT * FROM {}.{} WHERE survey_schedule_id IS NOT NULL").format(
                sql.Identifier(r_schema), sql.Identifier(REPORT_TABLE)))
            reports = {}
            for r in _rows(cur):
                reports.setdefault(str(r["survey_schedule_id"]), []).append(r)
            return [{"schedule": s, "reports": reports.get(str(s["id"]), [])} for s in schedules]
    raise RuntimeError(f"tables {SCHEDULE_TABLE} / {REPORT_TABLE} not found in {databases(creds)}")


# =============================================================================
# Is the survey ready to process?
# =============================================================================
def report_approved(row):
    status = str(row.get("current_status") or "").strip().upper()
    return status in APPROVED_STATUSES or (APPROVED_IF_APPROVED_AT and bool(row.get("approved_at")))


def _true(v):
    return v is True or str(v).strip().lower() in ("true", "t", "1", "yes")


def eligibility(schedule, reports):
    """(ok, [reasons it is not]) for one survey: schedule row + its wfm_report rows."""
    why = []
    if not schedule:
        return False, ["no wfm_survey_schedule row"]
    norm = lambda t: re.sub(r"\s+", " ", str(t or "")).strip().lower()           # noqa: E731
    if norm(schedule.get("current_status")) not in {norm(s) for s in FINAL_STATUSES}:
        why.append(f"status '{schedule.get('current_status')}' (needs {sorted(FINAL_STATUSES)})")
    if CHECK_SCHEDULE_LOCK and _true(schedule.get("is_locked")):
        why.append("survey is locked")
    if "is_active" in schedule and not _true(schedule.get("is_active")):
        why.append("survey is not active")
    if schedule.get("deleted_at"):
        why.append("survey is deleted")
    by_type = {}
    for r in reports:                                    # newest row per report type
        t = str(r.get("report_type") or "").upper()
        if t not in by_type or str(r.get("updated_at") or "") > str(by_type[t].get("updated_at") or ""):
            by_type[t] = r
    for t in (REQUIRED_REPORTS if CHECK_REPORTS else []):
        r = by_type.get(t)
        if r is None:
            why.append(f"{t}: no report")
        elif not report_approved(r):
            why.append(f"{t}: not approved ({r.get('current_status')})")
        elif CHECK_REPORT_LOCK and _true(r.get("is_locked")):
            why.append(f"{t}: report is locked")
    return not why, why


def fingerprint(schedule, reports):
    """Changes when the survey or any of its reports changes -> processed again (incremental)."""
    parts = [str(schedule.get("updated_at"))] + sorted(
        f"{r.get('report_type')}:{r.get('latest_version_id')}:{r.get('updated_at')}:{r.get('current_status')}"
        for r in reports)
    return "|".join(parts)


def survey_details(schedule, found=None):
    """The values the pipeline needs, from the schedule row (zone always UNKNOWN unless a zone column exists)."""
    found = found or {}
    zone = _clean("zone", schedule.get("_zone_code")) or found.get("zone") or UNKNOWN_ZONE
    return {"survey_id": schedule.get("id"), "upc": found.get("upc") or _clean("upc", schedule.get("upc")),
            "zone": zone, "zone_name": schedule.get("_zone_name"), "zone_from": schedule.get("_zone_source"),
            "ro": schedule.get("_ro_name"), "state": schedule.get("_state_name"), "piu": schedule.get("_piu_name"),
            "phase": found.get("phase") or _clean("phase", schedule.get("survey_phase")),
            "cycle": found.get("cycle") or _clean("cycle", schedule.get("survey_cycle")),
            "project_name": schedule.get("project_name"), "schedule_name": schedule.get("schedule_name"),
            # WFM ids, written into every silver table (wfm_ids.py)
            "project_id": schedule.get("project_id"), "zone_id": schedule.get("nsv_zone_id"),
            "ro_id": schedule.get("ro_id"), "state_id": schedule.get("state_id"), "piu_id": schedule.get("piu_id"),
            "nh_no": schedule.get("nh_no"), "survey_vendor_id": schedule.get("survey_vendor_id")}


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
            flat += [(f"{table}.{k}", k.split(".")[-1].split("[")[0].lower(), v) for k, v in cells.items()
                     if not k.startswith("_")]
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
    if r["details"].get("zone_from"):
        print(f"  zone   = {r['details']['zone']:20} <- master_zone.zone_code via {r['details']['zone_from']} "
              f"({r['details'].get('zone_name')})")
    for f in [f for f in FIELDS if not (f == "zone" and r["details"].get("zone_from"))]:
        print(f"  {f:6} = {r['found'].get(f) or '-':20} {('<- ' + r['found_in'][f]) if f in r['found_in'] else '(not found - add its column name to FIELDS)'}")
    print(f"\npipeline values: {r['details']}")
    print("ready to process: " + ("YES" if r["eligible"] else "NO - " + "; ".join(r["not_eligible_because"])))
    target = ROOT / "output" / "metadata" / f"{survey_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(r, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nsaved: {target}")
    return 0 if r["schedule"] else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
