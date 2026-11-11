"""
wfm_postgres_push.py - append a survey's silver + gold tables to the EXISTING tables of the WFM Postgres
(the database the survey status comes from: postgres_creds.txt, first of TARGET_DBS).

    python wfm_postgres_push.py --survey <id> [<id> ...]                 INSPECT only (default): nothing is written
    python wfm_postgres_push.py --survey <id> [<id> ...] --write         append
    options: --schema dlm (default env NSV_PG_TARGET_SCHEMA, else 'dlm') | --map ris_view_dw=ris_view_app_dic
             --no-delete (keep this survey's earlier rows: duplicates on a rerun) | --output <output folder>

What is pushed (latest run_date of the survey, folders found from the processed log output/state/):
  silver  <silver>/dlm_postgres/<table>.parquet   breach rows of the 13 defect tables
          <silver>/dlm_kpi/master_reporting_interval, dlm_threshold_mapping
  gold    <gold>/ris_trends_dw.csv, ris_view_dw.csv  (Status mapped Open -> open, Closed -> close)
Rules:
  - only tables that already exist in the schema are written (a missing table is reported, never created)
  - only columns the table has are written; data columns the table does not have are listed and left out;
    table columns the data does not have stay NULL / their default
  - values are converted to the column's type (integer, numeric, boolean, date / timestamp, text, json, uuid)
  - append: before the insert this survey's own earlier rows (same upc + phase + cycle) are deleted, so a rerun
    replaces them instead of duplicating them; every other row of the table is untouched (--no-delete: keep them)
  - one transaction per survey: any error -> nothing of that survey is written
"""
import argparse
import json
import math
import os
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
TARGET_SCHEMA = os.environ.get("NSV_PG_TARGET_SCHEMA", "dlm")
SILVER_TABLES = ["master_rutting_child_10m", "master_rutting_child_100m", "master_rutting_parent",
                 "master_roughness_child_10m", "master_roughness_child_100m", "master_roughness_parent",
                 "master_ravelling_child_10m", "master_ravelling_child_100m", "master_ravelling_parent",
                 "master_cracking", "master_cracking_child_100m", "master_cracking_parent",
                 "master_pothole"]
SILVER_REFERENCE = ["master_reporting_interval", "dlm_threshold_mapping"]   # no breach rows: from dlm_kpi
GOLD_TABLES = ["ris_trends_dw", "ris_view_dw"]          # final_intermediate is an intermediate step, not pushed

# Value fixes applied just before the push (the gold CSVs keep their own values).
# The PG enum ris_view_kpi_status only allows 'open' / 'close' (Databricks: ris_view_app_dic.Status).
STAGING_VALUE_MAP = {
    "ris_view_dw": {
        "Status": {"Open": "open", "Closed": "close"},
    },
}


def apply_value_map(table, df, log=print):
    """STAGING_VALUE_MAP for one table: listed values replaced, every other value kept as it is."""
    for col, vmap in STAGING_VALUE_MAP.get(table, {}).items():
        if col not in df.columns:
            log(f"  {table}: column '{col}' not found, value fix skipped")
            continue
        before = df[col].value_counts(dropna=False).to_dict()
        df[col] = df[col].map(lambda v: vmap.get(v, v))
        log(f"  {table}.{col}: {before} -> {df[col].value_counts(dropna=False).to_dict()}")
    return df


# =============================================================================
# The survey's tables
# =============================================================================
def survey_folders(survey_id, output):
    """(silver run_date folder, gold run_date folder, entry) from the processed log."""
    out = Path(output)
    state = json.loads((out / "state" / "wfm_trigger_state.json").read_text(encoding="utf-8"))
    e = state.get(survey_id)
    if not e:
        raise RuntimeError(f"{survey_id} is not in the processed log {out / 'state'}")
    part = f"zone={str(e['zone']).upper()}/phase={e['phase']}/cycle={e['cycle']}/{e['upc'].replace('/', '-')}"
    runs = sorted((out / "silver" / part).glob("run_date=*"))
    if not runs:
        raise RuntimeError(f"no silver folder {out / 'silver' / part}")
    silver = out / "silver" / part / f"run_date={e['run_date']}" if e.get("run_date") else runs[-1]
    silver = silver if silver.is_dir() else runs[-1]
    gold = out / "gold" / part / silver.name
    return silver, gold, e


def _lists_to_text(df):
    for c in df.columns:
        if df[c].dtype == object and df[c].map(lambda v: isinstance(v, (list, tuple))).any():
            df[c] = df[c].map(lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple)) else v)
    return df


def load_frames(silver, gold, log=print):
    frames = {}
    for t in SILVER_TABLES:
        f = silver / "dlm_postgres" / f"{t}.parquet"
        if f.is_file():
            frames[t] = pd.read_parquet(f)
    for t in SILVER_REFERENCE:
        f = silver / "dlm_kpi" / f"{t}.parquet"
        if f.is_file():
            frames[t] = pd.read_parquet(f)
    for t in GOLD_TABLES:
        f = gold / f"{t}.csv"
        if f.is_file():
            frames[t] = pd.read_csv(f, low_memory=False)
    for t in list(frames):
        frames[t] = apply_value_map(t, _lists_to_text(frames[t]), log)
    return frames


# =============================================================================
# Database
# =============================================================================
def _connect():
    import wfm_metadata
    c = wfm_metadata.load_creds()
    db = wfm_metadata.databases(c)[0]
    return wfm_metadata.connect(c, db), db


def table_columns(cur, schema, table):
    """{column: (data_type, udt_name)} of schema.table ({} when the table does not exist)."""
    cur.execute("SELECT column_name, data_type, udt_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position", (schema, table))
    return {r[0]: (r[1], r[2]) for r in cur.fetchall()}


def schemas_with(cur, table):
    cur.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = %s", (table,))
    return sorted(r[0] for r in cur.fetchall())


def _blank(v):
    return v is None or (isinstance(v, float) and math.isnan(v)) or (v is pd.NA) or (v is pd.NaT)


def _convert(v, data_type, udt):
    """One value -> the Python type psycopg2 sends for the column type."""
    if _blank(v):
        return None
    if isinstance(v, str) and not v.strip() and data_type not in ("text", "character varying", "character"):
        return None
    try:
        if udt in ("int2", "int4", "int8"):
            return int(round(float(v)))
        if udt in ("numeric", "float4", "float8"):
            f = float(v)
            return None if math.isnan(f) else f
        if udt == "bool":
            return str(v).strip().lower() in ("1", "1.0", "true", "t", "yes", "y")
        if udt in ("json", "jsonb"):
            return v if isinstance(v, str) else json.dumps(v, default=str)
    except (TypeError, ValueError):
        raise ValueError(f"value {v!r} does not fit a {data_type} column")
    return str(v)            # text, varchar, uuid, date, timestamp, enum: Postgres casts the text


def _key_columns(df):
    """The columns that identify this survey's rows (whatever the table calls them)."""
    pick = lambda *names: next((c for c in df.columns if c.lower() in names), None)
    return pick("upc"), pick("phase"), pick("cycle")


def plan(cur, schema, table, df, table_map):
    """What would be written for one table: dict with target, columns, rows, existing survey rows ..."""
    target = table_map.get(table, table)
    cols = table_columns(cur, schema, target)
    p = {"table": table, "target": f"{schema}.{target}", "rows": len(df), "exists": bool(cols)}
    if not cols:
        p["found_in_schemas"] = schemas_with(cur, target)
        return p
    p["columns"] = [c for c in df.columns if c in cols]
    p["not_in_table"] = [c for c in df.columns if c not in cols]
    p["table_only"] = [c for c in cols if c not in df.columns]
    ucol, pcol, ccol = _key_columns(df)
    keys = df[[c for c in (ucol, pcol, ccol) if c]].drop_duplicates() if ucol else pd.DataFrame()
    p["keys"] = (ucol, pcol, ccol)
    as_text = lambda v: str(int(v)) if isinstance(v, float) and v.is_integer() else str(v).strip()   # 2.0 -> '2'
    p["key_values"] = [[as_text(v) for v in row] for row in keys.itertuples(index=False, name=None)]
    existing = 0
    for row in p["key_values"]:
        where = " AND ".join(f'CAST("{c}" AS TEXT) = %s' for c in (ucol, pcol, ccol) if c)
        cur.execute(f'SELECT count(*) FROM "{schema}"."{target}" WHERE {where}', row)
        existing += cur.fetchone()[0]
    p["existing_survey_rows"] = existing
    return p


def write(cur, schema, p, df, delete=True):
    from psycopg2.extras import execute_values
    target = p["target"].split(".", 1)[1]
    cols = table_columns(cur, schema, target)
    deleted = 0
    if delete and p["key_values"]:
        ucol, pcol, ccol = p["keys"]
        where = " AND ".join(f'CAST("{c}" AS TEXT) = %s' for c in (ucol, pcol, ccol) if c)
        for row in p["key_values"]:
            cur.execute(f'DELETE FROM "{schema}"."{target}" WHERE {where}', row)
            deleted += cur.rowcount
    use = p["columns"]
    values = [tuple(_convert(v, *cols[c]) for c, v in zip(use, r)) for r in df[use].itertuples(index=False, name=None)]
    if values:
        execute_values(cur, f'INSERT INTO "{schema}"."{target}" ({", ".join(chr(34) + c + chr(34) for c in use)}) '
                            f'VALUES %s', values, page_size=1000)
    return deleted, len(values)


# =============================================================================
def push_survey(survey_id, output, schema, table_map, do_write, delete, log=print):
    silver, gold, e = survey_folders(survey_id, output)
    log(f"\n==================== {survey_id}  UPC {e['upc']}  zone {e['zone']}  phase {e['phase']}  "
        f"cycle {e['cycle']}  ({e.get('status')})")
    log(f"  silver {silver}\n  gold   {gold}")
    frames = load_frames(silver, gold, log)
    conn, db = _connect()
    log(f"  database {db}, schema {schema}  -  {'WRITE' if do_write else 'INSPECT only (add --write to append)'}")
    try:
        with conn.cursor() as cur:
            plans = {t: plan(cur, schema, t, df, table_map) for t, df in frames.items()}
            for t, p in plans.items():
                if not p["exists"]:
                    log(f"  {t:30} -> {p['target']:45} TABLE NOT FOUND - skipped"
                        + (f" (exists in schema {p['found_in_schemas']})" if p["found_in_schemas"] else ""))
                    continue
                log(f"  {t:30} -> {p['target']:45} {p['rows']:6} row(s) | {len(p['columns'])} column(s) | "
                    f"this survey already there: {p['existing_survey_rows']} row(s)")
                if p["not_in_table"]:
                    log(f"      left out (not in the table): {p['not_in_table']}")
                if p["table_only"]:
                    log(f"      stay NULL (only in the table): {p['table_only']}")
            if not do_write:
                conn.rollback()
                return {t: p for t, p in plans.items()}
            done = {}
            for t, p in plans.items():
                if p["exists"] and p["rows"]:
                    done[t] = write(cur, schema, p, frames[t], delete)
            conn.commit()
            for t, (d, n) in done.items():
                log(f"  WRITTEN {plans[t]['target']:45} deleted {d:6} (earlier rows of this survey) | inserted {n:6}")
            return done
    except Exception:
        conn.rollback()
        log(f"  FAILED - nothing of {survey_id} was written (rolled back)")
        raise
    finally:
        conn.close()


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey", nargs="+", required=True)
    ap.add_argument("--write", action="store_true", help="append (default: inspect only)")
    ap.add_argument("--schema", default=TARGET_SCHEMA)
    ap.add_argument("--map", nargs="*", default=[], help="pipeline_table=db_table, e.g. ris_view_dw=ris_view_app_dic")
    ap.add_argument("--no-delete", action="store_true", help="do not delete this survey's earlier rows first")
    ap.add_argument("--output", default=str(ROOT / "output"))
    o = ap.parse_args(args)
    table_map = dict(m.split("=", 1) for m in o.map)
    failed = 0
    for sid in o.survey:
        try:
            push_survey(sid, o.output, o.schema, table_map, o.write, not o.no_delete)
        except Exception as e:
            failed += 1
            print(f"  ERROR {sid}: {type(e).__name__}: {e}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
