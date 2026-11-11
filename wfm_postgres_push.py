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
import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
TARGET_SCHEMA = os.environ.get("NSV_PG_TARGET_SCHEMA", "public")
# pipeline table -> DHMS table, where the names differ (--map adds to / overrides this)
DEFAULT_TABLE_MAP = {"ris_trends_dw": "ris_trends_kpi", "ris_view_dw": "ris_view_kpi"}
SILVER_TABLES = ["master_rutting_child_10m", "master_rutting_child_100m", "master_rutting_parent",
                 "master_roughness_child_10m", "master_roughness_child_100m", "master_roughness_parent",
                 "master_ravelling_child_10m", "master_ravelling_child_100m", "master_ravelling_parent",
                 "master_cracking", "master_cracking_child_100m", "master_cracking_parent",
                 "master_pothole"]
SILVER_REFERENCE = ["master_reporting_interval", "dlm_threshold_mapping"]   # no breach rows: from dlm_kpi
# defects pushed with ALL rows (breach and non-breach, silver dlm_kpi/) - as the DHMS tables hold them;
# the other defects push only their breach rows (dlm_postgres/). Change with --all-rows.
ALL_ROWS_DEFECTS = ["rutting", "roughness"]
GOLD_TABLES = ["ris_trends_dw", "ris_view_dw"]          # final_intermediate is an intermediate step, not pushed
# our column -> database column, where the names differ beyond case / '(in_m)' -> '_m' (see db_column)
COLUMN_ALIASES = {"ro_id": "region_id",
                  "roughness_average": "roughness_average_mm_km",
                  "average_rutting_depth": "average_rutting_depth_mm"}

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
def survey_info(survey_id, out):
    """upc / zone / phase / cycle (+ status, run_date) of a survey: the processed log (local, else its S3 copy),
    else output/metadata/<survey id>.json (saved by every run), else a Postgres lookup."""
    f = out / "state" / "wfm_trigger_state.json"
    state = json.loads(f.read_text(encoding="utf-8")) if f.is_file() else {}
    s3 = os.environ.get("NSV_OUTPUT_S3")
    if survey_id not in state and s3:
        try:
            import boto3
            bucket, _, key = f"{s3.rstrip('/')}/state/wfm_trigger_state.json"[5:].partition("/")
            state = json.loads(boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read())
        except Exception:
            state = {}
    if state.get(survey_id, {}).get("upc"):
        return {**state[survey_id], "from": "processed log"}
    m = out / "metadata" / f"{survey_id}.json"
    if m.is_file():
        d = json.loads(m.read_text(encoding="utf-8")).get("details") or {}
        if d.get("upc") and d.get("phase"):
            return {**d, "status": "-", "from": str(m)}
    import wfm_metadata
    d = wfm_metadata.lookup(survey_id)["details"]
    if not d.get("upc"):
        raise RuntimeError(f"{survey_id}: not in the processed log, no metadata file and not found in Postgres")
    return {**d, "status": "-", "from": "Postgres"}


def survey_folders(survey_id, output):
    """(silver run_date folder, gold run_date folder, entry) - the run_date of the processed log, else the latest."""
    out = Path(output)
    e = survey_info(survey_id, out)
    part = f"zone={str(e['zone']).upper()}/phase={e['phase']}/cycle={e['cycle']}/{e['upc'].replace('/', '-')}"
    runs = sorted((out / "silver" / part).glob("run_date=*"))
    if not runs:
        raise RuntimeError(f"no silver folder {out / 'silver' / part}")
    silver = out / "silver" / part / f"run_date={e['run_date']}" if e.get("run_date") else runs[-1]
    silver = silver if silver.is_dir() else runs[-1]
    gold = out / "gold" / part / silver.name
    return silver, gold, e


def _norm(name):
    """'Affected_length_CA(in_m)' -> 'affected_length_ca_m', 'UPC Lane Length(in km)' -> 'upc_lane_length_km'."""
    n = str(name).strip().lower()
    n = re.sub(r"\(\s*in[\s_]*(k?m)\s*\)", r"_\1", n)
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", n)).strip("_")


def db_column(col, db_cols):
    """The database column for one of our columns (exact, alias, then normalized name), None when none."""
    if col in db_cols:
        return col
    n = _norm(col)
    for cand in (COLUMN_ALIASES.get(col), COLUMN_ALIASES.get(n), n):
        if cand and cand in db_cols:
            return cand
    return None


def load_frames(silver, gold, log=print, all_rows=ALL_ROWS_DEFECTS):
    frames = {}
    for t in SILVER_TABLES:
        whole = any(t.startswith(f"master_{d}") for d in all_rows)
        f = silver / ("dlm_kpi" if whole else "dlm_postgres") / f"{t}.parquet"
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
        frames[t] = apply_value_map(t, frames[t], log)
    return frames


def load_full(silver):
    """All rows of the silver defect tables (dlm_kpi/): the source of parent rows needed by foreign keys."""
    return {t: pd.read_parquet(silver / "dlm_kpi" / f"{t}.parquet") for t in SILVER_TABLES
            if (silver / "dlm_kpi" / f"{t}.parquet").is_file()}


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


def required_columns(cur, schema, table):
    """NOT NULL columns without a default: the insert fails unless we fill them."""
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s "
                "AND is_nullable = 'NO' AND column_default IS NULL", (schema, table))
    return [r[0] for r in cur.fetchall()]


def schemas_with(cur, table):
    cur.execute("SELECT table_schema FROM information_schema.tables WHERE table_name = %s", (table,))
    return sorted(r[0] for r in cur.fetchall())


def _blank(v):
    return v is None or (isinstance(v, float) and math.isnan(v)) or (v is pd.NA) or (v is pd.NaT)


def _clean_json(x):
    """NaN / Infinity (valid in Python's json, not in Postgres) -> null, recursively."""
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    if isinstance(x, dict):
        return {k: _clean_json(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_clean_json(v) for v in x]
    return x


def _json_text(v):
    """A value for a json / jsonb column: valid JSON text ('{"rutting_avg": NaN}' -> '{"rutting_avg": null}')."""
    if isinstance(v, str):
        try:
            v = json.loads(v)                      # Python reads NaN / Infinity
        except ValueError:
            return json.dumps(v)                   # plain text -> a JSON string
    return json.dumps(_clean_json(v), default=str, allow_nan=False)


def _convert(v, data_type, udt):
    """One value -> the Python type psycopg2 sends for the column type."""
    is_list = isinstance(v, (list, tuple)) or (hasattr(v, "tolist") and getattr(v, "ndim", 0) == 1)
    if udt.startswith("_"):                                   # ARRAY column (sources, image_urls)
        if is_list:
            return [str(x) for x in list(v) if not _blank(x)]
        if _blank(v) or (isinstance(v, str) and not v.strip()):
            return None
        return [x.strip() for x in str(v).split(",") if x.strip()]
    if is_list:
        v = ", ".join(map(str, list(v)))
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
            return _json_text(v)
    except (TypeError, ValueError):
        raise ValueError(f"value {v!r} does not fit a {data_type} column")
    return str(v)            # text, varchar, uuid, date, timestamp, enum: Postgres casts the text


def _key_columns(df):
    """The columns that identify this survey's rows (database names, after db_column)."""
    pick = lambda *names: next((c for c in df.columns if c.lower() in names), None)
    return pick("upc"), pick("phase"), pick("cycle")


def plan(cur, schema, table, df, table_map):
    """What would be written for one table: dict with target, columns, rows, existing survey rows ...
    Our columns are renamed to the database names (db_column); p['frame'] is what gets inserted."""
    target = table_map.get(table, table)
    cols = table_columns(cur, schema, target)
    p = {"table": table, "target": f"{schema}.{target}", "rows": len(df), "exists": bool(cols)}
    if not cols:
        p["found_in_schemas"] = schemas_with(cur, target)
        return p
    rename = {}
    for c in df.columns:
        d = db_column(c, cols)
        if d and d not in rename.values():
            rename[c] = d
    p["rename"] = rename
    p["renamed"] = {a: b for a, b in rename.items() if a != b}
    p["not_in_table"] = [c for c in df.columns if c not in rename]
    df = df[list(rename)].rename(columns=rename)
    p["frame"] = df
    p["columns"] = list(df.columns)
    p["table_only"] = [c for c in cols if c not in df.columns]
    p["required_missing"] = [c for c in required_columns(cur, schema, target) if c not in df.columns]
    # this survey's rows in the table: survey_id when both sides have it, else upc + phase + cycle; a table that
    # has neither (e.g. only upc) gets no delete key -> blocked (a delete by upc alone would hit other cycles)
    ucol, pcol, ccol = _key_columns(df)
    if "survey_id" in df.columns and df["survey_id"].notna().all():
        kcols = ["survey_id"]
    elif ucol and pcol and ccol:
        kcols = [ucol, pcol, ccol]
    else:
        kcols = []
    p["keys"] = kcols
    as_text = lambda v: str(int(v)) if isinstance(v, float) and v.is_integer() else str(v).strip()   # 2.0 -> '2'
    keys = df[kcols].drop_duplicates() if kcols else pd.DataFrame()
    p["key_values"] = [[as_text(v) for v in row] for row in keys.itertuples(index=False, name=None)]
    where = " AND ".join(f'CAST("{c}" AS TEXT) = %s' for c in kcols)
    existing = 0
    for row in p["key_values"]:
        cur.execute(f'SELECT count(*) FROM "{schema}"."{target}" WHERE {where}', row)
        existing += cur.fetchone()[0]
    p["existing_survey_rows"] = existing if kcols else "?"
    p["id_clash"] = 0                       # our ids already used by rows of ANOTHER survey (primary key conflict)
    if "id" in df.columns and len(p["key_values"]) == 1:
        cur.execute(f'SELECT count(*) FROM "{schema}"."{target}" WHERE CAST(id AS TEXT) = ANY(%s) AND NOT ({where})',
                    [df["id"].astype(str).tolist()] + p["key_values"][0])
        p["id_clash"] = cur.fetchone()[0]
    p["no_key"] = not kcols
    p["blocked"] = bool(p["required_missing"]) or bool(p["id_clash"]) or p["no_key"]
    return p


def delete_rows(cur, schema, p):
    """This survey's earlier rows of the table (by p['keys'])."""
    target = p["target"].split(".", 1)[1]
    deleted = 0
    if p["key_values"]:
        where = " AND ".join(f'CAST("{c}" AS TEXT) = %s' for c in p["keys"])
        for row in p["key_values"]:
            cur.execute(f'DELETE FROM "{schema}"."{target}" WHERE {where}', row)
            deleted += cur.rowcount
    return deleted


def insert_rows(cur, schema, p):
    from psycopg2.extras import execute_values
    df = p["frame"]
    target = p["target"].split(".", 1)[1]
    cols = table_columns(cur, schema, target)
    use = p["columns"]
    values = [tuple(_convert(v, *cols[c]) for c, v in zip(use, r)) for r in df[use].itertuples(index=False, name=None)]
    if values:
        execute_values(cur, f'INSERT INTO "{schema}"."{target}" ({", ".join(chr(34) + c + chr(34) for c in use)}) '
                            f'VALUES %s', values, page_size=1000)
    return len(values)


# =============================================================================
# Foreign keys: a child row (e.g. master_rutting_child_10m.parent_id) needs its parent row. Only breach rows are
# pushed, and the parent of a breached child is often not breached itself -> the referenced rows are added from
# silver dlm_kpi/ (all rows of this survey). Tables are written parents first (deleted children first).
# =============================================================================
def foreign_keys(cur, schema, table):
    """[(name, [columns], referenced table, [referenced columns])] of schema.table."""
    cur.execute("""
        SELECT con.conname, ref.relname,
               (SELECT array_agg(a.attname ORDER BY k.n) FROM unnest(con.conkey) WITH ORDINALITY k(att, n)
                  JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.att),
               (SELECT array_agg(a.attname ORDER BY k.n) FROM unnest(con.confkey) WITH ORDINALITY k(att, n)
                  JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.att)
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid JOIN pg_namespace ns ON ns.oid = rel.relnamespace
        JOIN pg_class ref ON ref.oid = con.confrelid
        WHERE con.contype = 'f' AND ns.nspname = %s AND rel.relname = %s""", (schema, table))
    return [(n, list(c), r, list(rc)) for n, r, c, rc in cur.fetchall()]


def _as_text(v):
    if _blank(v):
        return None
    return str(int(v)) if isinstance(v, float) and v.is_integer() else str(v).strip()


def _tuples(df, cols):
    if not all(c in df.columns for c in cols):
        return set()
    return {t for t in (tuple(_as_text(v) for v in row) for row in df[cols].itertuples(index=False, name=None))
            if None not in t}


def _in_db(cur, schema, table, cols, wanted, skip=None):
    """Which of the wanted key tuples exist in schema.table. skip = (key columns, [key values]) of this survey's
    rows, which are deleted before the insert and so do not count."""
    if not wanted:
        return set()
    first = sorted({t[0] for t in wanted})
    sel = ", ".join(f'CAST("{c}" AS TEXT)' for c in cols)
    sql, args = f'SELECT {sel} FROM "{schema}"."{table}" WHERE CAST("{cols[0]}" AS TEXT) = ANY(%s)', [first]
    if skip and skip[0] and skip[1]:
        for kv in skip[1]:
            sql += " AND NOT (" + " AND ".join(f'CAST("{c}" AS TEXT) = %s' for c in skip[0]) + ")"
            args += list(kv)
    cur.execute(sql, args)
    return {tuple(r) for r in cur.fetchall()} & wanted


def resolve_foreign_keys(cur, schema, plans, full, delete=True, log=print, null_unmatched=False):
    """Adds the referenced rows of our own tables (from full = silver dlm_kpi frames) and returns the write order
    (referenced tables first). A reference that is neither pushed, in the database nor in silver blocks the table."""
    ours = {p["target"].split(".", 1)[1]: t for t, p in plans.items() if p["exists"]}
    fks = {t: foreign_keys(cur, schema, p["target"].split(".", 1)[1]) for t, p in plans.items() if p["exists"]}
    for _ in range(4):                                   # parents of added parents, a few levels at most
        changed = False
        for t, p in plans.items():
            if not p["exists"] or not len(p["frame"]):
                continue
            p.setdefault("fk_missing", {})
            for name, cols, ref, rcols in fks[t]:
                need = _tuples(p["frame"], cols)
                if not need:
                    continue
                r = ours.get(ref)
                if r:
                    need -= _tuples(plans[r]["frame"], rcols)
                skip = (plans[r]["keys"], plans[r]["key_values"] or [list(k) for k in _tuples(p["frame"], plans[r]["keys"])]) \
                    if r and delete else None
                need -= _in_db(cur, schema, ref, rcols, need, skip)
                if need and r and r in full:
                    src = full[r].rename(columns=plans[r]["rename"])
                    src = src[[c for c in plans[r]["columns"] if c in src.columns]]
                    keys = [tuple(_as_text(v) for v in row) for row in src[rcols].itertuples(index=False, name=None)] \
                        if all(c in src.columns for c in rcols) else []
                    add = src[[k in need for k in keys]] if keys else src.iloc[0:0]
                    if len(add):
                        frame = pd.concat([plans[r]["frame"], add], ignore_index=True)
                        frame = frame.drop_duplicates("id") if "id" in frame.columns else frame.drop_duplicates()
                        plans[r]["fk_added"] = plans[r].get("fk_added", 0) + len(frame) - len(plans[r]["frame"])
                        plans[r]["frame"], plans[r]["rows"] = frame, len(frame)
                        if plans[r]["keys"]:              # the delete key of a table that had no rows before
                            plans[r]["key_values"] = [list(k) for k in _tuples(frame, plans[r]["keys"])]
                        need -= _tuples(add, rcols)
                        changed = True
                if need:
                    p["fk_missing"][name] = (cols, ref, rcols, len(need), sorted(need)[:3])
                else:
                    p["fk_missing"].pop(name, None)
        if not changed:
            break
    if null_unmatched:                       # the reference column set to NULL where the referenced row is nowhere
        for t, p in plans.items():
            for name, (cols, ref, rcols, n, sample) in list(p.get("fk_missing", {}).items()):
                target = p["target"].split(".", 1)[1]
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND "
                            "table_name = %s AND is_nullable = 'YES'", (schema, target))
                nullable = {r[0] for r in cur.fetchall()}
                if not set(cols) <= nullable:
                    continue                 # NOT NULL column: stays blocked
                bad = [tuple(_as_text(v) for v in row) for row in p["frame"][cols].itertuples(index=False, name=None)]
                missing = {k for k in bad if None not in k} - _in_db(cur, schema, ref, rcols, {k for k in bad if None not in k})
                mask = [k in missing for k in bad]
                frame = p["frame"].copy()
                frame.loc[mask, cols] = None
                p["frame"] = frame
                p.setdefault("fk_nulled", {})[name] = (cols, ref, rcols, int(sum(mask)), sample)
                p["fk_missing"].pop(name)
    for p in plans.values():
        if p.get("fk_missing"):
            p["blocked"] = True
    # write order: a table after the tables it references (only edges between our own tables)
    order, seen = [], set()

    def visit(t, path=()):
        if t in seen or t in path:
            return
        for _, _, ref, _ in fks.get(t, []):
            if ours.get(ref) and ours[ref] != t:
                visit(ours[ref], path + (t,))
        seen.add(t)
        order.append(t)
    for t in plans:
        visit(t)
    return order


# =============================================================================
# Database ids of the gold KPI tables (foreign keys of ris_view_kpi / ris_trends_kpi)
#   survey_id        -> wfm_survey_schedule.id          = the WFM survey id
#   survey_source_id -> master_survey_system.id         system_code 'NSV'
#   upc_id           -> master_upc_project.id           upc = the survey's UPC (else listed in alternate_upcs)
#   defect_type_id   -> master_defect_type.id           (ris_view only, may stay NULL) NSV defect of that name;
#                       when several: asset type as in the DLM notebook (Pothole / Cracking 'Flexible Pavement',
#                       Roughness / Ravelling 'Pavement (NSV)', Rutting 'PAVEMENT'); still several or none -> NULL
# =============================================================================
DEFECT_ASSET_TYPE = {"pothole": "flexible pavement", "cracking": "flexible pavement",
                     "roughness": "pavement (nsv)", "ravelling": "pavement (nsv)", "rutting": "pavement"}


def _one(cur, sql, args):
    cur.execute(sql, args)
    rows = cur.fetchall()
    return str(rows[0][0]) if len(rows) == 1 else (None if not rows else "MANY")


def add_gold_ids(cur, schema, frames, survey_id, upc, log=print):
    source = _one(cur, f'SELECT id FROM "{schema}".master_survey_system WHERE upper(system_code) = %s', ("NSV",))
    upc_id = _one(cur, f'SELECT id FROM "{schema}".master_upc_project WHERE upc = %s', (upc,))
    if upc_id is None:
        upc_id = _one(cur, f'SELECT id FROM "{schema}".master_upc_project WHERE CAST(alternate_upcs AS TEXT) LIKE %s',
                      (f"%{upc}%",))
    log(f"  gold ids: survey_id {survey_id} | survey_source_id (NSV) {source} | upc_id ({upc}) {upc_id}")
    bad = [n for n, v in (("survey_source_id", source), ("upc_id", upc_id)) if v in (None, "MANY")]
    if bad:
        log(f"      NOT FOUND / NOT UNIQUE: {bad} - the gold tables stay blocked")
    cur.execute(f'SELECT id, lower(defect_name), lower(coalesce(asset_type, \'\')) FROM "{schema}".master_defect_type '
                f"WHERE coalesce(is_active, true) AND (upper(coalesce(survey_system, '')) = 'NSV' OR detectable_by_nsv)")
    by_name = {}
    for i, name, asset in cur.fetchall():
        by_name.setdefault(name, []).append((str(i), asset))
    for t in GOLD_TABLES:
        if t not in frames:
            continue
        df = frames[t].copy()
        df["survey_id"] = survey_id
        if source not in (None, "MANY"):
            df["survey_source_id"] = source
        if upc_id not in (None, "MANY"):
            df["upc_id"] = upc_id
        if t == "ris_view_dw" and "Defect_Type" in df.columns:
            ids = {}
            for d in df["Defect_Type"].dropna().unique():
                cands = by_name.get(str(d).lower(), [])
                if len(cands) > 1:
                    cands = [c for c in cands if c[1] == DEFECT_ASSET_TYPE.get(str(d).lower())]
                ids[d] = cands[0][0] if len(cands) == 1 else None
            df["defect_type_id"] = df["Defect_Type"].map(ids)
            log(f"      defect_type_id: " + ", ".join(f"{k}={v or 'NULL'}" for k, v in ids.items()))
        frames[t] = df


# =============================================================================
def push_survey(survey_id, output, schema, table_map, do_write, delete, log=print, only=None, skip=(),
                null_unmatched=False, all_rows=ALL_ROWS_DEFECTS, folders=None):
    """folders = (silver run_date folder, gold run_date folder, {'upc', 'zone', 'phase', 'cycle'}) when the caller
    knows them (run_pipeline.py); else found from the processed log / metadata."""
    table_map = {**DEFAULT_TABLE_MAP, **(table_map or {})}
    silver, gold, e = folders or survey_folders(survey_id, output)
    log(f"\n==================== {survey_id}  UPC {e['upc']}  zone {e['zone']}  phase {e['phase']}  "
        f"cycle {e['cycle']}  ({e.get('status')})")
    log(f"  silver {silver}\n  gold   {gold}")
    frames = load_frames(silver, gold, log, all_rows)
    log(f"  silver rows: ALL (breach + non-breach) for {list(all_rows)}, breach rows only for the other defects")
    if only == "silver":
        frames = {t: d for t, d in frames.items() if t not in GOLD_TABLES}
    elif only == "gold":
        frames = {t: d for t, d in frames.items() if t in GOLD_TABLES}
    for t in skip:
        if frames.pop(t, None) is not None:
            log(f"  {t:30} skipped (--skip)")
    conn, db = _connect()
    log(f"  database {db}, schema {schema}  -  {'WRITE' if do_write else 'INSPECT only (add --write to append)'}")
    try:
        with conn.cursor() as cur:
            if any(t in GOLD_TABLES for t in frames):
                add_gold_ids(cur, schema, frames, survey_id, e["upc"], log)
            plans = {t: plan(cur, schema, t, df, table_map) for t, df in frames.items()}
            order = resolve_foreign_keys(cur, schema, plans, load_full(silver) if only != "gold" else {}, delete, log,
                                         null_unmatched=null_unmatched)
            for t, p in plans.items():
                if not p["exists"]:
                    log(f"  {t:30} -> {p['target']:45} TABLE NOT FOUND - skipped"
                        + (f" (exists in schema {p['found_in_schemas']})" if p["found_in_schemas"] else ""))
                    continue
                log(f"  {t:30} -> {p['target']:45} {p['rows']:6} row(s) | {len(p['columns'])} column(s) | "
                    f"this survey already there: {p['existing_survey_rows']} row(s)"
                    + ("   ** BLOCKED **" if p["blocked"] else ""))
                if p["renamed"]:
                    log(f"      renamed: {p['renamed']}")
                if p["not_in_table"]:
                    log(f"      left out (not in the table): {p['not_in_table']}")
                if p["table_only"]:
                    log(f"      stay NULL / default (only in the table): {p['table_only']}")
                if p["required_missing"]:
                    log(f"      REQUIRED (NOT NULL, no default) but not in our data: {p['required_missing']}")
                if p["id_clash"]:
                    log(f"      ID CLASH: {p['id_clash']} of our ids already belong to rows of another survey")
                if p["no_key"]:
                    log("      NO SURVEY KEY: the table has neither survey_id nor upc + phase + cycle - this survey's "
                        "earlier rows cannot be told apart (a delete by upc alone would hit other cycles)")
                if p.get("fk_added"):
                    log(f"      + {p['fk_added']} parent row(s) added from silver dlm_kpi (referenced by child rows)")
                for name, (cols, ref, rcols, n, sample) in p.get("fk_nulled", {}).items():
                    log(f"      FOREIGN KEY {name}: {cols} set to NULL on {n} row(s) - the referenced {ref}{rcols} "
                        f"does not exist (e.g. {sample[:1]}); upc / lane / parent_start_chainage / parent_end_chainage "
                        f"still identify the parent window")
                for name, (cols, ref, rcols, n, sample) in p.get("fk_missing", {}).items():
                    log(f"      FOREIGN KEY {name}: {cols} -> {ref}{rcols}: {n} value(s) found nowhere "
                        f"(not pushed, not in the database, not in silver) e.g. {sample}")
            if not do_write:
                conn.rollback()
                return {t: p for t, p in plans.items()}
            blocked = [t for t, p in plans.items() if p.get("blocked") and p["rows"]]
            if blocked:
                raise RuntimeError(f"nothing written - blocked tables {blocked} (required columns / id clash, see "
                                   f"above); push without them with --only silver, or fix them first")
            todo = [t for t in order if plans[t]["exists"]]
            deleted = {t: delete_rows(cur, schema, plans[t]) if delete else 0 for t in reversed(todo)}  # children first
            done = {t: (deleted[t], insert_rows(cur, schema, plans[t])) for t in todo if plans[t]["rows"]}  # parents first
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
    ap.add_argument("--only", choices=["silver", "gold"], help="push only the silver or only the gold tables")
    ap.add_argument("--skip", nargs="*", default=[], help="pipeline tables to leave out, e.g. master_rutting_child_10m")
    ap.add_argument("--all-rows", nargs="*", default=ALL_ROWS_DEFECTS,
                    help="defects pushed with all rows (default: rutting roughness); '--all-rows' alone = none")
    ap.add_argument("--null-unmatched-fk", action="store_true",
                    help="set a foreign-key column to NULL where the referenced row exists nowhere (nullable columns only)")
    o = ap.parse_args(args)
    table_map = dict(m.split("=", 1) for m in o.map)
    failed = 0
    for sid in o.survey:
        try:
            push_survey(sid, o.output, o.schema, table_map, o.write, not o.no_delete, only=o.only, skip=o.skip,
                        null_unmatched=o.null_unmatched_fk, all_rows=o.all_rows)
        except Exception as e:
            failed += 1
            print(f"  ERROR {sid}: {type(e).__name__}: {e}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
