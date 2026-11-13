"""
wfm_lake.py - the 'Latest' tables: every silver / gold table as ONE parquet file holding every survey
(all UPCs). Plus the S3 layout of the whole pipeline (s3_rel).

    python wfm_lake.py --survey <id> [<id> ...]     add surveys already processed (backfill)
    python wfm_lake.py --survey <id> --no-s3        local only

run_pipeline.py calls write_latest after gold (+ WFM ids, + Postgres push); --no-latest switches it off.

S3 (<root> = --output-s3 / env NSV_OUTPUT_S3, e.g. s3://dhms-new-stgae/NSV):
    <root>/Bronze/zone=<z>/phase=<p>/cycle=<c>/<UPC>/d1..d4/run=<time>/        bronze (as before)
    <root>/Silver/zone=<z>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/dlm_kpi/   silver per survey, ALL rows
    <root>/Silver/Latest/<table>/<table>.parquet                                ONE file, every survey
    <root>/Gold/zone=<z>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/            gold per survey
    <root>/Gold/Latest/<table>/<table>.parquet                                  ONE file, every survey
    <root>/logs/...  <root>/state/wfm_trigger_state.json                        as before
Locally the same files are in <output>/latest/silver/<table>/ and <output>/latest/gold/<table>/.

Latest: each run reads the table's file (from S3 when --output-s3 is set, else the local one), removes the rows of
the survey being processed (a re-run never doubles a survey), appends the survey's rows and writes ONE file again
(overwritten). Any other parquet file in the folder (e.g. the earlier part-<survey id>.parquet files) is merged in
and then deleted, so the folder always holds exactly one file. Silver = every row (breach and non-breach); the Postgres push
takes breach_flag = 1 itself. Gold = ris_trends_dw, ris_view_dw, final_intermediate as gold wrote them.

Every row gets (when the table does not already have the column): survey_id, upc, zone, phase, cycle,
run_date, ingested_at. Types are the same for every survey so the rows always fit one table:
numbers -> double, true/false -> boolean, dates -> timestamp, everything else (text, lists) -> string.
"""
import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
GOLD_TABLES = ["ris_trends_dw", "ris_view_dw", "final_intermediate"]
EXTRA = ["survey_id", "upc", "zone", "phase", "cycle", "run_date", "ingested_at"]
# local folder (relative to <output>) -> S3 folder (relative to --output-s3); longest match wins
S3_LAYOUT = {"bronze/nsv": "Bronze", "silver": "Silver", "gold": "Gold",
             "latest/silver": "Silver/Latest", "latest/gold": "Gold/Latest"}


def s3_rel(rel):
    """Local path relative to <output> -> the path in S3 relative to the root (S3_LAYOUT; others unchanged)."""
    rel = Path(rel).as_posix()
    for local in sorted(S3_LAYOUT, key=len, reverse=True):
        if rel == local or rel.startswith(local + "/"):
            return S3_LAYOUT[local] + rel[len(local):]
    return rel


# =============================================================================
# The tables of one survey
# =============================================================================
def survey_tables(silver_dir, gold_dir):
    """{'silver': {table: DataFrame}, 'gold': {...}} - silver dlm_kpi (all rows), gold csv."""
    out = {"silver": {}, "gold": {}}
    for f in sorted((Path(silver_dir) / "dlm_kpi").glob("*.parquet")):
        out["silver"][f.stem] = pd.read_parquet(f)
    for t in GOLD_TABLES:
        f = Path(gold_dir) / f"{t}.csv"
        if f.is_file():
            out["gold"][t] = pd.read_csv(f, low_memory=False)
    return out


def _text(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    if isinstance(v, (list, tuple)) or (hasattr(v, "tolist") and getattr(v, "ndim", 0) >= 1):
        return json.dumps([x.item() if hasattr(x, "item") else x for x in list(v)], default=str)
    if isinstance(v, dict):
        return json.dumps(v, default=str)
    return str(v)


def normalize(df):
    """Same column types for every survey: double / boolean / timestamp / string."""
    df = df.copy()
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_bool_dtype(s):
            df[c] = s.astype("boolean")
        elif pd.api.types.is_numeric_dtype(s):
            df[c] = s.astype("float64")
        elif pd.api.types.is_datetime64_any_dtype(s):
            df[c] = pd.to_datetime(s, utc=True).dt.tz_localize(None).astype("datetime64[us]")
        else:
            vals = s.dropna()
            kinds = set(vals.map(type).unique()) if len(vals) else set()
            if kinds and kinds <= {bool}:
                df[c] = s.astype("boolean")
            elif any(k in (list, tuple, dict) or hasattr(k, "ndim") for k in kinds):
                df[c] = s.map(_text).astype("string")            # lists / arrays -> JSON text
            else:
                df[c] = s.astype("string")
    return df


def with_extra(df, info, ingested_at):
    """The survey columns added at the end (only those the table does not have, in any letter case)."""
    have = {str(c).lower() for c in df.columns}
    values = {**{k: info.get(k) for k in EXTRA}, "ingested_at": ingested_at}
    df = df.copy()
    for c in EXTRA:
        if c not in have:
            df[c] = None if values[c] in (None, "") else str(values[c])
    return df


# =============================================================================
# Write
# =============================================================================
def _merge(old_frames, new, sid):
    """Earlier rows (without this survey) + this survey's rows, one normalized frame."""
    keep = []
    for df in old_frames:
        if "survey_id" in df.columns:
            df = df[df["survey_id"].astype("string") != sid]
        if len(df):
            keep.append(df)
    if not keep:
        return new
    return normalize(pd.concat(keep + [new], ignore_index=True, sort=False))


def _update_table(table_dir, name, new, sid, s3=None):
    """table_dir/<name>.parquet = every file of the folder (local, or S3 when s3=(client, bucket, prefix)) without
    this survey + new; the other files of the folder are deleted. Returns the total rows of the table."""
    import tempfile
    table_dir.mkdir(parents=True, exist_ok=True)
    target = table_dir / f"{name}.parquet"
    old, s3_keys = [], []
    if s3:
        client, bucket, prefix = s3
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/"):
            s3_keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
        with tempfile.TemporaryDirectory() as tmp:
            for i, key in enumerate(s3_keys):
                f = Path(tmp) / f"{i}.parquet"
                client.download_file(bucket, key, str(f))
                old.append(pd.read_parquet(f))
    else:
        old = [pd.read_parquet(f) for f in sorted(table_dir.glob("*.parquet"))]
    df = _merge(old, new, sid)
    tmp = table_dir / f".{name}.tmp"
    df.to_parquet(tmp, index=False)
    shutil.move(str(tmp), str(target))                     # never a half-written file
    for f in table_dir.glob("*.parquet"):                  # local: only the one file
        if f != target:
            f.unlink()
    if s3:
        key = f"{prefix}/{name}.parquet"
        client.upload_file(str(target), bucket, key)       # the new file first, then the old ones go
        for k in s3_keys:
            if k != key:
                client.delete_object(Bucket=bucket, Key=k)
    return len(df)


def write_latest(silver_dir, gold_dir, info, output, output_s3=None, log=print):
    """This survey into every Latest table: <output>/latest/<layer>/<table>/<table>.parquet (one file), the same
    file in <output_s3>/<Layer>/Latest/<table>/. info: survey_id, upc, zone, phase, cycle, run_date.
    Returns {layer: {table: [rows of this survey, rows of the table]}}."""
    sid = info["survey_id"]
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out = Path(output)
    client = bucket = prefix = None
    if output_s3:
        import boto3
        client = boto3.client("s3")
        bucket, _, prefix = output_s3.rstrip("/")[5:].partition("/")
    done = {}
    for layer, tables in survey_tables(silver_dir, gold_dir).items():
        for table, df in tables.items():
            rel = f"latest/{layer}/{table}"
            s3 = (client, bucket, f"{prefix}/{s3_rel(rel)}".lstrip("/")) if client else None
            total = _update_table(out / rel, table, normalize(with_extra(df, info, ingested_at)), sid, s3)
            done.setdefault(layer, {})[table] = [len(df), total]
        if done.get(layer):
            where = f"{output_s3.rstrip('/')}/{s3_rel(f'latest/{layer}')}" if client else out / "latest" / layer
            log(f"  Latest {layer:6} {len(done[layer]):3} table(s): +{sum(v[0] for v in done[layer].values()):8} row(s) "
                f"of this survey, {sum(v[1] for v in done[layer].values()):9} row(s) in all -> {where}")
    return done


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey", nargs="+", required=True)
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--output-s3", default=os.environ.get("NSV_OUTPUT_S3"))
    ap.add_argument("--no-s3", action="store_true", help="write the local Latest tables only")
    o = ap.parse_args(args)
    import wfm_postgres_push as pg
    failed = 0
    for sid in o.survey:
        try:
            silver, gold, e = pg.survey_folders(sid, o.output)
            info = {"survey_id": sid, "upc": e["upc"], "zone": e["zone"], "phase": e["phase"], "cycle": e["cycle"],
                    "run_date": silver.name.replace("run_date=", "")}
            print(f"\n{sid}  UPC {e['upc']}  zone {e['zone']}  phase {e['phase']}  cycle {e['cycle']}  ({silver.name})")
            write_latest(silver, gold, info, o.output, None if o.no_s3 else o.output_s3)
        except Exception as ex:
            failed += 1
            print(f"{sid}: FAILED {type(ex).__name__}: {ex}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
