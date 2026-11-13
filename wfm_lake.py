"""
wfm_lake.py - every survey's silver + gold also as TABLES: one folder per table, parquet parts, so reading the
folder gives ONE table with every survey in it (Spark / Athena / pandas / DuckDB read the folder as a table).

    python wfm_lake.py --survey <id> [<id> ...]     build the lake for surveys already processed (backfill)
    python wfm_lake.py --survey <id> --no-s3        local only

run_pipeline.py calls write_lake after gold (+ WFM ids, + Postgres push); --no-lake switches it off.

Four datasets:
    silver_kpi       silver dlm_kpi/      every silver table, all rows (breach + non-breach)
    silver_postgres  silver dlm_postgres/ every silver table as for Postgres (breach rows)
    gold_kpi         gold ris_trends_dw, ris_view_dw, final_intermediate (as gold wrote them)
    gold_postgres    gold ris_trends_kpi, ris_view_kpi (the Postgres names + value fixes, e.g. Status open/close)

Layout (<lake> = <output>/lake locally, <output-s3>/lake in S3; env NSV_LAKE_S3 overrides the S3 place):
    <lake>/<dataset>/history/<table>/ingest_date=<YYYY-MM-DD>/part-<survey id>-<time>.parquet
        every run appended, only partitioned by the ingest date (nothing else in the path)
    <lake>/<dataset>/latest/<table>/part-<survey id>.parquet
        all surveys / UPCs of the table in ONE folder, one part per survey: a new survey adds a part,
        a re-run of the same survey replaces its own part (so 'latest' never holds a survey twice)

Every row gets (when the table does not already have the column): survey_id, upc, zone, phase, cycle,
run_date, ingest_date, ingested_at. Types are made the same for every survey so the parts always read as one
table: numbers -> double, true/false -> boolean, dates -> timestamp, everything else (text, lists) -> string.
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
DATASETS = ["silver_kpi", "silver_postgres", "gold_kpi", "gold_postgres"]
GOLD_KPI = ["ris_trends_dw", "ris_view_dw", "final_intermediate"]
EXTRA = ["survey_id", "upc", "zone", "phase", "cycle", "run_date", "ingest_date", "ingested_at"]


# =============================================================================
# The tables of one survey
# =============================================================================
def survey_tables(silver_dir, gold_dir, log=print):
    """{dataset: {table: DataFrame}} from the silver / gold run_date folders (what exists)."""
    silver_dir, gold_dir = Path(silver_dir), Path(gold_dir)
    out = {d: {} for d in DATASETS}
    for dataset, sub in (("silver_kpi", "dlm_kpi"), ("silver_postgres", "dlm_postgres")):
        for f in sorted((silver_dir / sub).glob("*.parquet")):
            out[dataset][f.stem] = pd.read_parquet(f)
    for t in GOLD_KPI:
        f = gold_dir / f"{t}.csv"
        if f.is_file():
            out["gold_kpi"][t] = pd.read_csv(f, low_memory=False)
    import wfm_postgres_push as pg                                  # the same names / value fixes as the push
    for t, target in pg.DEFAULT_TABLE_MAP.items():
        if t in out["gold_kpi"]:
            out["gold_postgres"][target] = pg.apply_value_map(t, out["gold_kpi"][t].copy(), log=lambda *_: None)
    return out


def _is_list(v):
    return isinstance(v, (list, tuple)) or (hasattr(v, "tolist") and getattr(v, "ndim", 0) >= 1)


def _text(v):
    if v is None or (isinstance(v, float) and v != v):
        return None
    if _is_list(v):
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
    values = {"survey_id": info.get("survey_id"), "upc": info.get("upc"), "zone": info.get("zone"),
              "phase": info.get("phase"), "cycle": info.get("cycle"), "run_date": info.get("run_date"),
              "ingest_date": ingested_at[:10], "ingested_at": ingested_at}
    df = df.copy()
    for c in EXTRA:
        if c not in have:
            df[c] = None if values[c] in (None, "") else str(values[c])
    return df


# =============================================================================
# Write
# =============================================================================
def write_lake(silver_dir, gold_dir, info, output, lake_s3=None, log=print):
    """Write this survey's tables into <output>/lake (and lake_s3). info: survey_id, upc, zone, phase, cycle,
    run_date. Returns {dataset: {table: rows}}."""
    sid = info["survey_id"]
    now = datetime.now()
    ingested_at, stamp = now.strftime("%Y-%m-%d %H:%M:%S"), now.strftime("%Y%m%d_%H%M%S")
    lake = Path(output) / "lake"
    client = None
    if lake_s3:
        import boto3
        client = boto3.client("s3")
        bucket, _, prefix = lake_s3[5:].partition("/")
    log(f"  lake: {lake}" + (f"  ->  {lake_s3}" if lake_s3 else "") + f"  survey {sid}  ingest_date {ingested_at[:10]}")
    done = {}
    for dataset, tables in survey_tables(silver_dir, gold_dir, log).items():
        for table, df in tables.items():
            df = normalize(with_extra(df, info, ingested_at))
            rels = (f"{dataset}/history/{table}/ingest_date={ingested_at[:10]}/part-{sid}-{stamp}.parquet",
                    f"{dataset}/latest/{table}/part-{sid}.parquet")
            for i, rel in enumerate(rels):
                f = lake / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                if i == 0:
                    df.to_parquet(f, index=False)
                else:
                    shutil.copyfile(lake / rels[0], f)               # the same file: written once
                if client:
                    client.upload_file(str(f), bucket, f"{prefix.rstrip('/')}/{rel}")
            done.setdefault(dataset, {})[table] = len(df)
        if done.get(dataset):
            log(f"    {dataset:16} {len(done[dataset]):3} table(s), {sum(done[dataset].values()):8} row(s)")
    return done


def lake_s3_default(output_s3=None):
    s3 = os.environ.get("NSV_LAKE_S3") or (f"{output_s3.rstrip('/')}/lake" if output_s3 else None)
    return s3.rstrip("/") if s3 else None


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey", nargs="+", required=True)
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--output-s3", default=os.environ.get("NSV_OUTPUT_S3"))
    ap.add_argument("--no-s3", action="store_true", help="write the local lake only")
    o = ap.parse_args(args)
    import wfm_postgres_push as pg
    s3 = None if o.no_s3 else lake_s3_default(o.output_s3)
    failed = 0
    for sid in o.survey:
        try:
            silver, gold, e = pg.survey_folders(sid, o.output)
            info = {"survey_id": sid, "upc": e["upc"], "zone": e["zone"], "phase": e["phase"], "cycle": e["cycle"],
                    "run_date": silver.name.replace("run_date=", "")}
            print(f"\n{sid}  UPC {e['upc']}  zone {e['zone']}  phase {e['phase']}  cycle {e['cycle']}  ({silver.name})")
            write_lake(silver, gold, info, o.output, s3)
        except Exception as ex:
            failed += 1
            print(f"{sid}: FAILED {type(ex).__name__}: {ex}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
