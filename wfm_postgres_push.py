"""
wfm_postgres_push.py - load a survey's silver / gold tables into the WFM Postgres (the database the survey
status comes from).   NOT ACTIVE YET: run_pipeline.py has the call commented out (see "POSTGRES PUSH").

    python wfm_postgres_push.py <silver folder> <gold folder> --upc N/02001/12001/GJ --phase 2 --cycle 1 [--dry-run]

  silver  <silver folder>/dlm_postgres/<table>.parquet   (breach rows)  -> <schema>.<table>
  gold    <gold folder>/<table>.csv                                    -> <schema>.<table>
  Rows of the same UPC + phase + cycle are deleted first, so a rerun replaces them (one transaction per table).
  Tables that do not exist are created from the data (pandas types) - check them before the first real push.
  Target schema: env NSV_PG_TARGET_SCHEMA (default 'dlm'). Credentials: as wfm_metadata.py (postgres_creds.txt).
"""
import argparse
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
                 "master_pothole", "master_reporting_interval", "dlm_threshold_mapping"]
GOLD_TABLES = ["ris_trends_dw", "ris_view_dw"]          # final_intermediate is an intermediate step, not pushed


def _engine():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL
    import wfm_metadata
    c = wfm_metadata.load_creds()
    db = wfm_metadata.databases(c)[0]
    return create_engine(URL.create("postgresql+psycopg2", username=c["DB_USER"], password=c["DB_PASSWORD"],
                                    host=c["DB_HOST"], port=int(c.get("DB_PORT") or 5432), database=db))


def _key_columns(df):
    """The columns that identify this survey's rows in a table (whatever the table calls them)."""
    upc = next((c for c in ("upc", "upc_code", "UPC") if c in df.columns), None)
    phase = next((c for c in ("phase", "Phase") if c in df.columns), None)
    cycle = next((c for c in ("cycle", "Cycle") if c in df.columns), None)
    return upc, phase, cycle


def push(silver_dir, gold_dir, upc, phase, cycle, dry_run=False, log=print):
    from sqlalchemy import inspect, text
    frames = {}
    for t in SILVER_TABLES:
        f = Path(silver_dir) / "dlm_postgres" / f"{t}.parquet"
        f = f if f.is_file() else Path(silver_dir) / "dlm_kpi" / f"{t}.parquet"     # tables without breach rows
        if f.is_file():
            frames[t] = pd.read_parquet(f)
    for t in GOLD_TABLES:
        f = Path(gold_dir) / f"{t}.csv"
        if f.is_file():
            frames[t] = pd.read_csv(f, low_memory=False)
    for df in frames.values():                               # lists (image_urls) -> text for Postgres
        for c in df.columns:
            if df[c].dtype == object and df[c].map(lambda v: isinstance(v, (list, tuple))).any():
                df[c] = df[c].map(lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple)) else v)

    log(f"POSTGRES PUSH -> schema {TARGET_SCHEMA}: " + ", ".join(f"{t} ({len(d)})" for t, d in frames.items())
        + ("  (dry run)" if dry_run else ""))
    if dry_run:
        return {t: len(d) for t, d in frames.items()}
    engine = _engine()
    existing = set(inspect(engine).get_table_names(schema=TARGET_SCHEMA))
    done = {}
    for t, df in frames.items():
        with engine.begin() as conn:                          # one transaction per table
            if t in existing:
                ucol, pcol, ccol = _key_columns(df)
                if ucol:
                    where = [f'"{ucol}" = :upc'] + ([f'CAST("{pcol}" AS TEXT) = :phase'] if pcol else []) \
                            + ([f'CAST("{ccol}" AS TEXT) = :cycle'] if ccol else [])
                    conn.execute(text(f'DELETE FROM "{TARGET_SCHEMA}"."{t}" WHERE ' + " AND ".join(where)),
                                 {"upc": upc if ucol != "upc_code" else upc.replace("/", "_"),
                                  "phase": str(phase), "cycle": str(cycle)})
            df.to_sql(t, conn, schema=TARGET_SCHEMA, if_exists="append", index=False, method="multi", chunksize=2000)
        done[t] = len(df)
        log(f"  {TARGET_SCHEMA}.{t}: {len(df)} row(s)")
    return done


def main(args):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("silver_dir")
    ap.add_argument("gold_dir")
    ap.add_argument("--upc", required=True)
    ap.add_argument("--phase", required=True)
    ap.add_argument("--cycle", required=True)
    ap.add_argument("--dry-run", action="store_true")
    o = ap.parse_args(args)
    push(o.silver_dir, o.gold_dir, o.upc, o.phase, o.cycle, o.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
