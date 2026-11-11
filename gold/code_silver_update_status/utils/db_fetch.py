"""Postgres tables (udm_defect_reference, dlm_defect_case) -> <FETCHED_DIR>/<table>.csv.

A table is fetched when its CSV is missing or REFRESH_DB_TABLES=true; otherwise the stored CSV is reused.
The tables are always read back from the CSVs, so a fresh fetch and a reused one give the same data.
"""
import logging
from pathlib import Path

import pandas as pd

from utils.io_utils import read_text_csv

log = logging.getLogger(__name__)


def _connect(db):
    import psycopg2  # only needed when something has to be fetched

    missing = [k for k in ("host", "name", "user", "password") if not getattr(db, k)]
    if missing:
        raise ValueError(f"DB settings missing in .env: {', '.join('DB_' + k.upper() for k in missing)}")
    conn = psycopg2.connect(host=db.host, port=db.port, dbname=db.name, user=db.user,
                            password=db.password, connect_timeout=30)
    log.info("PostgreSQL connected — %s/%s", db.host, db.name)
    return conn


def _fetch_table(conn, schema, table):
    from psycopg2 import sql

    query = sql.SQL("SELECT * FROM {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [d.name for d in cur.description]
        # dtype=object keeps ints with NULLs as ints (no 123 -> 123.0, which would break the id joins)
        return pd.DataFrame(cur.fetchall(), columns=columns, dtype=object)


def fetch_db_tables(settings):
    """Dict table name -> DataFrame (all columns as text), read from <FETCHED_DIR>/<table>.csv."""
    fetched_dir = Path(settings.fetched_dir)
    paths = {t: fetched_dir / f"{t}.csv" for t in settings.db_tables}
    to_fetch = [t for t, p in paths.items() if settings.refresh_db_tables or not p.exists()]

    if to_fetch:
        fetched_dir.mkdir(parents=True, exist_ok=True)
        conn = _connect(settings.db)
        try:
            for table in to_fetch:
                df = _fetch_table(conn, settings.db.schema, table)
                df.to_csv(paths[table], index=False)
                log.info("Fetched %-25s — %s rows × %s cols -> %s",
                         table, f"{len(df):,}", df.shape[1], paths[table])
        finally:
            conn.close()
            log.info("PostgreSQL connection closed")

    for table in settings.db_tables:
        if table not in to_fetch:
            log.info("Using stored %s (REFRESH_DB_TABLES=false to keep, true to re-fetch)", paths[table])
    return {t: read_text_csv(p, empty_as_null=True) for t, p in paths.items()}
