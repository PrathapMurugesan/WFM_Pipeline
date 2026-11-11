"""Reading the silver tables / fetched DB tables and writing the updated silver tables.

Silver CSVs are read and written as raw text so every existing value is written back unchanged
(no 100 -> 100.0 or date re-formatting); only the status columns are added.
"""
import logging
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

_EXTENSIONS = (".parquet", ".csv")


def read_text_csv(path, empty_as_null=False):
    """CSV with every column as text. empty_as_null=False keeps empty cells as '' (round-trips exactly)."""
    return pd.read_csv(path, dtype=str, keep_default_na=empty_as_null, na_filter=empty_as_null,
                       encoding="utf-8-sig")


def find_table_file(directory, name):
    """Return <directory>/<name>.<ext> for the first supported extension that exists."""
    for ext in _EXTENSIONS:
        candidate = Path(directory) / f"{name}{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"No {name}.(parquet|csv) found in {directory}")


def load_silver_table(input_dir, name):
    path = find_table_file(input_dir, name)
    df = pd.read_parquet(path) if path.suffix == ".parquet" else read_text_csv(path)
    log.info("Loaded %-30s rows: %s", path.name, f"{len(df):,}")
    return df


def save_csv(df, output_dir, name):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info("Saved %s — %s rows", path, f"{len(df):,}")
    return path
