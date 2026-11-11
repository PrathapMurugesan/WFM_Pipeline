"""Reporting intervals (RI) per UPC / phase / cycle from the silver master_reporting_interval table.

Replaces the Databricks table nhai_dhms.ris_kpi_output.upc_reporting_intervals.
Three layouts are accepted, all turned into one row per upc / phase / cycle with zone,
<defect>_interval_ca / <defect>_interval_irc and spine_interval:

- wide: one row per UPC_code / Zone / Phase / Cycle with <Defect>_Interval_CA / <Defect>_Interval_IRC
- legacy (input_silver_data/upc_reporting_intervals): 'UPC code as per O&M master' / Zone / Phase / Cycle with
        Ravelling_CA / Ravelling_IRC / Roughness_CA / Roughness_IRC / Rutting_CA / Rutting_IRC / Cracks_CA / Cracks_IRC
- long: one row per upc / phase / cycle / standard (CA or IRC) with <defect>_reporting_interval
        (falls back to overall_reporting_interval); a standard with no row gets DEFAULT_INTERVAL

spine_interval = smallest filled interval over every defect and standard (DEFAULT_INTERVAL when all are empty);
it sets the row size of final_intermediate. Empty intervals fall back to DEFAULT_INTERVAL.
Ravelling and zone are optional in the source: a missing column is logged and left empty.
"""
import logging

import pandas as pd

from utils.helpers import to_int, to_str_id
from utils.io_utils import find_table_file, read_table_file

log = logging.getLogger(__name__)

RI_KEYS = ["upc", "phase", "cycle"]
DEFECTS = ["roughness", "rutting", "cracking", "ravelling"]
STANDARDS = ["ca", "irc"]

INTERVAL_COLUMNS = [f"{d}_interval_{s}" for d in DEFECTS for s in STANDARDS]


def _digits(series):
    """regexp_extract(col, '(\\d+)', 1): 'Phase 2' -> '2', 2 -> '2'."""
    return to_str_id(series.astype("string").str.extract(r"(\d+)", expand=False))


def _optional(raw, column, cast):
    """raw[column] through cast, or an empty column (logged) when the source has no such column."""
    if column is not None and column in raw.columns:
        return cast(raw[column])
    log.warning("Reporting intervals: no column %s — left empty", column)
    return pd.Series(pd.NA, index=raw.index, dtype="Int64" if cast is to_int else "string")


def _zone(series):
    return series.astype("string").str.strip()


def _drop_duplicates(ri, label):
    dups = ri.duplicated(RI_KEYS, keep="first")
    if dups.any():
        log.warning("%s duplicate upc/phase/cycle rows%s — keeping the first of each", int(dups.sum()), label)
        ri = ri[~dups]
    return ri


def _from_wide(raw):
    ri = pd.DataFrame({
        "upc": raw["UPC_code"].astype("string"),
        "zone": _optional(raw, "Zone", _zone),
        "phase": _digits(raw["Phase"]),
        "cycle": _digits(raw["Cycle"]),
    })
    for defect in DEFECTS:
        for standard in STANDARDS:
            ri[f"{defect}_interval_{standard}"] = _optional(
                raw, f"{defect.capitalize()}_Interval_{standard.upper()}", to_int)
    return _drop_duplicates(ri, "")


LEGACY_DEFECT_NAMES = {"roughness": "Roughness", "rutting": "Rutting", "cracking": "Cracks", "ravelling": "Ravelling"}


def _from_legacy(raw):
    """Old upc_reporting_intervals layout (input_silver_data / the notebook's Databricks table):
    'UPC code as per O&M master' (or UPC_code_as_per_O&M_master), Zone, 'Phase 1', 'Cycle 1',
    Ravelling_CA / Ravelling_IRC / Roughness_CA / Roughness_IRC / Rutting_CA / Rutting_IRC / Cracks_CA / Cracks_IRC."""
    cols = {str(c).strip().replace(" ", "_").lower(): c for c in raw.columns}
    ri = pd.DataFrame({
        "upc": raw[cols["upc_code_as_per_o&m_master"]].astype("string").str.strip(),
        "zone": _optional(raw, cols.get("zone"), _zone),
        "phase": _digits(raw[cols["phase"]]),
        "cycle": _digits(raw[cols["cycle"]]),
    })
    for defect, name in LEGACY_DEFECT_NAMES.items():
        for standard in STANDARDS:
            ri[f"{defect}_interval_{standard}"] = _optional(raw, cols.get(f"{name}_{standard}".lower()), to_int)
    ri = ri[ri["upc"].notna() & (ri["upc"] != "")]
    return _drop_duplicates(ri, "")


def _standard_rows(raw, standard):
    """Rows for one standard ('ca' / 'irc'); a standard value like 'CA & IRC' counts for both."""
    std = raw["standard"].astype("string").str.upper()
    rows = raw[std.str.contains(standard.upper(), regex=False).fillna(False).astype(bool)]

    out = pd.DataFrame({
        "upc": rows["upc"].astype("string"),
        "zone": _optional(rows, "zone", _zone),
        "phase": _digits(rows["phase"]),
        "cycle": _digits(rows["cycle"]),
    })
    overall = to_int(rows["overall_reporting_interval"])
    for defect in DEFECTS:
        out[f"{defect}_interval_{standard}"] = _optional(rows, f"{defect}_reporting_interval", to_int).fillna(overall)

    return _drop_duplicates(out, f" for standard {standard.upper()}")


def _from_long(raw, default_interval):
    ca = _standard_rows(raw, "ca")
    irc = _standard_rows(raw, "irc")
    ri = ca.merge(irc, on=RI_KEYS, how="outer", indicator=True, suffixes=("", "_irc_row"))
    ri["zone"] = ri["zone"].fillna(ri.pop("zone_irc_row"))

    for side, std in (("right_only", "CA"), ("left_only", "IRC")):
        n = int((ri["_merge"] == side).sum())
        if n:
            log.warning("%s upc/phase/cycle rows have no %s reporting interval — using DEFAULT_INTERVAL=%s",
                        n, std, default_interval)
    return ri.drop(columns="_merge")


def load_reporting_intervals(input_dir, table_name, default_interval):
    """One row per upc / phase / cycle with zone, an interval (m) per defect and standard, and spine_interval.
    Empty intervals fall back to default_interval."""
    path = find_table_file(input_dir, table_name)
    raw = read_table_file(path)

    if "standard" in raw.columns:
        ri = _from_long(raw, default_interval)
    elif "Roughness_CA" in raw.columns:
        ri = _from_legacy(raw)
    else:
        ri = _from_wide(raw)

    # Smallest filled interval, taken before empty intervals are defaulted
    ri["spine_interval"] = ri[INTERVAL_COLUMNS].min(axis=1).fillna(default_interval).astype("Int64")
    for col in INTERVAL_COLUMNS:
        ri[col] = ri[col].fillna(default_interval).astype("Int64")

    log.info("Reporting intervals loaded — %s upc/phase/cycle rows (from %s)", f"{len(ri):,}", path.name)
    return ri[RI_KEYS + ["zone", "spine_interval"] + INTERVAL_COLUMNS].reset_index(drop=True)
