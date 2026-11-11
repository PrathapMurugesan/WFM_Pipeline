"""Defect report: metric per reporting-interval window, flagged where metric_value > threshold_value.

The source is chosen per defect / standard (CA, IRC) / upc / phase / cycle from the reporting interval; the first
source with rows for that standard is used ("inputs" = the measures inputs averaged over the window's rows, then the
formula — see utils/measure_windows.py):

  roughness / rutting
    1000 m -> parent (as-is) | child 100m inputs -> 1000 m | child 10m inputs -> 1000 m
    100 m  -> child 100m (as-is) | child 10m inputs -> 100 m
    other  -> child 10m inputs -> interval (e.g. 50 m) | child 100m (as-is)
  cracking (child 10m only)
    any    -> child 10m inputs -> interval

As-is sources keep their own segments; their metric_value is the provider's value.
"""
import logging

import pandas as pd

from utils.helpers import to_int
from utils.measure_windows import input_windows
from utils.reporting_intervals import RI_KEYS

log = logging.getLogger(__name__)

# Notebook table key -> silver file name (the cracking 10m table comes from settings)
SOURCE_TABLES = {
    "roughness_parent": "master_roughness_parent",
    "roughness": "master_roughness_child_10m",
    "roughness_child_100m": "master_roughness_child_100m",
    "rutting_parent": "master_rutting_parent",
    "rutting": "master_rutting_child_10m",
    "rutting_child_100m": "master_rutting_child_100m",
}

# defect -> 10m base table key (parent / child 100m keys are <defect>_parent / <defect>_child_100m)
DEFECTS = {"roughness": "roughness", "rutting": "rutting", "cracking": "cracking"}
TEN_M_ONLY = {"cracking"}  # defects computed from the child 10m only, at every interval

PARENT_INTERVAL = 1000
CHILD_100M_INTERVAL = 100
AS_IS, INPUTS = "as-is", "inputs"

REPORT_TYPES = {"ca": "As per CA", "irc": "As per IRC"}

SRC_COLS = ["source_system", "upc", "phase", "cycle", "lane",
            "start_chainage", "end_chainage", "detected_on",
            "metric_value", "threshold_name", "threshold_value"]

WINDOW_KEYS = ["source_system", "upc", "phase", "cycle", "lane", "detected_on"]


def prepare_source(df):
    # measures (child inputs) is kept when present: utils/measure_windows.py averages it into windows
    out = df[SRC_COLS + (["measures"] if "measures" in df.columns else [])].copy()
    for col in ("start_chainage", "end_chainage", "metric_value", "threshold_value"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def _candidates(defect, interval):
    """Source preference for one interval -> list of (table_key, mode); mode AS_IS keeps the source segments,
    INPUTS averages the measures inputs into windows of the interval."""
    child_10m = (DEFECTS[defect], INPUTS)
    if defect in TEN_M_ONLY:
        return [child_10m]
    parent = (f"{defect}_parent", AS_IS)
    child_100m = f"{defect}_child_100m"
    if interval == PARENT_INTERVAL:
        return [parent, (child_100m, INPUTS), child_10m]
    if interval == CHILD_100M_INTERVAL:
        return [(child_100m, AS_IS), child_10m]
    return [child_10m, (child_100m, AS_IS)]


def _has_rows(ri_rows, table_key, rt, sources):
    """Boolean mask: ri_rows whose upc/phase/cycle have rows for this standard in the table."""
    src = sources[table_key]
    avail = src.loc[src["threshold_name"] == REPORT_TYPES[rt], RI_KEYS].drop_duplicates()
    return (ri_rows.merge(avail, on=RI_KEYS, how="left", indicator=True)["_merge"] == "both").to_numpy()


def _source_groups(defect, rt, ri_rt, interval_col, sources):
    """Routing for one defect / standard -> list of (interval, table_key, mode, ri_rows)."""
    groups = []
    for iv in sorted(ri_rt[interval_col].unique()):
        remaining = ri_rt[ri_rt[interval_col] == iv]
        for table_key, mode in _candidates(defect, int(iv)):
            found = _has_rows(remaining, table_key, rt, sources)
            if found.any():
                groups.append((int(iv), table_key, mode, remaining[found]))
            remaining = remaining[~found]
    return groups


def _report_part(defect, rt, interval, table_key, mode, ri_rows, sources):
    src = sources[table_key]
    src = src[src["threshold_name"] == REPORT_TYPES[rt]]
    matched = src.merge(ri_rows[RI_KEYS], on=RI_KEYS, how="inner")

    if mode == INPUTS:
        matched = input_windows(matched, defect, interval, WINDOW_KEYS, table_key)
        reporting_interval = pd.Series(interval, index=matched.index, dtype="Int64")
    else:  # parent / child 100m as-is: actual segment length
        reporting_interval = to_int((matched["end_chainage"] - matched["start_chainage"]).abs())

    return pd.DataFrame({
        "source_system": matched["source_system"],
        "upc": matched["upc"],
        "upc_code_as_per_om_master": matched["upc"],
        "phase": matched["phase"],
        "cycle": matched["cycle"],
        "defect_detected_on": matched["detected_on"],
        "lane": matched["lane"],
        "start_chainage": matched["start_chainage"],
        "end_chainage": matched["end_chainage"],
        "defect_type": f"{defect}_{rt}",
        "reporting_interval": reporting_interval,
        "metric_value": matched["metric_value"],
        "threshold_value": matched["threshold_value"],
        "defect_flag": (matched["metric_value"] > matched["threshold_value"]).astype(int),
        "source_table": table_key if mode == AS_IS else f"{table_key} (inputs -> {interval} m)",
    })


def build_defect_report(sources, ri):
    """sources: notebook table key -> prepared source DataFrame; ri: wide reporting intervals."""
    parts = []
    for defect in DEFECTS:
        for rt in REPORT_TYPES:
            interval_col = f"{defect}_interval_{rt}"
            ri_rt = ri[RI_KEYS + [interval_col]]
            for interval, table_key, mode, ri_rows in _source_groups(defect, rt, ri_rt, interval_col, sources):
                parts.append(_report_part(defect, rt, interval, table_key, mode, ri_rows, sources))
    log.info("Defect report parts: %s", len(parts))

    report = (
        pd.concat(parts, ignore_index=True)
        .sort_values(["upc", "defect_type", "phase", "cycle", "lane", "start_chainage"], kind="stable")
        .reset_index(drop=True)
    )

    log.info("Full report: %s rows | defect types: %s",
             f"{len(report):,}", sorted(report["defect_type"].unique()))
    counts = report.groupby(["upc", "defect_type", "source_table"]).size()
    for (upc, dt, table), n in counts.items():
        log.info("  %s | %-14s | %-36s | %s rows", upc, dt, table, f"{n:,}")
    return report
