"""final_intermediate rows: the 10m master_chainage spine rolled up to the smallest reporting
interval (spine_interval) of each upc / zone / phase / cycle, then flags and affected lengths.

Windows sit on a fixed grid, floor(chainage / spine_interval) * spine_interval, so they line up with the
defect report's averaged windows; the first / last window of a run is cut to the chainage actually covered
(e.g. 254530-254550 on a 50 m grid). A window that crosses a pavement_type change becomes two rows.

Flags (flag_interval_spine) come from a defect report: per defect / standard the window comes from the parent
(interval 1000), child 100m (interval 100) or the child 10m averaged to the interval when the parent / child 100m
has no rows for that standard; 50 m windows come from the child 10m measures (utils/measure_windows.py).
A row gets flag 1 when it lies fully inside a window with defect_flag = 1 (same rule as final_intermediate).
"""
import logging

import numpy as np
import pandas as pd

from utils.helpers import upc_slash
from utils.spine_flags import DEFECT_TYPES, FLAG_TYPES, covered, defected_windows

log = logging.getLogger(__name__)

FLAG_COLUMNS = [f"{c}_{rt}_flag" for rt in ("ca", "irc") for c in (*DEFECT_TYPES, "defect")]
LENGTH_COLUMNS = [col for rt in ("ca", "irc")
                  for col in (*(f"{d}_{rt}_length" for d in DEFECT_TYPES), f"total_affected_length_{rt}")]

SURVEY_KEYS = ["upc", "zone", "phase", "cycle"]
ROW_KEYS = SURVEY_KEYS + ["lane", "source_system", "defect_detected_on", "pavement_type"]
BASE_GRID = 10  # master_chainage segment length

STRUCTURE_COLUMNS = [
    "upc", "zone", "source_system", "defect_detected_on",
    "start_chainage", "end_chainage", "phase", "cycle", "lane", "pavement_type",
    "spine_interval",
    "roughness_interval_ca", "roughness_interval_irc",
    "cracking_interval_ca", "cracking_interval_irc",
    "rutting_interval_ca", "rutting_interval_irc",
    "ravelling_interval_ca", "ravelling_interval_irc",
]


def _chainage_rows(chainage_df):
    start = pd.to_numeric(chainage_df["start_chainage"], errors="coerce")
    end = pd.to_numeric(chainage_df["end_chainage"], errors="coerce")
    rows = pd.DataFrame({
        "upc": upc_slash(chainage_df["upc"]),
        "zone": chainage_df["zone"].astype("string").str.strip(),
        "phase": chainage_df["phase"],
        "cycle": chainage_df["cycle"],
        "lane": chainage_df["lane"],
        "source_system": chainage_df["source_system"],
        "defect_detected_on": chainage_df["defect_detected_on"],
        "pavement_type": chainage_df["pavement_type"],
        "_lo": np.fmin(start, end),
        "_hi": np.fmax(start, end),
    })
    bad = rows["_lo"].isna() | rows["_hi"].isna()
    if bad.any():
        log.warning("master_chainage: %s rows without a numeric start / end chainage — dropped", int(bad.sum()))
    return rows[~bad]


def _attach_spine_interval(rows, ri, default_interval):
    rows = rows.merge(ri[SURVEY_KEYS + ["spine_interval"]], on=SURVEY_KEYS, how="left")

    missing = rows["spine_interval"].isna()
    if missing.any():
        surveys = rows.loc[missing, SURVEY_KEYS].drop_duplicates()
        log.warning("%s upc/zone/phase/cycle in master_chainage have no reporting interval row — "
                    "using DEFAULT_INTERVAL=%s: %s", len(surveys), default_interval,
                    surveys.astype(str).agg("/".join, axis=1).tolist())
        rows["spine_interval"] = rows["spine_interval"].fillna(default_interval)

    # Not yet handled: an interval that is not a multiple of the 10m grid (e.g. 12.5) cuts through 10m rows
    off_grid = rows.loc[rows["spine_interval"] % BASE_GRID != 0, SURVEY_KEYS + ["spine_interval"]].drop_duplicates()
    if not off_grid.empty:
        log.warning("spine_interval not a multiple of %s m — windows cut through 10m rows: %s",
                    BASE_GRID, off_grid.astype(str).agg("/".join, axis=1).tolist())
    return rows


def build_interval_spine(chainage_df, ri, default_interval):
    """One row per spine_interval window per upc / zone / phase / cycle / lane / source_system /
    defect_detected_on / pavement_type, with the reporting intervals of its survey."""
    rows = _attach_spine_interval(_chainage_rows(chainage_df), ri, default_interval)

    interval = rows["spine_interval"].astype(float)
    rows["_window"] = np.floor(rows["_lo"] / interval) * interval
    spine = (
        rows.groupby(ROW_KEYS + ["spine_interval", "_window"], dropna=False)
        .agg(start_chainage=("_lo", "min"), end_chainage=("_hi", "max"))
        .reset_index()
        .merge(ri.drop(columns="spine_interval"), on=SURVEY_KEYS, how="left")
    )

    spine = (
        spine[STRUCTURE_COLUMNS]
        .sort_values(["upc", "phase", "cycle", "lane", "start_chainage"], kind="stable")
        .reset_index(drop=True)
    )
    log.info("Interval spine: %s 10m rows -> %s rows", f"{len(rows):,}", f"{len(spine):,}")
    for key, n in spine.groupby(SURVEY_KEYS + ["spine_interval"], dropna=False).size().items():
        log.info("  %s | spine_interval %s m | %s rows", " / ".join(map(str, key[:-1])), key[-1], f"{n:,}")
    return spine


def flag_interval_spine(spine, defect_report):
    """spine + a 0/1 flag per defect and standard, defect_<std>_flag = 1 if any of them is 1, and the affected
    lengths <defect>_<std>_length = flag x spine_interval, total_affected_length_<std> = defect_<std>_flag x
    spine_interval."""
    spine = spine.reset_index(drop=True)
    segments = pd.DataFrame({
        **{k: spine[k] for k in ("upc", "phase", "cycle", "lane", "source_system")},
        "detected_on": spine["defect_detected_on"],
        "_row": np.arange(len(spine)),
        "_smin": np.fmin(spine["start_chainage"], spine["end_chainage"]),
        "_smax": np.fmax(spine["start_chainage"], spine["end_chainage"]),
    })
    defected = defected_windows(defect_report)

    out = spine.copy()
    for dt in FLAG_TYPES:
        out[f"{dt}_flag"] = covered(segments, defected[defected["_dt"] == dt])
    for rt in ("ca", "irc"):
        out[f"defect_{rt}_flag"] = out[[f"{d}_{rt}_flag" for d in DEFECT_TYPES]].max(axis=1)

    for rt in ("ca", "irc"):
        for d in DEFECT_TYPES:
            out[f"{d}_{rt}_length"] = out[f"{d}_{rt}_flag"] * out["spine_interval"]
        out[f"total_affected_length_{rt}"] = out[f"defect_{rt}_flag"] * out["spine_interval"]

    out = out[list(spine.columns) + FLAG_COLUMNS + LENGTH_COLUMNS]
    for col in FLAG_COLUMNS:
        counts = out[col].value_counts(dropna=False)
        log.info("  %-20s %s", col, ", ".join(f"{k}={v:,}" for k, v in counts.sort_index().items()))
    for col in LENGTH_COLUMNS:
        log.info("  %-26s total: %s m", col, f"{int(out[col].sum()):,}")
    return out
