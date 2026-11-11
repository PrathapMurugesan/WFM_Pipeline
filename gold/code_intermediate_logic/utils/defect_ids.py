"""Silver defect ids per 10m row of final_intermediate, from the tables in SILVER_STATUS_DIR.

Matched on upc / source_system / phase / cycle / lane ('As per CA' rows only):
  roughness / rutting / ravelling parent -> parent window: parent_start_chainage <= start_chainage < parent_end_chainage
  cracking (10m child)                   -> child segment: child start_chainage <= start_chainage < child end_chainage
                                            (about 29% of the children are offset from the 10m grid, e.g. 254537-254547,
                                            so an exact start/end match would miss them)
  pothole (point)                        -> start_chainage <= defect_chainage < end_chainage; a 10m row can hold
                                            several potholes -> ids joined with ',' in chainage order

Status columns come from final_status of the same silver row the id was taken from (so id + upc / phase /
cycle, never the id alone):
  <defect>_status             -> final_status of that parent / cracking row (Open / Closed / null)
  pothole_open_status_count   -> number of the row's potholes with final_status 'Open'
  pothole_closed_status_count -> number with 'Closed' (potholes without a status are in neither count);
                                 both counts are null on rows without potholes

Silver ids restart per survey: an id is only unique together with upc / phase / cycle.
"""
import logging

import numpy as np
import pandas as pd

from utils.helpers import to_str_id, upc_slash
from utils.io_utils import load_columns

log = logging.getLogger(__name__)

ID_KEYS = ["upc", "source_system", "phase", "cycle", "lane"]
THRESHOLD = "As per CA"
SEPARATOR = ","

# output column -> (silver table, start column, end column); start <= 10m row start < end
SEGMENT_SOURCES = {
    "roughness_parent_defect_id": ("master_roughness_parent", "parent_start_chainage", "parent_end_chainage"),
    "rutting_parent_defect_id": ("master_rutting_parent", "parent_start_chainage", "parent_end_chainage"),
    "ravelling_parent_defect_id": ("master_ravelling_parent", "parent_start_chainage", "parent_end_chainage"),
    "cracking_defect_id": (None, "start_chainage", "end_chainage"),  # table = CRACKING_TABLE
}
POTHOLE_COLUMN = "pothole_defect_id"
POTHOLE_TABLE = "master_pothole"

# id column -> status column
STATUS_COLUMNS = {
    "roughness_parent_defect_id": "roughness_status",
    "rutting_parent_defect_id": "rutting_status",
    "ravelling_parent_defect_id": "ravelling_status",
    "cracking_defect_id": "cracking_status",
}
POTHOLE_COUNTS = {"Open": "pothole_open_status_count", "Closed": "pothole_closed_status_count"}

DEFECT_ID_COLUMNS = list(SEGMENT_SOURCES) + [POTHOLE_COLUMN]

# Computed but not needed in the output for now: dropped just before final_intermediate is saved (main.py)
NOT_SAVED_COLUMNS = ["ravelling_parent_defect_id", "ravelling_status",
                     POTHOLE_COLUMN, *POTHOLE_COUNTS.values()]


def _normalize_keys(df):
    """Same key types on both sides: upc in '/' form, phase / cycle as '2' (not '2.0'), all as text."""
    df = df.copy()
    df["upc"] = upc_slash(df["upc"])
    for col in ("phase", "cycle"):
        df[col] = to_str_id(df[col])
    for col in ("source_system", "lane"):
        df[col] = df[col].astype("string")
    return df


def _load_source(status_dir, table, chainage_cols):
    df = load_columns(status_dir, table, ID_KEYS + ["id", "threshold_name", "final_status"] + chainage_cols)
    df = _normalize_keys(df[df["threshold_name"] == THRESHOLD])
    for col in chainage_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")  # same type as the 10m rows (merge_asof)
    return df.dropna(subset=chainage_cols + ["id"])


def _segment_ids(rows, src, lo, hi, label):
    """(id, final_status) of the src segment [lo, hi) that contains each row's start_chainage (null when none)."""
    segments = src[ID_KEYS + ["id", "final_status", lo, hi]].drop_duplicates().sort_values(lo, kind="stable")
    overlap = segments.groupby(ID_KEYS, dropna=False)[hi].shift() > segments[lo]
    if overlap.any():
        log.warning("%s: %s overlapping segments — a 10m row gets the one starting last", label, int(overlap.sum()))

    left = rows[ID_KEYS + ["_row", "start_chainage"]].dropna(subset=["start_chainage"]).sort_values("start_chainage")
    matched = pd.merge_asof(left, segments, left_on="start_chainage", right_on=lo, by=ID_KEYS, direction="backward")
    matched = matched[matched["start_chainage"] < matched[hi]]

    out = pd.DataFrame({"id": pd.NA, "final_status": pd.NA}, index=rows["_row"], dtype="string")
    out.loc[matched["_row"].to_numpy(), ["id", "final_status"]] = matched[["id", "final_status"]].astype("string").to_numpy()
    return out["id"].to_numpy(), out["final_status"].to_numpy()


def _pothole_ids(rows, potholes):
    """Per row: ','-joined ids of the potholes whose defect_chainage lies in [start_chainage, end_chainage),
    and the number of them with final_status Open / Closed."""
    grid = (rows[ID_KEYS + ["start_chainage", "end_chainage"]].dropna(subset=["start_chainage"])
            .drop_duplicates(ID_KEYS + ["start_chainage"]).sort_values("start_chainage"))
    pts = potholes.sort_values(["defect_chainage", "id"], kind="stable")
    placed = pd.merge_asof(pts, grid, left_on="defect_chainage", right_on="start_chainage",
                           by=ID_KEYS, direction="backward")
    inside = placed["defect_chainage"] < placed["end_chainage"]
    if (~inside).any():
        log.warning("Pothole: %s of %s potholes lie outside every 10m row — not placed",
                    f"{int((~inside).sum()):,}", f"{len(placed):,}")

    placed = placed[inside]
    grouped = placed.groupby(ID_KEYS + ["start_chainage"], dropna=False, sort=False)
    joined = grouped["id"].agg(lambda s: SEPARATOR.join(s.astype(str))).rename(POTHOLE_COLUMN).to_frame()
    for status, column in POTHOLE_COUNTS.items():
        joined[column] = (placed["final_status"] == status).groupby(
            [placed[k] for k in ID_KEYS + ["start_chainage"]], dropna=False, sort=False).sum()

    out = (rows[ID_KEYS + ["_row", "start_chainage"]]
           .merge(joined.reset_index(), on=ID_KEYS + ["start_chainage"], how="left").sort_values("_row"))
    out[POTHOLE_COLUMN] = out[POTHOLE_COLUMN].astype("string")
    for column in POTHOLE_COUNTS.values():
        out[column] = out[column].astype("Int64")  # rows without potholes stay null
    return out[[POTHOLE_COLUMN] + list(POTHOLE_COUNTS.values())].to_numpy().T


def _rollup_status(rows, src, lo, start_ids, start_statuses):
    """Per row, over every segment overlapping [start_chainage, end_chainage) — the one holding the row start plus
    those starting inside the row: Open if any is Open, Closed if all are Closed, else empty."""
    grid = (rows[ID_KEYS + ["_row", "start_chainage", "end_chainage"]].dropna(subset=["start_chainage"])
            .sort_values("start_chainage"))
    segments = src[ID_KEYS + ["id", "final_status", lo]].drop_duplicates().sort_values(lo, kind="stable")
    placed = pd.merge_asof(segments, grid, left_on=lo, right_on="start_chainage", by=ID_KEYS, direction="backward")
    placed = placed[placed[lo] < placed["end_chainage"]]

    has_start = pd.notna(start_ids)
    members = pd.concat([
        placed[["_row", "id", "final_status"]].astype({"id": "string", "final_status": "string"}),
        pd.DataFrame({"_row": rows["_row"].to_numpy()[has_start], "id": start_ids[has_start],
                      "final_status": start_statuses[has_start]}).astype({"id": "string", "final_status": "string"}),
    ]).drop_duplicates(["_row", "id"])

    status = members["final_status"]
    per_row = members.assign(_open=(status == "Open").fillna(False), _closed=(status == "Closed").fillna(False)) \
        .groupby("_row").agg(any_open=("_open", "any"), all_closed=("_closed", "all"))
    out = pd.Series(pd.NA, index=np.arange(len(rows)), dtype="string")
    out[per_row.index[per_row["all_closed"]]] = "Closed"
    out[per_row.index[per_row["any_open"]]] = "Open"
    return out.to_numpy()


def attach_segment_ids(df, status_dir, cracking_table, id_columns, rollup_status=()):
    """df + the given SEGMENT_SOURCES id columns, then their status columns. Same rule as attach_defect_ids: the
    silver segment holding the row's start_chainage (a 50 / 100 m row of final_intermediate takes the cracking
    child at its start only). For id columns in rollup_status the status covers every segment overlapping the row
    instead (Open if any is Open, Closed if all are Closed, else empty)."""
    rows = _normalize_keys(df[ID_KEYS + ["start_chainage", "end_chainage"]])
    for col in ("start_chainage", "end_chainage"):
        rows[col] = pd.to_numeric(rows[col], errors="coerce").astype("float64")
    rows["_row"] = np.arange(len(rows))
    out = df.copy()
    statuses = {}
    for column in id_columns:
        table, lo, hi = SEGMENT_SOURCES[column]
        name = table or cracking_table
        src = _load_source(status_dir, name, [lo, hi])
        out[column], statuses[STATUS_COLUMNS[column]] = _segment_ids(rows, src, lo, hi, name)
        if column in rollup_status:
            statuses[STATUS_COLUMNS[column]] = _rollup_status(rows, src, lo, out[column].to_numpy(),
                                                              statuses[STATUS_COLUMNS[column]])
    for column, values in statuses.items():
        out[column] = values

    for column in id_columns:
        n = int(out[column].notna().sum())
        log.info("  %-28s rows with id: %s / %s (%.2f%%)", column, f"{n:,}", f"{len(out):,}", 100 * n / max(len(out), 1))
    for column in statuses:
        counts = out[column].value_counts(dropna=False)
        log.info("  %-28s %s", column, ", ".join(f"{'null' if pd.isna(k) else k}={v:,}" for k, v in counts.items()))
    return out


def attach_defect_ids(final, status_dir, cracking_table):
    """final_intermediate + defect id columns, then status columns (appended at the end)."""
    rows = _normalize_keys(final[ID_KEYS + ["start_chainage", "end_chainage"]])
    rows["_row"] = np.arange(len(rows))
    out = final.copy()
    statuses = {}

    for column, (table, lo, hi) in SEGMENT_SOURCES.items():
        src = _load_source(status_dir, table or cracking_table, [lo, hi])
        out[column], statuses[STATUS_COLUMNS[column]] = _segment_ids(rows, src, lo, hi, table or cracking_table)

    potholes = _load_source(status_dir, POTHOLE_TABLE, ["defect_chainage"])
    out[POTHOLE_COLUMN], *counts = _pothole_ids(rows, potholes)
    statuses.update(zip(POTHOLE_COUNTS.values(), counts))

    for column, values in statuses.items():
        out[column] = values
    for column in POTHOLE_COUNTS.values():
        out[column] = out[column].astype("Int64")

    for column in DEFECT_ID_COLUMNS:
        n = int(out[column].notna().sum())
        log.info("  %-28s rows with id: %s / %s (%.2f%%)", column, f"{n:,}", f"{len(out):,}", 100 * n / max(len(out), 1))
    multi = out[POTHOLE_COLUMN].str.contains(SEPARATOR, regex=False).fillna(False)
    log.info("  %-28s rows with >1 pothole: %s", POTHOLE_COLUMN, f"{int(multi.sum()):,}")
    for column in STATUS_COLUMNS.values():
        counts = out[column].value_counts(dropna=False)
        log.info("  %-28s %s", column, ", ".join(f"{'null' if pd.isna(k) else k}={v:,}" for k, v in counts.items()))
    for column in POTHOLE_COUNTS.values():
        log.info("  %-28s total: %s | rows > 0: %s", column, f"{int(out[column].sum()):,}",
                 f"{int((out[column] > 0).sum()):,}")
    return out
