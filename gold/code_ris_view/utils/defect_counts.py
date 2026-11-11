"""Defect rows from all defect tables, project details, and breached-defect counts per grain.

Status grain (from final_status of the silver_data_with_status tables): every defect type gets two
rows, status 'Open' and 'Closed' (a missing count is 0); a defect without final_status (no DB case)
counts as NO_STATUS_AS.

IRC counts of ravelling / roughness / rutting come from their child 10m tables (the parent tables
only have 'As per CA' rows): the child's 'As per IRC' rows take the final_status of their parent,
matched on PARENT_KEYS. CA counts of these defects stay on the parent rows.
"""
import logging

import pandas as pd

log = logging.getLogger(__name__)

GRAIN_COLS = ["source_system", "upc", "phase", "cycle", "detected_on", "defect_type", "zone", "status"]
DETAIL_COLS = ["defect_id", "affected_length_ca", "affected_length_irc", "breach_flag", "threshold_name"]
COUNT_COLS = ["Total_Defects_IRC", "Total_Defects_CA"]

STATUSES = ["Open", "Closed"]
NO_STATUS_AS = "Open"  # defects with no DB case (final_status empty) are not closed

IRC = "As per IRC"
CA = "As per CA"
# Child 10m row -> its parent. parent_id alone repeats across cycles; no pavement_type, because
# children of a 'Hybrid' parent are 'Asphalt' / 'Cement Concrete'.
PARENT_KEYS = ["parent_id", "zone", "phase", "cycle", "source_system", "detected_on"]
BASE_COLS = [c for c in GRAIN_COLS if c != "status"]
CHILD_COLUMNS = list(dict.fromkeys(BASE_COLS + DETAIL_COLS + PARENT_KEYS))  # columns read from child tables


def child_irc_rows(name, parent, child):
    """'As per IRC' rows of a child 10m table, with final_status of their parent row."""
    # status from the CA parent row (a parent table that also holds 'As per IRC' rows - the WFM silver -
    # would give every key twice; for CA-only parent tables this changes nothing)
    status = parent.loc[parent["threshold_name"] == CA, PARENT_KEYS + ["final_status"]]
    if status.duplicated(PARENT_KEYS).any():
        raise ValueError(f"{name}: parent rows are not unique on {', '.join(PARENT_KEYS)}")

    irc = child[child["threshold_name"] == IRC]
    rows = irc.merge(status, on=PARENT_KEYS, how="left", validate="many_to_one", indicator=True)
    unmatched = int((rows["_merge"] == "left_only").sum())
    if unmatched:
        log.warning("%s: %s child IRC rows have no parent — status counts as %s",
                    name, f"{unmatched:,}", NO_STATUS_AS)
    log.info("%-28s — %s child IRC rows | status: %s", name, f"{len(rows):,}",
             ", ".join(f"{k}={v:,}" for k, v in rows["final_status"].value_counts(dropna=False).items()))
    return rows.drop(columns="_merge")


def combine_defects(tables, children=None):
    """All rows (no breach_flag filter here; it is applied in the defect counts) + status.

    children: {parent table name: child 10m table}. For those defects the IRC rows come from the
    child table (status from the parent) and only the CA rows of the parent are kept.
    """
    children = children or {}
    missing = [name for name, df in tables.items() if "final_status" not in df.columns]
    if missing:
        raise ValueError(f"No final_status column in {', '.join(missing)} — INPUT_DIR must be "
                         "silver_data_with_status (output of code_silver_update_status)")

    cols = BASE_COLS + DETAIL_COLS + ["final_status"]
    frames = []
    for name, df in tables.items():
        if name in children:
            frames.append(df.loc[df["threshold_name"] == CA, cols])
            frames.append(child_irc_rows(name, df, children[name])[cols])
        else:
            frames.append(df[cols])
    combined = pd.concat(frames, ignore_index=True)
    combined["status"] = combined["final_status"].astype("string").fillna(NO_STATUS_AS)
    combined = combined[GRAIN_COLS + DETAIL_COLS].drop_duplicates().reset_index(drop=True)
    log.info("ris_view_df1 — %s rows | status: %s", f"{len(combined):,}",
             ", ".join(f"{k}={v:,}" for k, v in combined["status"].value_counts().items()))
    return combined


def _add_missing_statuses(counts, group_cols):
    """Every grain gets both an 'Open' and a 'Closed' row (counts 0 when missing)."""
    keys = [c for c in group_cols if c != "status"]
    full = counts[keys].drop_duplicates().merge(pd.DataFrame({"status": STATUSES}), how="cross")
    full = full.merge(counts, on=group_cols, how="left", validate="one_to_one")
    full[COUNT_COLS] = full[COUNT_COLS].fillna(0).astype("int64")
    return full


def count_defects(defects, project_info):
    """Left-join project details on upc, then count distinct breached defect_ids per grain (incl. status).

    A defect counts for a standard when breach_flag == 1, the row is that standard's threshold
    and its affected length for that standard is > 0.
    """
    df = defects.merge(project_info, on="upc", how="left")
    group_cols = [c for c in df.columns if c not in DETAIL_COLS]

    breached = df["breach_flag"] == 1
    irc = breached & (df["threshold_name"] == "As per IRC") & (df["affected_length_irc"] > 0)
    ca = breached & (df["threshold_name"] == "As per CA") & (df["affected_length_ca"] > 0)

    counts = (
        df.assign(_irc_id=df["defect_id"].where(irc), _ca_id=df["defect_id"].where(ca))
        .groupby(group_cols, dropna=False)
        .agg(Total_Defects_IRC=("_irc_id", "nunique"), Total_Defects_CA=("_ca_id", "nunique"))
        .reset_index()
    )
    counts = _add_missing_statuses(counts, group_cols)
    log.info("ris_view_df4 — %s rows × %s cols", f"{len(counts):,}", counts.shape[1])
    return counts
