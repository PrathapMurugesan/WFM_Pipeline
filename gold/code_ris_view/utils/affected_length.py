"""Affected lengths from final_intermediate.

Roughness / Rutting / Cracking -> summed from the <defect>_<std>_length columns, per status
                                 (<defect>_status of the 10m row; empty -> NO_STATUS_AS)
Pothole / Ravelling           -> 0
Total                         -> summed from total_affected_length_<std> per UPC grain and status; a 10m
                                 row is 'Closed' only when roughness / rutting / cracking status are all
                                 Closed, else 'Open' (empty status counts as Open)

Grain: upc / source_system / detected_on / phase / cycle (pavement_type is not used: final_intermediate
takes it from master_chainage, which does not match the pavement types of the defect tables).
"""
import logging

import pandas as pd

from utils.defect_counts import COUNT_COLS, NO_STATUS_AS, STATUSES
from utils.project_master import PROJECT_COLUMNS

log = logging.getLogger(__name__)

LENGTH_COLS = ["affected_length_ca_m", "affected_length_irc_m", "affected_length_ca_km", "affected_length_irc_km"]
# defect_type -> (CA length, IRC length, status) columns of final_intermediate
DEFECT_LENGTH_COLUMNS = {
    "Roughness": ("roughness_ca_length", "roughness_irc_length", "roughness_status"),
    "Rutting": ("rutting_ca_length", "rutting_irc_length", "rutting_status"),
    "Cracking": ("cracking_ca_length", "cracking_irc_length", "cracking_status"),
}
UPC_GRAIN = ["upc", "source_system", "detected_on", "phase", "cycle"]


def _sum_lengths(intermediate, grain, ca_col, irc_col):
    out = (
        intermediate.rename(columns={"defect_detected_on": "detected_on"})
        .groupby(grain, dropna=False)
        .agg(affected_length_ca_m=(ca_col, "sum"), affected_length_irc_m=(irc_col, "sum"))
        .astype("float64")
        .reset_index()
    )
    out["affected_length_ca_km"] = out["affected_length_ca_m"] / 1000
    out["affected_length_irc_km"] = out["affected_length_irc_m"] / 1000
    return out


def _defect_lengths(intermediate, defect, ca_col, irc_col, status_col):
    """Affected lengths of one defect type per UPC grain + status (the 10m row's <defect>_status;
    empty -> NO_STATUS_AS, as in the defect counts)."""
    rows = intermediate.assign(status=intermediate[status_col].astype("string").fillna(NO_STATUS_AS))
    return _sum_lengths(rows, UPC_GRAIN + ["status"], ca_col, irc_col).assign(defect_type=defect)


def attach_defect_lengths(counts, intermediate):
    """Per-defect affected lengths joined on the UPC grain + defect_type + status.
    Pothole / Ravelling have no length columns -> 0."""
    lengths = pd.concat(
        [_defect_lengths(intermediate, defect, ca, irc, status)
         for defect, (ca, irc, status) in DEFECT_LENGTH_COLUMNS.items()],
        ignore_index=True,
    )
    log.info("aff_len_df — %s rows", f"{len(lengths):,}")

    out = counts.merge(lengths, on=UPC_GRAIN + ["defect_type", "status"], how="left", validate="one_to_one")
    out[LENGTH_COLS] = out[LENGTH_COLS].fillna(0)
    log.info("ris_view_df5 — %s rows × %s cols", f"{len(out):,}", out.shape[1])
    return out


def _total_status(intermediate):
    """Status of each 10m row's total affected length: 'Closed' only when roughness_status,
    rutting_status and cracking_status are all Closed; otherwise 'Open' (empty status -> NO_STATUS_AS)."""
    all_closed = pd.Series(True, index=intermediate.index)
    for _, _, status_col in DEFECT_LENGTH_COLUMNS.values():
        all_closed &= intermediate[status_col].astype("string").fillna(NO_STATUS_AS) == "Closed"
    return all_closed.map({True: "Closed", False: "Open"})


def _total_lengths(intermediate):
    """total_affected_length_<std> per UPC grain and status (_total_status of the 10m row);
    both statuses present for every UPC grain (0 when none)."""
    rows = intermediate.rename(columns={"defect_detected_on": "detected_on"})
    sums = (
        rows.assign(status=_total_status(rows))
        .groupby(UPC_GRAIN + ["status"], dropna=False)
        .agg(affected_length_ca_m=("total_affected_length_ca", "sum"),
             affected_length_irc_m=("total_affected_length_irc", "sum"))
        .reset_index()
    )

    out = rows[UPC_GRAIN].drop_duplicates().merge(pd.DataFrame({"status": STATUSES}), how="cross")
    out = out.merge(sums, on=UPC_GRAIN + ["status"], how="left", validate="one_to_one")
    for std in ("ca", "irc"):
        out[f"affected_length_{std}_m"] = out[f"affected_length_{std}_m"].fillna(0).astype("float64")
        out[f"affected_length_{std}_km"] = out[f"affected_length_{std}_m"] / 1000
    return out


def build_total_rows(intermediate, defect_rows):
    """'Total' rows per UPC grain and status (Open / Closed): total affected lengths split by the
    10m row status (_total_status), zone / project details taken from the defect rows, and the defect
    counts of that status summed over all defect types."""
    totals = _total_lengths(intermediate)
    totals["defect_type"] = "Total"

    enrich_cols = ["zone"] + list(PROJECT_COLUMNS)
    lookup = defect_rows[UPC_GRAIN + enrich_cols].drop_duplicates(UPC_GRAIN)
    defect_totals = defect_rows.groupby(UPC_GRAIN + ["status"], dropna=False)[COUNT_COLS].sum().reset_index()

    totals = (totals.merge(lookup, on=UPC_GRAIN, how="left")
              .merge(defect_totals, on=UPC_GRAIN + ["status"], how="left", validate="one_to_one"))
    totals[COUNT_COLS] = totals[COUNT_COLS].fillna(0).astype("int64")
    log.info("df_interm_rollup2 — %s rows", f"{len(totals):,}")
    return totals
