"""defect_reference_id / current_status / final_status for the silver defect tables.

Per table (notebook cells 6-10):
  1) defect_reference_id: <table>.id = udm_defect_reference.defect_id, for that table's defect_collection
  2) current_status:      defect_reference_id = dlm_defect_case.defect_reference_id
  3) final_status:        'Closed' if current_status is CLOSED / CANCELLED, 'Open' for any other status,
                          null when there is no current_status
Both joins are left joins on keys that must be unique on the DB side, so no silver row is duplicated or lost.
"""
import logging

import pandas as pd

log = logging.getLogger(__name__)

# silver table -> udm_defect_reference.defect_collection
STATUS_TABLES = {
    "master_roughness_parent": "roughness_parent",
    "master_rutting_parent": "rutting_parent",
    "master_ravelling_parent": "ravelling_parent",
    "master_pothole": "pothole_child_ca",
    "master_cracking": "cracking_child_ca",
}

STATUS_COLUMNS = ["defect_reference_id", "current_status", "final_status"]
CLOSED_STATUSES = {"CLOSED", "CANCELLED"}


def _unique(df, key, label):
    """Keys must be unique on the DB side (the notebook asserted the joins kept the row count)."""
    dups = df[key].duplicated(keep=False)
    if dups.any():
        raise ValueError(f"{label}: {int(dups.sum())} rows share a {key} — the join would duplicate silver rows")
    return df


def reference_map(udm_defect_reference, defect_collection):
    """silver id -> defect_reference_id for one defect_collection."""
    ref = udm_defect_reference.loc[udm_defect_reference["defect_collection"] == defect_collection,
                                   ["defect_id", "id"]]
    ref = ref.dropna(subset=["defect_id"]).rename(columns={"defect_id": "id", "id": "defect_reference_id"})
    return _unique(ref, "id", f"udm_defect_reference[{defect_collection}]")


def case_status(dlm_defect_case):
    """defect_reference_id -> current_status."""
    cases = dlm_defect_case[["defect_reference_id", "current_status"]].dropna(subset=["defect_reference_id"])
    return _unique(cases, "defect_reference_id", "dlm_defect_case")


def final_status(current_status):
    status = current_status.astype("string").str.strip().str.upper()
    out = pd.Series("Open", index=current_status.index, dtype="string")
    out[status.isin(CLOSED_STATUSES).fillna(False).astype(bool)] = "Closed"
    out[current_status.isna()] = pd.NA
    return out


def add_status(silver, defect_collection, udm_defect_reference, cases):
    """silver + defect_reference_id / current_status / final_status, placed right after id."""
    ids = silver["id"].astype("string")
    ref = reference_map(udm_defect_reference, defect_collection)

    keys = pd.DataFrame({"id": ids}).merge(ref, on="id", how="left", validate="many_to_one")
    keys = keys.merge(cases, on="defect_reference_id", how="left", validate="many_to_one")
    keys["final_status"] = final_status(keys["current_status"])
    if len(keys) != len(silver):
        raise AssertionError("Join changed the row count")

    rest = [c for c in silver.columns if c not in ["id"] + STATUS_COLUMNS]
    out = pd.concat([silver[["id"]].reset_index(drop=True), keys[STATUS_COLUMNS],
                     silver[rest].reset_index(drop=True)], axis=1)

    counts = out["final_status"].value_counts(dropna=False)
    log.info("  %-28s defect_reference_id: %s / %s | final_status: %s",
             defect_collection, f"{int(out['defect_reference_id'].notna().sum()):,}", f"{len(out):,}",
             ", ".join(f"{'null' if pd.isna(k) else k}={v:,}" for k, v in counts.items()))
    return out


def log_closed_case_check(udm_defect_reference, dlm_defect_case):
    """Notebook cell 11: CLOSED / CANCELLED cases and whether their defect_reference_id is in
    udm_defect_reference.id, per defect_type / record_level / current_status."""
    status = dlm_defect_case["current_status"].astype("string").str.strip().str.upper()
    closed = dlm_defect_case[status.isin(CLOSED_STATUSES).fillna(False).astype(bool)]
    in_ref = closed["defect_reference_id"].isin(set(udm_defect_reference["id"].dropna()))

    summary = (closed.assign(in_udm_defect_reference=in_ref)
               .groupby(["defect_type", "record_level", "current_status", "in_udm_defect_reference"], dropna=False)
               .size().rename("cases").reset_index())
    log.info("CLOSED / CANCELLED cases: %s | defect_reference_id in udm_defect_reference: %s",
             f"{len(closed):,}", f"{int(in_ref.sum()):,}")
    for row in summary.itertuples(index=False):
        log.info("  %-20s | %-12s | %-10s | in ref: %-5s | %s cases", row.defect_type, row.record_level,
                 row.current_status, row.in_udm_defect_reference, f"{row.cases:,}")
