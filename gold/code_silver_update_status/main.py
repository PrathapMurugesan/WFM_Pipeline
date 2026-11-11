"""Silver status update (modular version of code_notebooks/silver_status_update.ipynb).

Fetches udm_defect_reference and dlm_defect_case from Postgres into FETCHED_DIR as CSV, adds
defect_reference_id / current_status / final_status to the silver defect tables in INPUT_DIR and
writes the updated tables to OUTPUT_DIR. All settings live in code_silver_update_status/.env.

    python main.py
"""
import logging

from utils.config import load_settings
from utils.db_fetch import fetch_db_tables
from utils.io_utils import load_silver_table, save_csv
from utils.status import STATUS_TABLES, add_status, case_status, log_closed_case_check

log = logging.getLogger("silver_status")


def filter_upcs(df, keep_upcs):
    """Keep only selected UPCs, compared on the '/' form."""
    if not keep_upcs:
        return df
    return df[df["upc"].astype("string").str.replace("_", "/").isin(keep_upcs).fillna(False).astype(bool)]


def run(settings):
    # --- 1. DB tables (fetched to FETCHED_DIR as CSV, or reused from there) ---
    db = fetch_db_tables(settings)
    udm_defect_reference, dlm_defect_case = db["udm_defect_reference"], db["dlm_defect_case"]
    cases = case_status(dlm_defect_case)
    if not settings.keep_upcs:
        log.info("KEEP_UPCS empty — all UPCs kept")

    # --- 2. Silver tables + status columns -> OUTPUT_DIR ---
    outputs = {}
    for table, defect_collection in STATUS_TABLES.items():
        silver = filter_upcs(load_silver_table(settings.input_dir, table), settings.keep_upcs)
        outputs[table] = add_status(silver, defect_collection, udm_defect_reference, cases)
        save_csv(outputs[table], settings.output_dir, table)

    # --- 3. Check: CLOSED / CANCELLED cases found in udm_defect_reference ---
    log_closed_case_check(udm_defect_reference, dlm_defect_case)
    return outputs


def main():
    settings = load_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    run(settings)


if __name__ == "__main__":
    main()
