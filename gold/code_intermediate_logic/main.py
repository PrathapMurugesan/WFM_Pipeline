"""Affected-length intermediate pipeline (modular version of
code_notebooks/Affected length intermediate logic v3 in_use.ipynb, on the smallest-reporting-interval grain).

Reads the silver tables and the reporting intervals from INPUT_DIR and the silver defect ids / statuses from
SILVER_STATUS_DIR (output of code_silver_update_status), and writes final_intermediate.csv to OUTPUT_DIR:
one row per smallest-reporting-interval window of each upc / zone / phase / cycle, with defect flags, affected
lengths, ids and statuses. All settings live in code_intermediate_logic/.env.

    python main.py
"""
import logging

from utils.config import load_settings
from utils.defect_ids import attach_segment_ids
from utils.defect_report import SOURCE_TABLES, SRC_COLS, build_defect_report, prepare_source
from utils.interval_spine import build_interval_spine, flag_interval_spine
from utils.io_utils import load_table, save_csv
from utils.reporting_intervals import load_reporting_intervals

log = logging.getLogger("affected_length")

# id columns of final_intermediate (statuses follow in the same order)
ID_COLUMNS = ["roughness_parent_defect_id", "rutting_parent_defect_id", "cracking_defect_id"]


def run(settings):
    # --- 1. Load sources ---
    # Roughness / rutting parent and child tables are optional: a missing one is treated as empty and routing
    # falls back to the next source. Cracking uses only the child 10m table (CRACKING_TABLE)
    sources = {key: prepare_source(load_table(settings.input_dir, name, optional=True, columns=SRC_COLS))
               for key, name in SOURCE_TABLES.items()}
    sources["cracking"] = prepare_source(load_table(settings.input_dir, settings.cracking_table))
    chainage = load_table(settings.input_dir, "master_chainage")
    ri = load_reporting_intervals(settings.input_dir, settings.reporting_intervals_table, settings.default_interval)

    # --- 2. Metric per reporting-interval window, flagged against the threshold
    #        (parent / child 100m as-is, else the child measures inputs averaged per window, then the formula) ---
    defect_report = build_defect_report(sources, ri)

    # --- 3. Rows on the smallest reporting interval per upc / zone / phase / cycle -> flags, affected lengths ---
    final = flag_interval_spine(build_interval_spine(chainage, ri, settings.default_interval), defect_report)

    # --- 4. Roughness / rutting parent and cracking ids + statuses (tables from code_silver_update_status);
    #        cracking_status: Open if any cracking child in the row is Open, Closed if all are Closed ---
    final = attach_segment_ids(final, settings.silver_status_dir, settings.cracking_table, ID_COLUMNS,
                               rollup_status=["cracking_defect_id"])
    save_csv(final, settings.output_dir, "final_intermediate")
    return final


def main():
    settings = load_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    )
    run(settings)


if __name__ == "__main__":
    main()
