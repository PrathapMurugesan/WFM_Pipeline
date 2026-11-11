"""
wfm_ids.py - the WFM ids of the survey (from Postgres) into every silver table: dlm_kpi/ and dlm_postgres/,
parquet + csv. run_pipeline.py calls add_wfm_ids after gold (gold reads the silver tables unchanged).

    python wfm_ids.py <silver run_date folder> <survey id>      (details from output/metadata/<survey id>.json)

Columns (appended at the end, the same value on every row of the survey):
    survey_id      wfm_survey_schedule.id
    project_id     wfm_survey_schedule.project_id        project_name  wfm_survey_schedule.project_name
    zone_id        wfm_survey_schedule.nsv_zone_id       (zone code stays in 'zone')
    ro_id, ro_name         wfm_survey_schedule.ro_id    -> master_region_zone name
    state_id, state_name   wfm_survey_schedule.state_id -> master_state name
    piu_id, piu_name       wfm_survey_schedule.piu_id   -> master_piu name
Tables: master_* and dlm_threshold_mapping. A column that already exists is overwritten.
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
ID_COLUMNS = {"survey_id": "survey_id", "project_id": "project_id", "project_name": "project_name",
              "zone_id": "zone_id", "ro_id": "ro_id", "ro_name": "ro", "state_id": "state_id",
              "state_name": "state", "piu_id": "piu_id", "piu_name": "piu"}


def load_details(survey_id, output=ROOT / "output"):
    f = Path(output) / "metadata" / f"{survey_id}.json"
    if not f.is_file():
        return None
    return json.loads(f.read_text(encoding="utf-8")).get("details") or None


def _csv_value(v):
    return ", ".join(map(str, v)) if hasattr(v, "__len__") and not isinstance(v, (str, bytes)) else v


def add_wfm_ids(silver_dir, details, log=print):
    """Returns the number of tables updated; missing ids are logged (column left empty)."""
    silver_dir = Path(silver_dir)
    values = {col: (None if details.get(key) in (None, "") else str(details.get(key)))
              for col, key in ID_COLUMNS.items()}
    empty = [c for c, v in values.items() if v is None]
    log(f"WFM ids -> silver: " + ", ".join(f"{c}={v}" for c, v in values.items() if c.endswith("_id"))
        + (f"  | EMPTY: {empty}" if empty else ""))
    n = 0
    for sub in ("dlm_kpi", "dlm_postgres"):
        for f in sorted((silver_dir / sub).glob("*.parquet")):
            if not (f.stem.startswith("master_") or f.stem == "dlm_threshold_mapping"):
                continue
            df = pd.read_parquet(f)
            for col, v in values.items():
                df[col] = pd.Series([v] * len(df), index=df.index, dtype="object")
            df.to_parquet(f, index=False)
            csv = f.with_suffix(".csv")
            if csv.is_file():
                df.assign(**{c: df[c].map(_csv_value) for c in df.columns if df[c].dtype == object}) \
                  .to_csv(csv, index=False, encoding="utf-8-sig")
            n += 1
    log(f"  WFM ids written to {n} table file(s) in dlm_kpi/ + dlm_postgres/")
    return n


if __name__ == "__main__":
    d = load_details(sys.argv[2])
    if not d:
        sys.exit(f"no details in output/metadata/{sys.argv[2]}.json")
    add_wfm_ids(sys.argv[1], d)
