# NSV pipeline: WFM report → bronze → silver → gold

One uploaded WFM report (= one UPC) goes through three layers. Each layer writes the same layout as the earlier NSV pipeline.

| Layer | Folder | What it does |
|---|---|---|
| Bronze | [`bronze/`](bronze/README.md) | Checks every file and part of the upload against [`schema/nsv_schema.json`](bronze/schema/nsv_schema.json) (D1–D4, NHAI instruction sheet), extracts what is valid and reports the rest |
| Silver | [`silver/`](silver/README.md) | DLM tables: rutting / roughness / ravelling / cracking at 10 m, 100 m and 1 km, potholes, chainage spine, reporting intervals, threshold mapping, DQ against D2 |
| Gold | [`gold/`](gold/README.md) | RIS KPIs: `ris_trends_dw.csv`, `final_intermediate.csv`, `ris_view_dw.csv` |

## Input: the WFM upload
```
s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<report id>/D1/1793260589326-LRMS_DataTemplate.xlsx
                                                               /D2/...  /D3/...  /D4/...
```
- `D1`..`D4` (or `Deliverable 1 - RAMS Data` …) are read as d1..d4.
- The upload timestamp in front of each file name (`1793260589326-`) is removed before any check. The name as uploaded and the
  upload time are kept in the bronze inventory (`uploaded_name`, `uploaded_at`).
- A file uploaded twice is read from its **newest** upload only. The older copy is listed as `superseded_upload`.
- The root is `REPORTS_S3_ROOT` in [`bronze/settings.py`](bronze/settings.py), or the environment variable `NSV_REPORTS_S3_ROOT`.

## How a survey gets processed
```
wfm_trigger.py  (cron, every 30 min)
  1. Postgres  wfm_survey_schedule (id = survey id = S3 folder name) + wfm_report (survey_schedule_id)
  2. ready?    D1, D2, D3, D4 reports approved (current_status APPROVED or approved_at set), is_locked false
  3. S3        s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<survey id>/D1..D4 there?
  4. changed?  report versions / status / update times + S3 files vs the last successful run
  5. run_pipeline.py  bronze -> silver -> gold   (UPC, survey_phase, survey_cycle from Postgres; zone UNKNOWN)
  6. state     output/state/wfm_trigger_state.json  (+ <output-s3>/state/)
```
The rules are at the top of [`wfm_metadata.py`](wfm_metadata.py): `REQUIRED_REPORTS`, `APPROVED_STATUSES`, and the lock checks.
A failed run is retried on the next trigger, up to 3 times per version.

## Setup on EC2 (Ubuntu, once)
```bash
apt update && apt install -y git python3-venv python3-pip
cd /opt && git clone https://github.com/PrathapMurugesan/WFM_Pipeline.git && cd WFM_Pipeline
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
nano postgres_creds.txt && chmod 600 postgres_creds.txt      # DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_SCHEMA / TARGET_DBS
.venv/bin/python wfm_trigger.py --dry-run                   # what would be processed, and why the rest is not
./deploy/install_cron.sh 30                                 # every 30 min  (OUTPUT_S3=s3://... to also upload)
```
The instance's IAM role needs to read `nhai-dhms-uat/wfm_reports/*` (and write to `OUTPUT_S3` if set).
`postgres_creds.txt` is git-ignored and never committed.

## Run by hand
```bash
.venv/bin/python wfm_metadata.py <survey id>                   # Postgres rows, pipeline values, ready or not (and why)
.venv/bin/python run_pipeline.py <survey id>                   # one survey (stops with exit 3 if not ready)
.venv/bin/python run_pipeline.py <survey id> --force           # ... even if not approved / locked
.venv/bin/python wfm_trigger.py --survey <survey id> --force   # through the trigger (state updated)
  run_pipeline options: --upc / --zone --phase --cycle (instead of Postgres), --no-db, --run-date,
                        --project-master <xlsx|s3://> (gold ris_view), --output-s3 s3://<bucket>/<prefix>, --from silver
```

## Output (`./output`, or `--output`)
```
output/bronze/nsv/zone=UNKNOWN/phase=<p>/cycle=<c>/<UPC>/d1..d4/run=<time>/      + bronze/nsv/_batches/batch=<time>_report.csv
output/silver/zone=UNKNOWN/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/dlm_kpi/ dlm_postgres/ dq/ run.log
output/gold/zone=UNKNOWN/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/ris_trends_dw.csv final_intermediate.csv [ris_view_dw.csv]
output/metadata/<survey id>.json     output/logs/     output/state/wfm_trigger_state.json
```
Without Postgres values (`--no-db` and no `--zone/--phase/--cycle`), the output is filed as `report=<survey id>/<UPC>`.

| Exit code (run_pipeline) | Meaning |
|---|---|
| 0 | all OK |
| 2 | finished with problems: the upload is not fully accepted by the schema, or a silver DQ check against D2 failed |
| 3 | not processed: the survey is not ready |
| 1 | a layer could not run |

## Changes from the earlier pipeline
- **Bronze** is the new schema-driven ingestion. Its output is still `bronze/nsv/zone=/phase=/cycle=/<UPC>/dN/run=<time>/`, with the same table names.
- **Silver** (`silver/common/project.py`): the D2 Detailed template has two CA sheets (Flexible + Rigid). Both are read and
  stacked with a `pavement_sheet` column. The earlier code read only the first. If both D2 templates were delivered, silver uses one
  (Detailed) for the tables *and* the thresholds.
- **Silver** also files reports as `report=<id>/<UPC>` (zone / phase / cycle `NA` / `0` / `0` when not given).
- **Silver / gold** are written per run date (`run_date=<date>/`), so a rerun never overwrites an earlier one.
- **Gold:** code unchanged.
- The EC2 shell wrappers (`run_*_s3.sh`) are replaced by `run_pipeline.py`.
