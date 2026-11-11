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
- **Zip files** (for example `D4/…-Pothole_Images.zip`) are opened and read as the folder they stand for (`d4/Pothole_Images/…`), in any deliverable.
  If the zip's single top folder repeats the zip's name, it is not repeated. `__MACOSX` entries are skipped. On S3, each zip is downloaded once to a temp folder.
  Each opened zip is listed in the bronze report (`zip_opened`).
- A missing required workbook (for example no CRRI_PCI) is a `WORKBOOK_MISSING` problem, but the deliverable's other workbooks are still extracted.
- The root is `REPORTS_S3_ROOT` in [`bronze/settings.py`](bronze/settings.py), or the environment variable `NSV_REPORTS_S3_ROOT`.

## How a survey gets processed
```
wfm_trigger.py  (cron, every 15 min - deploy/install_cron.sh)
  1. Postgres   wfm_survey_schedule (id = survey id = S3 folder name), current_status = 'Survey Final Approved'
  2. once only  output/state/wfm_trigger_state.json + processed_surveys.csv (also <output-s3>/state/):
                a processed survey is never picked again; a failed one is retried (max 3)
  3. source     s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<survey id>/D1..D4 must be there
  4. MIGRATE    copy to s3://dhms-new-stgae/NSV_Pipeline/wfm_reports/<survey id>/ - every zip unzipped (damaged
                zip entries recovered, CRC-checked), upload timestamps removed, videos left out
                (+ _manifests/<survey id>.json)                                               wfm_migrate.py
  5. BRONZE     reads the migrated copy -> SILVER
  6. CDN        silver master_pothole / master_cracking rows with image_name + display_defect_id ->
                POST https://nhaidhms-stage-api.dic.org.in/defects/images/upload (image, defect_type,
                display_defect_id; HTTP 201 -> data.url) -> new column image_url (dlm_kpi + dlm_postgres;
                rows without an id get it by image_name); list in silver dq/cdn_images.csv    wfm_cdn.py
  7. GOLD       RIS_KPI_Golden: silver_update (defect status from Postgres udm_defect_reference /
                dlm_defect_case; unreachable -> all Open, flagged as a problem) -> ris_trends ->
                intermediate -> ris_view (per defect type one Open and one Closed row)
  8. POSTGRES   silver dlm_postgres + gold -> the same Postgres: written, NOT active yet    wfm_postgres_push.py
  9. results + all logs -> s3://dhms-new-stgae/NSV_Pipeline/ ; survey marked processed
```
Settings (environment): `NSV_OUTPUT_S3` (results), `NSV_STAGE_S3` (migrated copy), `NSV_CDN_BASE_URL` (CDN),
`NSV_REPORTS_S3_ROOT` (source). The rule is at the top of [`wfm_metadata.py`](wfm_metadata.py) (`FINAL_STATUSES`).

## Setup on EC2 (Ubuntu, once)
```bash
apt update && apt install -y git python3-venv python3-pip
cd /opt && git clone https://github.com/PrathapMurugesan/WFM_Pipeline.git && cd WFM_Pipeline
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
nano postgres_creds.txt && chmod 600 postgres_creds.txt      # DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_SCHEMA / TARGET_DBS
.venv/bin/python wfm_trigger.py --dry-run                   # what would be processed, and why the rest is not
OUTPUT_S3=s3://dhms-new-stgae/NSV_Pipeline ./deploy/install_cron.sh 15   # every 15 min, results uploaded to S3
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

## All logs of one run, in one place
Every run, including a failed one, copies its logs into **one folder** per UPC and run (also uploaded to `<output-s3>/logs/...`):
```
output/logs/zone=B/phase=2/cycle=1/N-02001-12001-GJ/run=<time>/
    pipeline.log                       everything bronze, silver and gold printed (the whole run)
    bronze_d1_run.log .. d4            per deliverable: files found, header checks, rejections
    bronze_d1_manifest.json .. d4      every workbook / sheet / file with its status
    bronze_report.csv                  one row per file / sheet, problems in UPPER CASE
    bronze_batch.json                  bronze summary
    silver_run.log  silver_dq_summary.csv  silver_dq_mismatches.csv  silver_rule_checks.csv
    gold_status_run.log  gold_run.log  gold_ris_view_run.log
    metadata.json                      the survey's Postgres rows
    run_summary.json                   exit code, folders, which files were collected
```
The newest run of a UPC: `ls -td output/logs/*/*/*/<UPC>/run=* | head -1`. The cron trigger itself logs to `output/logs/trigger_<date>.log`.

## Output in S3 (`--output-s3`, or the environment variable `NSV_OUTPUT_S3`)
The same layout as the earlier NSV pipeline, under `s3://dhms-new-stgae/NSV_Pipeline/`:
```
bronze/nsv/zone=B/phase=2/cycle=1/<UPC>/d1..d4/run=<time>/     bronze/nsv/_batches/
silver/zone=B/phase=2/cycle=1/<UPC>/run_date=<date>/           silver/_batches/
gold/zone=B/phase=2/cycle=1/<UPC>/run_date=<date>/
logs/zone=B/phase=2/cycle=1/<UPC>/run=<time>/  (all logs of the run)     state/wfm_trigger_state.json
```

## Output (`./output`, or `--output`)
```
output/bronze/nsv/zone=<zone>/phase=<p>/cycle=<c>/<UPC>/d1..d4/run=<time>/      + bronze/nsv/_batches/batch=<time>_report.csv
output/silver/zone=<zone>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/dlm_kpi/ dlm_postgres/ dq/ run.log
output/gold/zone=<zone>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/ris_trends_dw.csv final_intermediate.csv [ris_view_dw.csv]
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
- **Gold:** the RIS_KPI_Golden code, with two small fixes for the WFM silver (marked in the code):
  chainages read as numbers of one type (`code_intermediate_logic/utils/defect_ids.py`), and the IRC child rows
  take their status from the CA parent row, as the WFM parent tables also hold IRC rows
  (`code_ris_view/utils/defect_counts.py`). `run_pipeline.py` passes the folders / Postgres settings (no gold .env
  change needed); reporting intervals from silver `master_reporting_interval`.
  Postgres schema of the status tables: env `NSV_STATUS_DB_SCHEMA` (default: DB_SCHEMA of postgres_creds.txt, else public).
- The EC2 shell wrappers (`run_*_s3.sh`) are replaced by `run_pipeline.py`.
