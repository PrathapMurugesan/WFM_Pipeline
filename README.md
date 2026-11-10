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

## Run (one report)
```bash
pip install -r requirements.txt
python run_pipeline.py 3433cb89-f583-438c-ab44-6e99455e1bf4                          # the WFM report id
python run_pipeline.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --upc N/02001/19004/DL   # if the data has several UPCs
python run_pipeline.py s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<report id>/
python run_pipeline.py "<local copy of a report>" --report-id <id>
python run_pipeline.py --from silver --report-id <id> --upc N/02001/19004/DL         # bronze already done
  options:  --project-master <xlsx|s3://>   gold ris_view (without it: ris_trends + intermediate only)
            --output-s3 s3://<bucket>/<prefix>   also upload every layer there
            --zone A --phase 2 --cycle 2     optional: file the output by zone/phase/cycle (earlier layout)
```
- **Filing.** WFM reports are stored per **report id**, not per zone / phase / cycle, so the output is filed by report:
  `report=<report id>/<UPC>`. In the silver and gold tables, zone / phase / cycle are `NA` / `0` / `0`.
  Give `--zone --phase --cycle` to file the output as before and to fill those columns.
- **Survey details from Postgres.** The S3 folder name is the survey id (`wfm_survey_schedule.id`). When `--zone/--phase/--cycle/--upc`
  are not given, `run_pipeline.py` looks them up in `wfm_survey_schedule` (+ the linked `wfm_report` rows) with `wfm_metadata.py`
  and then files the output by zone / phase / cycle as before. To see the rows and what was found: `python wfm_metadata.py <survey id>`.
  Credentials: `postgres_creds.txt` (in this folder or `silver/`, **never committed**, see `.gitignore`) or the environment variables
  `DB_HOST DB_PORT DB_USER DB_PASSWORD DB_SCHEMA TARGET_DBS`. The database is reachable only inside the VPC, so run this on the EC2 instance.
- **UPC.** From Postgres, else found in the data (the UPC columns of D1 / D4). When the data holds more than one UPC, give `--upc`.
- **S3 credentials.** The normal AWS chain (environment variables, `~/.aws`, or the EC2 instance's IAM role). They are never stored in the code.
  Reading the reports needs `s3:ListBucket` + `s3:GetObject` on `nhai-dhms-uat/wfm_reports/*`.

## Output (`./output`, or `--output`)
```
output/bronze/nsv/report=<id>/<UPC>/d1..d4/run=<time>/   + bronze/nsv/_batches/batch=<time>_report.csv
output/silver/report=<id>/<UPC>/dlm_kpi/ dlm_postgres/ dq/ run.log
output/gold/report=<id>/<UPC>/ris_trends_dw.csv final_intermediate.csv [ris_view_dw.csv] gold_run.log
output/logs/pipeline_<time>_<UPC>.log
```
| Exit code | Meaning |
|---|---|
| 0 | all OK |
| 2 | finished with problems: the upload is not fully accepted by the schema, or a silver DQ check against D2 failed |
| 1 | a layer could not run |

`--strict` stops after bronze or silver problems instead of continuing.

## Changes from the earlier pipeline
- **Bronze** is the new schema-driven ingestion. Its output is still `bronze/nsv/zone=/phase=/cycle=/<UPC>/dN/run=<time>/`, with the same table names.
- **Silver** (`silver/common/project.py`): the D2 Detailed template has two CA sheets (Flexible + Rigid). Both are read and
  stacked with a `pavement_sheet` column. The earlier code read only the first. If both D2 templates were delivered, silver uses one
  (Detailed) for the tables *and* the thresholds.
- **Silver** also files reports as `report=<id>/<UPC>` (zone / phase / cycle `NA` / `0` / `0` when not given).
- **Gold:** unchanged.
- The EC2 shell wrappers (`run_*_s3.sh`) are replaced by `run_pipeline.py`.
