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

## Run
```bash
pip install -r requirements.txt
python run_pipeline.py 57d89b35-0ceb-4bcc-b35a-fb947c0f8268 --zone A --phase 2 --cycle 2 --upc N/02001/19004/DL \
       --project-master s3://.../project_master.xlsx            # optional: gold ris_view needs it
python run_pipeline.py s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<report id>/ --zone A --phase 2 --cycle 2
python run_pipeline.py "<local copy of a report>" --zone A --phase 2 --cycle 2 --upc N/02001/19004/DL
python run_pipeline.py --from silver --zone A --phase 2 --cycle 2 --upc N/02001/19004/DL   # bronze already done
python run_pipeline.py ... --output-s3 s3://<bucket>/NSV_Pipeline                         # also upload every layer
```
- **UPC:** `--upc`. Without it, bronze takes the UPC from the folder name, or else the single UPC found in the data's UPC columns.
  The report id is not a UPC.
- **Zone / phase / cycle:** always given, because nothing in the upload states them.
- **S3 credentials:** the normal AWS chain (environment variables, `~/.aws`, or the EC2 instance's IAM role). They are never stored in the code.
  Reading the reports needs `s3:ListBucket` + `s3:GetObject` on `nhai-dhms-uat/wfm_reports/*`.

## Output (`./output`, or `--output`)
```
output/bronze/nsv/zone=A/phase=2/cycle=2/<UPC>/d1..d4/run=<time>/   + bronze/nsv/_batches/batch=<time>_report.csv
output/silver/zone=A/phase=2/cycle=2/<UPC>/dlm_kpi/ dlm_postgres/ dq/ run.log
output/gold/zone=A/phase=2/cycle=2/<UPC>/ris_trends_dw.csv final_intermediate.csv [ris_view_dw.csv] gold_run.log
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
- **Gold:** unchanged.
- The EC2 shell wrappers (`run_*_s3.sh`) are replaced by `run_pipeline.py`.
