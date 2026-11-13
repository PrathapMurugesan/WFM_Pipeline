# WFM pipeline – runbook (EC2, Ubuntu)

What it does, every 15 minutes:
**Postgres** (survey status *Survey Final Approved*, not processed before) → **migrate** the upload to the stage bucket
(unzip, no videos) → **bronze** → **silver** → **images to the CDN** (`image_url`) → **gold** → results + logs to S3 →
survey marked **processed** (never picked again).

| What | Where |
|---|---|
| Source uploads | `s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<survey id>/D1..D4` |
| Migrated copy (unzipped) | `s3://dhms-new-stgae/NSV_Pipeline/wfm_reports/<survey id>/` + `_manifests/<survey id>.json` |
| Results | `s3://dhms-new-stgae/NSV/{Bronze, Silver, Gold}/zone=<z>/phase=<p>/cycle=<c>/<UPC>/` + `Silver/Latest/<table>/`, `Gold/Latest/<table>/` |
| All logs of a run | `s3://dhms-new-stgae/NSV/logs/zone=<z>/phase=<p>/cycle=<c>/<UPC>/run=<time>/` |
| Processed log | `s3://dhms-new-stgae/NSV/state/processed_surveys.csv` (+ `wfm_trigger_state.json`) |
| Code / local copy | `/opt/WFM_Pipeline` (same layout under `/opt/WFM_Pipeline/output/`) |

---

## 1. Install (once)

```bash
sudo su
apt update && apt install -y git python3-venv python3-pip unzip p7zip-full
cd /opt
git clone https://github.com/PrathapMurugesan/WFM_Pipeline.git
cd /opt/WFM_Pipeline
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## 2. Postgres credentials (once – never committed to git)

```bash
nano /opt/WFM_Pipeline/postgres_creds.txt        # paste: DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_SCHEMA / TARGET_DBS
chmod 600 /opt/WFM_Pipeline/postgres_creds.txt
```

## 3. Settings (once)

```bash
echo 'export NSV_OUTPUT_S3=s3://dhms-new-stgae/NSV' >> ~/.bashrc
source ~/.bashrc
```
Defaults that normally need no change:
`NSV_STAGE_S3=s3://dhms-new-stgae/NSV_Pipeline/wfm_reports`,
`NSV_CDN_BASE_URL=https://nhaidhms-stage-api.dic.org.in`,
`NSV_REPORTS_S3_ROOT=s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/`.

## 4. Check access (once, and after any AWS change)

```bash
cd /opt/WFM_Pipeline
aws s3 ls s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/ | head -3              # read the uploads
echo test > /tmp/t && aws s3 cp /tmp/t $NSV_OUTPUT_S3/_t && aws s3 rm $NSV_OUTPUT_S3/_t   # write the stage bucket
curl -s -o /dev/null -w "CDN %{http_code}\n" https://nhaidhms-stage-api.dic.org.in    # CDN reachable
.venv/bin/python wfm_metadata.py <any survey id>                                       # Postgres reachable
```

## 5. See what would run (safe – changes nothing)

```bash
.venv/bin/python wfm_trigger.py --dry-run
```
Shows every survey: `NEW` = would be processed now; `skip … status '…'` = not Final Approved yet;
`already_processed` in the summary = done before.

## 6. Run once by hand

```bash
.venv/bin/python wfm_trigger.py                                     # all new Final Approved surveys
.venv/bin/python wfm_trigger.py --survey <survey id>                # one survey (must be Final Approved)
.venv/bin/python wfm_trigger.py --survey <survey id> --force        # again / although not Final Approved
.venv/bin/python wfm_trigger.py --survey <survey id> --force --cdn-dry-run   # test, no CDN upload
.venv/bin/python wfm_trigger.py --reprocess --export      # ALL final-approved surveys again (new logic) + one zip each
```

## 7. Schedule the trigger (every 15 minutes)

```bash
cd /opt/WFM_Pipeline
OUTPUT_S3=s3://dhms-new-stgae/NSV PROJECT_MASTER=$NSV_PROJECT_MASTER ./deploy/install_cron.sh --daily-ist 00:30
crontab -l | grep wfm-trigger                  # check it is installed (time shown in the server's time zone)
```
- every 15 minutes instead: `./deploy/install_cron.sh 15`
- runs under `flock`: a run still busy is never started twice
- log: `/opt/WFM_Pipeline/output/logs/trigger_<yyyymmdd>.log`
- stop it: `./deploy/install_cron.sh --remove`  ·  other interval: `./deploy/install_cron.sh 30`

## 8. Check everything

```bash
cd /opt/WFM_Pipeline
.venv/bin/python wfm_status.py                         # trigger on? what was processed? last run per survey
.venv/bin/python wfm_status.py --survey <survey id>    # one survey: problems, CDN, folders (local + S3)
tail -f output/logs/trigger_$(date +%Y%m%d).log        # watch the trigger live
```

One run's folder (`output/logs/zone=…/<UPC>/run=<time>/`, same in S3):

| File | Look here for |
|---|---|
| `run_summary.json` | exit code, folders, CDN totals |
| `pipeline.log` | everything the run printed |
| `migration.json` | files copied / videos left out / damaged zip entries |
| `bronze_report.csv` | one row per file / sheet – problems in UPPER CASE |
| `bronze_d1_run.log` … `d4` | details per deliverable |
| `silver_run.log`, `silver_dq_summary.csv` | silver tables and the checks against D2 |
| `cdn_images.csv` | every image: CDN link or error |
| `metadata.json` | the survey's Postgres rows (ids: project, zone, RO, state, PIU) |
| `gold_status_run.log` | defect status from Postgres (udm_defect_reference / dlm_defect_case) |
| `gold_run.log`, `gold_ris_view_run.log` | gold KPIs (ris_trends, final_intermediate, ris_view) |

## 9. Update the code

```bash
cd /opt/WFM_Pipeline && git pull && .venv/bin/pip install -r requirements.txt
```
The cron job picks up the new code on its next run.

## Exit codes / statuses

| | |
|---|---|
| `ok` (0) | all layers fine |
| `problems` (2) | finished, but bronze found gaps in the upload or a silver check against D2 failed – counts as processed |
| `failed` (1) | a step could not run – retried on the next trigger (max 3), then left for `--survey <id> --force` |
| `not_ready` (3) | not Final Approved (only with `run_pipeline.py` directly) |

## Postgres push (DHMS tables)
Every processed survey is appended to the DHMS Postgres when the cron job has `--pg-push`
(install with `PG_PUSH=1 ... ./deploy/install_cron.sh --daily-ist 00:30`). By hand:
```bash
.venv/bin/python wfm_postgres_push.py --survey <id> [<id> ...] --null-unmatched-fk            # inspect (writes nothing)
.venv/bin/python wfm_postgres_push.py --survey <id> [<id> ...] --null-unmatched-fk --write    # append
```
- schema `public` (env `NSV_PG_TARGET_SCHEMA`), gold -> `ris_trends_kpi` / `ris_view_kpi`, `ris_view` Status `open` / `close`
- rutting + roughness: all rows; cracking / pothole / ravelling: breach rows only
- this survey's earlier rows are replaced (survey_id for gold, upc + phase + cycle for silver), all other rows untouched
- `fk_rutting_c10_parent` / `c100`: rutting children get `parent_id` NULL (the FK references `defect_id`, which only breached
  parents have); upc / lane / parent_start_chainage / parent_end_chainage keep the link
- one transaction per survey; the result is in the run's `pipeline.log` and `run_summary.json` (`pg_push`)

## S3 layout and the Latest tables

`NSV_OUTPUT_S3=s3://dhms-new-stgae/NSV` (the cron job: `OUTPUT_S3=s3://dhms-new-stgae/NSV`):

```
NSV/Bronze/zone=<z>/phase=<p>/cycle=<c>/<UPC>/d1..d4/run=<time>/          bronze, as before
NSV/Silver/zone=<z>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/dlm_kpi/     silver per survey, EVERY row
NSV/Silver/Latest/<table>/part-<survey id>.parquet                         one folder per table, all surveys
NSV/Gold/zone=<z>/phase=<p>/cycle=<c>/<UPC>/run_date=<date>/               gold per survey
NSV/Gold/Latest/<table>/part-<survey id>.parquet                           ris_trends_dw, ris_view_dw, final_intermediate
NSV/logs/   NSV/state/wfm_trigger_state.json
```

- Silver keeps ONE copy with every row (breach and non-breach); there is no `dlm_postgres/` any more
  (`NSV_BREACH_COPY=1` brings it back). The Postgres push takes `breach_flag = 1` itself (rutting + roughness: all rows).
- Latest: a new survey adds its part; a re-run of the same survey replaces its own part (never twice in a table).
  Every row has survey_id, upc, zone, phase, cycle, run_date, ingested_at. Read a folder as one table:
  `pd.read_parquet("output/latest/silver/master_cracking")`, `spark.read.parquet("s3://dhms-new-stgae/NSV/Silver/Latest/master_cracking")`.
- Add surveys already processed: `.venv/bin/python wfm_lake.py --survey <id> <id> ...` (`--no-latest` on a run skips it).
