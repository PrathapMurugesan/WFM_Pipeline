"""
run_pipeline.py - NSV end to end for ONE uploaded WFM report (= one UPC): bronze -> silver -> gold.

    python run_pipeline.py 3433cb89-f583-438c-ab44-6e99455e1bf4                 WFM report id (UPC found in the data)
    python run_pipeline.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --upc N/02001/19004/DL   when the data has several UPCs
    python run_pipeline.py s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/3433cb89-.../
    python run_pipeline.py "<local copy of a report>" --report-id <id>
    options:
      --zone A --phase 2 --cycle 2           optional: file the output by zone/phase/cycle (earlier layout)
                                             not given -> looked up in Postgres (wfm_survey_schedule.id = report id,
                                             + wfm_report; see wfm_metadata.py); found -> the zone/phase/cycle layout
      --no-db                                do not look up zone / phase / cycle / UPC in Postgres
      --no-status-db                         gold: do not read the defect status from Postgres (all Open)
      --force                                process although D1-D4 are not all approved / a lock is set
      --run-date 2026-11-10                  silver / gold folder run_date= (default: today)
      --project-master <xlsx|csv|s3://...>   gold ris_view needs it (without: ris_trends + intermediate only)
      --tracker <xlsx|s3://...>              NSV Activity Tracker for silver (optional)
      --output <folder>                      local output root (default ./output)
      --output-s3 s3://bucket/prefix         also upload every layer there (Bronze/ Silver/ Gold/, see below);
                                             default: env NSV_OUTPUT_S3, e.g. s3://dhms-new-stgae/NSV
      --from silver | --only gold            start at / run only one layer (bronze already in --output;
                                             give --report-id + --upc, or --zone/--phase/--cycle + --upc)
      --strict                               stop when bronze has problems or a silver DQ check fails
      --no-latest                            do not add the survey to the Latest tables (wfm_lake.py)

Steps: MIGRATE (copy to --stage-s3, unzip, no videos) -> BRONZE (reads the migrated copy) -> SILVER ->
       IMAGES -> CDN (image_url = CDN link per row) -> GOLD -> [POSTGRES PUSH with --pg-push] -> LATEST -> upload + logs
Input: settings.REPORTS_S3_ROOT of the bronze module (s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/)
       <report id>/D1..D4/<upload timestamp>-<file name>
Output (<part> = report=<report id>/<UPC>, or zone=A/phase=2/cycle=2/<UPC> when those are given):
    <output>/bronze/nsv/<part>/d1..d4/run=<time>/     bronze   (schema check + extraction)
    <output>/silver/<part>/run_date=<date>/dlm_kpi/ dq/ run.log    silver, every row (breach + non-breach)
    <output>/gold/<part>/run_date=<date>/ris_trends_dw.csv final_intermediate.csv ris_view_dw.csv gold_run.log
    <part> from Postgres: zone=<master_zone.zone_code>/phase=<survey_phase>/cycle=<survey_cycle>/<UPC>
    <output>/logs/pipeline_<time>_<UPC>.log
    <output>/latest/silver|gold/<table>/part-<survey id>.parquet   every survey of a table in one folder
S3 (--output-s3 <root>):  <root>/Bronze/<part>/...   <root>/Silver/<part>/run_date=<date>/...   <root>/Silver/Latest/<table>/
                          <root>/Gold/<part>/run_date=<date>/...   <root>/Gold/Latest/<table>/   <root>/logs/  <root>/state/
S3 access (input / --output-s3 / s3:// files) uses the normal AWS credential chain (env, ~/.aws, IAM role).
Exit code: 0 = all layers ok, 1 = a layer failed, 2 = finished with problems (bronze problems / DQ FAIL),
           3 = not processed: the survey is not ready (D1-D4 not all approved, or locked) - see --force.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
import wfm_lake                                               # noqa: E402  (S3 layout + Latest tables)
from wfm_lake import S3_LAYOUT, s3_rel                       # noqa: E402
BRONZE, SILVER, GOLD = ROOT / "bronze", ROOT / "silver", ROOT / "gold"
LAYERS = ["bronze", "silver", "gold"]
REPORT_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
# silver writes nothing for an empty table, and gold stops on a missing file -> checked before gold
GOLD_TABLES = ["master_chainage", "master_reporting_interval", "dlm_threshold_mapping",
               "master_roughness_parent", "master_rutting_parent", "master_ravelling_parent",
               "master_pothole", "master_cracking"]
GOLD_VIEW_TABLES = ["master_ravelling_child_10m", "master_roughness_child_10m", "master_rutting_child_10m"]
# gold silver_update: Postgres tables with the defect case status; without Postgres they are used empty
# (every defect then counts as Open). Columns = the ones the gold code reads.
STATUS_DB_TABLES = {"udm_defect_reference": ["id", "defect_id", "defect_collection"],
                    "dlm_defect_case": ["defect_reference_id", "current_status", "defect_type", "record_level"]}


def gold_status_env(log, no_status_db):
    """Environment for gold code_silver_update_status: the DB_* settings from postgres_creds.txt (the same
    database the survey status comes from). Schema: env NSV_STATUS_DB_SCHEMA, else DB_SCHEMA, else public.
    None when there is no database (--no-status-db or no credentials). Independent of --no-db: the trigger
    passes --no-db (it already has zone / phase / cycle) but the defect status must still be read."""
    if no_status_db:
        return None
    try:
        import wfm_metadata
        c = wfm_metadata.load_creds()
    except Exception as e:
        log(f"  defect status: no Postgres credentials ({e})")
        return None
    return {"DB_HOST": c["DB_HOST"], "DB_PORT": str(c.get("DB_PORT") or 5432), "DB_USER": c["DB_USER"],
            "DB_PASSWORD": c["DB_PASSWORD"], "DB_NAME": wfm_metadata.databases(c)[0],
            "DB_SCHEMA": os.environ.get("NSV_STATUS_DB_SCHEMA") or c.get("DB_SCHEMA") or "public"}


def empty_status_tables(folder):
    """Header-only CSVs of the status tables: silver_update then reads them instead of Postgres."""
    folder.mkdir(parents=True, exist_ok=True)
    for table, cols in STATUS_DB_TABLES.items():
        (folder / f"{table}.csv").write_text(",".join(cols) + "\n", encoding="utf-8")


class Log:
    """Print and append to the pipeline log."""
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.file = open(path, "a", encoding="utf-8")

    def __call__(self, text=""):
        line = f"{datetime.now():%H:%M:%S} {text}" if text else ""
        print(line, flush=True)
        self.file.write(line + "\n")
        self.file.flush()


def run(log, title, cmd, cwd, env=None, out_file=None):
    """Run a layer as its own process (each layer has its own settings / packages). Returns the exit code."""
    log(f"======== {title} ========")
    log("  " + " ".join(f'"{c}"' if " " in str(c) else str(c) for c in cmd))
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", **(env or {})}
    extra = open(out_file, "w", encoding="utf-8") if out_file else None
    proc = subprocess.Popen([str(c) for c in cmd], cwd=cwd, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
    for line in proc.stdout:
        sys.stdout.write(line)
        log.file.write(line)
        if extra:
            extra.write(line)
    proc.wait()
    if extra:
        extra.close()
    log(f"  {title}: exit code {proc.returncode}")
    return proc.returncode


def s3_client():
    import boto3                                          # only needed when something is on S3
    return boto3.client("s3")


def fetch(path, folder):
    """A local path as it is, or an s3:// file downloaded into folder."""
    if not path or not str(path).startswith("s3://"):
        return path
    bucket, _, key = str(path)[5:].partition("/")
    target = Path(folder) / Path(key).name
    target.parent.mkdir(parents=True, exist_ok=True)
    s3_client().download_file(bucket, key, str(target))
    return str(target)


def upload(log, local, s3_url):
    """Upload a local folder to s3://bucket/prefix (same relative layout)."""
    bucket, _, prefix = s3_url[5:].partition("/")
    client, n = s3_client(), 0
    for f in sorted(p for p in Path(local).rglob("*") if p.is_file()):
        client.upload_file(str(f), bucket, f"{prefix.rstrip('/')}/{f.relative_to(local).as_posix()}")
        n += 1
    log(f"  uploaded {n} file(s) -> {s3_url}")


def upload_file(local, s3_url):
    bucket, _, key = s3_url[5:].partition("/")
    s3_client().upload_file(str(local), bucket, key)


def _latest_run(folder):
    runs = sorted(Path(folder).glob("run=*")) if Path(folder).is_dir() else []
    return runs[-1] if runs else None


def collect_logs(ctx, code):
    """
    Every log of this run in ONE folder:  <output>/logs/<part>/run=<time>/   (+ the same path in S3)
        pipeline.log                    everything printed by bronze, silver and gold (the full run)
        bronze_d1_run.log .. d4         per deliverable: files found, header checks, rejections
        bronze_d1_manifest.json .. d4   per deliverable: every workbook / sheet / file with its status
        bronze_report.csv               one row per file / sheet - problems in UPPER CASE
        bronze_batch.json               bronze summary (accepted, totals)
        silver_run.log, silver_dq_summary.csv, silver_dq_mismatches.csv, silver_rule_checks.csv
        gold_status_run.log             gold silver_update: defect status from Postgres
        gold_run.log, gold_ris_view_run.log
        metadata.json                  the survey's Postgres rows (zone / phase / cycle / UPC, approvals)
        run_summary.json                exit code, layers, folders, problems
    """
    import shutil
    log, out, o = ctx["log"], ctx["out"], ctx["o"]
    part = ctx.get("part") or f"report={o.report_id or 'unknown'}/{(o.upc or 'unknown').replace('/', '-')}"
    dest = out / "logs" / part / f"run={ctx['stamp']}"
    dest.mkdir(parents=True, exist_ok=True)
    copied = []

    def take(src, name):
        if src and Path(src).is_file():
            shutil.copy2(src, dest / name)
            copied.append(name)

    for d in ("d1", "d2", "d3", "d4"):
        run = _latest_run(ctx["bronze_dir"] / d) if ctx.get("bronze_dir") else None
        if run and (not ctx.get("batch") or run.name >= f"run={ctx['stamp']}"):   # bronze ran now: its new runs only
            take(run / f"{d}_run.log", f"bronze_{d}_run.log")
            take(run / f"{d}_manifest.json", f"bronze_{d}_manifest.json")
    batch = ctx.get("batch")
    if batch:
        b = out / "bronze" / "nsv" / "_batches" / f"batch={batch['batch_id']}"
        take(f"{b}_report.csv", "bronze_report.csv")
        take(f"{b}.json", "bronze_batch.json")
    if ctx.get("silver_dir"):
        sd = ctx["silver_dir"]
        take(sd / "run.log", "silver_run.log")
        for f in ("dq_summary.csv", "dq_mismatches.csv", "rule_checks.csv"):
            take(sd / "dq" / f, f"silver_{f}")
    if ctx.get("gold_dir"):
        for f in ("gold_status_run.log", "gold_run.log", "gold_ris_view_run.log"):
            take(ctx["gold_dir"] / f, f)
    if o.report_id:
        take(out / "metadata" / f"{o.report_id}.json", "metadata.json")
        take(out / "metadata" / f"{o.report_id}_migration.json", "migration.json")
    if ctx.get("silver_dir"):
        take(ctx["silver_dir"] / "dq" / "cdn_images.csv", "cdn_images.csv")
    summary = {"exit_code": code, "survey_id": o.report_id, "upc": o.upc, "part": part, "run": ctx["stamp"],
               "migrated_to": ctx.get("location"), "cdn": ctx.get("cdn"), "pg_push": ctx.get("pg_push"), "latest": ctx.get("latest"),
               "run_date": o.run_date, "folders": {k: str(ctx[k]) for k in ("bronze_dir", "silver_dir", "gold_dir") if ctx.get(k)},
               "files": copied}
    (dest / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log(f"all logs of this run: {dest}")
    if o.output_s3:
        s3_dest = f"{o.output_s3.rstrip('/')}/logs/{part}/run={ctx['stamp']}"
        log(f"  -> {s3_dest}")
    log.file.flush()
    shutil.copy2(log.path, dest / "pipeline.log")                 # last: complete
    if o.output_s3:
        upload(lambda *_: None, dest, s3_dest)


def newest_bronze_batch(out, since=""):
    """The bronze batch summary written by THIS run (batch id >= the run's start), else None."""
    batches = sorted(b for b in (out / "bronze" / "nsv" / "_batches").glob("batch=*.json")
                     if b.stem[len("batch="):] >= since)
    return json.loads(batches[-1].read_text(encoding="utf-8")) if batches else None


def main(args):
    """Run the layers, then ALWAYS gather every log of the run into one folder (also after a failure)."""
    ctx = {}
    code = 1
    try:
        code = _run(args, ctx)
        return code
    finally:
        if ctx.get("log"):
            try:
                collect_logs(ctx, code)
            except Exception as e:
                print(f"log collection failed: {type(e).__name__}: {e}")


def _run(args, ctx):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")    # layer logs print '→', '✖' (Windows console)
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", nargs="?", help="report id, s3:// report folder or local folder (not needed with --from silver)")
    ap.add_argument("--report-id", help="report id (local folder not named by it / --from silver)")
    ap.add_argument("--zone", help="optional, with --phase and --cycle")
    ap.add_argument("--phase")
    ap.add_argument("--cycle")
    ap.add_argument("--upc", help="N/02001/19004/DL (default: found by bronze in the folder name / the data)")
    ap.add_argument("--project-master")
    ap.add_argument("--tracker")
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--output-s3", default=os.environ.get("NSV_OUTPUT_S3"),
                    help="s3://bucket/prefix to upload every layer to (default: env NSV_OUTPUT_S3)")
    ap.add_argument("--from", dest="start", choices=LAYERS, default="bronze")
    ap.add_argument("--only", choices=LAYERS)
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--no-db", action="store_true", help="do not look up zone/phase/cycle/UPC in Postgres")
    ap.add_argument("--no-status-db", action="store_true", help="gold: no defect status from Postgres (all Open)")
    ap.add_argument("--pg-push", action="store_true", default=os.environ.get("NSV_PG_PUSH", "") == "1",
                    help="append silver + gold to the DHMS Postgres after gold (wfm_postgres_push.py; env NSV_PG_PUSH=1)")
    ap.add_argument("--no-latest", action="store_true",
                    help="do not add this survey to the Latest tables (Silver/Latest, Gold/Latest)")
    ap.add_argument("--stage-s3", default=os.environ.get("NSV_STAGE_S3", "s3://dhms-new-stgae/NSV_Pipeline/wfm_reports"),
                    help="where the survey is migrated to (unzipped, no videos) before bronze")
    ap.add_argument("--no-migrate", action="store_true", help="bronze reads the source directly (no copy / unzip)")
    ap.add_argument("--no-cdn", action="store_true", help="do not push the defect images to the CDN")
    ap.add_argument("--cdn-url", default=os.environ.get("NSV_CDN_BASE_URL", "https://nhaidhms-stage-api.dic.org.in"))
    ap.add_argument("--cdn-dry-run", action="store_true", help="match the images, upload nothing")
    ap.add_argument("--force", action="store_true", help="process even when the reports are not approved / locked")
    ap.add_argument("--run-date", default=datetime.now().strftime("%Y-%m-%d"),
                    help="silver / gold folder run_date=<date> (default: today)")
    o = ap.parse_args(args)
    layers = [o.only] if o.only else LAYERS[LAYERS.index(o.start):]
    if "bronze" in layers and not o.source:
        ap.error("the report (id / s3:// / folder) is needed to run bronze")
    zpc = [x for x in (o.zone, o.phase, o.cycle) if x]
    if zpc and len(zpc) != 3:
        ap.error("--zone, --phase and --cycle go together (or leave all three out)")
    if "bronze" not in layers and not (o.report_id or (o.upc and zpc)):
        ap.error("without bronze: give --report-id (or --upc with --zone/--phase/--cycle)")

    out = Path(o.output).resolve()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    upc = o.upc.strip().replace("/", "-").replace("_", "-").upper() if o.upc else None
    log = Log(out / "logs" / f"pipeline_{stamp}_{upc or 'upc'}.log")
    ctx.update(log=log, out=out, o=o, stamp=stamp)
    log(f"NSV pipeline: {o.source or '(bronze not run)'}  "
        + (f"zone {o.zone} phase {o.phase} cycle {o.cycle}  " if zpc else f"report {o.report_id or '(from source)'}  ")
        + f"UPC {upc or '(from bronze / data)'}  layers {layers}  output {out}")
    work = out / "_work"
    problems = []

    # ---------------------------------------------------------------- survey details from Postgres
    survey_id = o.report_id or (Path(str(o.source).rstrip("/")).name if o.source else None)
    if not o.no_db and survey_id and REPORT_ID.match(survey_id) and not (zpc and o.upc):
        try:
            import wfm_metadata
            meta = wfm_metadata.lookup(survey_id)
        except Exception as e:
            meta = None
            log(f"POSTGRES lookup not used ({type(e).__name__}: {e}) - continuing with what was given")
        if meta is not None:
            d = meta["details"]
            log(f"POSTGRES: {len(meta['schedule'])} wfm_survey_schedule row(s), {len(meta['reports'])} wfm_report row(s) "
                f"for {survey_id}: UPC {d.get('upc')} zone {d.get('zone')} phase {d.get('phase')} cycle {d.get('cycle')}")
            if not meta["eligible"]:
                log("  NOT ready to process: " + "; ".join(meta["not_eligible_because"]))
                if not o.force:
                    log("  stopped (--force processes it anyway)")
                    return 3
                log("  --force: processed anyway")
            if not zpc and d.get("phase") and d.get("cycle"):
                o.zone, o.phase, o.cycle = d["zone"], d["phase"], d["cycle"]
                zpc = [o.zone, o.phase, o.cycle]
            if not o.upc and d.get("upc"):
                o.upc = d["upc"]
                upc = o.upc.replace("/", "-").upper()
            o.report_id = o.report_id or survey_id
            meta_file = out / "metadata" / f"{survey_id}.json"
            meta_file.parent.mkdir(parents=True, exist_ok=True)
            meta_file.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    if "bronze" not in layers and not upc:
        log("no UPC (give --upc, or let the Postgres lookup find it) - stopped")
        return 1

    # ---------------------------------------------------------------- bronze
    location = o.source
    if "bronze" in layers and not o.no_migrate:
        survey_id = o.report_id or Path(str(o.source).rstrip("/")).name
        log("======== MIGRATE (copy to stage, unzip, no videos) ========")
        try:
            import wfm_migrate
            manifest = wfm_migrate.migrate(o.source, survey_id, o.stage_s3, log=log)
        except Exception as e:
            log(f"MIGRATE failed: {type(e).__name__}: {e} - stopped")
            return 1
        location = manifest["location"]
        o.report_id = o.report_id or survey_id
        ctx["migration"] = manifest
        mfile = out / "metadata" / f"{survey_id}_migration.json"
        mfile.parent.mkdir(parents=True, exist_ok=True)
        mfile.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        if manifest["unreadable_count"]:
            problems.append(f"migrate: {manifest['unreadable_count']} file(s) could not be copied (damaged) - "
                            f"see metadata/{mfile.name}")
    ctx["location"] = location

    if "bronze" in layers:
        cmd = [sys.executable, "main.py", location]
        if zpc:
            cmd += ["--zone", o.zone, "--phase", o.phase, "--cycle", o.cycle]
        if o.report_id:
            cmd += ["--report-id", o.report_id]
        if o.upc:
            cmd += ["--upc", o.upc]
        rc = run(log, "BRONZE (schema check + extraction)", cmd, BRONZE, env={"NSV_OUTPUT": str(out)})
        batch = newest_bronze_batch(out, since=stamp)
        ctx["batch"] = batch
        if rc == 1 or not batch:
            log("BRONZE could not run - stopped")
            return 1
        upc = batch["survey"]["upc_folder"]
        part = batch["survey"]["part"]
        if rc == 2:
            problems.append(f"bronze: {batch['problems']} problem(s) - see bronze/nsv/_batches/batch={batch['batch_id']}_report.csv")
            log(f"  BRONZE: upload NOT accepted ({batch['problems']} problem(s))")
            if o.strict:
                return 2

    if "bronze" not in layers:
        part = (f"zone={o.zone.upper()}/phase={o.phase}/cycle={o.cycle}/{upc}" if zpc
                else f"report={o.report_id}/{upc}")
    # silver + gold: one folder per run date (a later run of the same survey never overwrites an earlier one)
    dated = f"{part}/run_date={o.run_date}"
    bronze_dir, silver_dir, gold_dir = out / "bronze" / "nsv" / part, out / "silver" / dated, out / "gold" / dated
    ctx.update(part=part, bronze_dir=bronze_dir, silver_dir=silver_dir, gold_dir=gold_dir)

    # ---------------------------------------------------------------- silver
    if "silver" in layers:
        if not bronze_dir.is_dir():
            log(f"no bronze for {part} in {out / 'bronze' / 'nsv'} - stopped")
            return 1
        tracker = fetch(o.tracker, work / "tracker") if o.tracker else str(work / "tracker" / "none.xlsx")
        cmd = [sys.executable, "main.py", "--project", str(bronze_dir), "--out", str(out / "silver"),
               "--tracker", tracker, "--no-excel", "--csv"]
        rc = run(log, "SILVER", cmd, SILVER, env={"NSV_BRONZE_S3_ROOT": (o.output_s3.rstrip("/") + "/" + S3_LAYOUT["bronze/nsv"])
                                                 if o.output_s3 else str(out / "bronze" / "nsv"),
                                                 "NSV_RUN_DATE": o.run_date})
        import shutil
        shutil.rmtree(silver_dir / "dlm_postgres", ignore_errors=True)   # breach-only copy of an earlier run
        if not (silver_dir / "dlm_kpi").is_dir():
            log("SILVER wrote no tables - stopped")
            return 1
        if rc != 0:
            problems.append(f"silver: a table or DQ check failed - see silver/{dated}/run.log and dq/")
            if o.strict:
                return 2

    # ---------------------------------------------------------------- images -> CDN (after silver)
    if "silver" in layers and not o.no_cdn:
        cdn_source = location if "bronze" in layers else (
            f"{o.stage_s3.rstrip('/')}/{o.report_id}" if o.report_id and not o.no_migrate else o.source)
        log("======== IMAGES -> CDN ========")
        try:
            import wfm_cdn
            cdn = wfm_cdn.push_images(silver_dir, cdn_source, dry_run=o.cdn_dry_run, base_url=o.cdn_url.rstrip("/"), log=log)
            ctx["cdn"] = cdn
            if cdn["failed"] or cdn["not_found"]:
                problems.append(f"cdn: {cdn['failed']} upload(s) failed, {cdn['not_found']} image(s) not found - "
                                f"see silver/{dated}/dq/cdn_images.csv")
        except Exception as e:
            problems.append(f"cdn: {type(e).__name__}: {e}")
            log(f"CDN step failed: {type(e).__name__}: {e} (silver tables keep their image paths)")

    # ---------------------------------------------------------------- gold
    if "gold" in layers:
        kpi = silver_dir / "dlm_kpi"
        pm = fetch(o.project_master, work / "master") if o.project_master else None
        pm = str(Path(pm).resolve()) if pm else None              # gold runs from its own folder
        need = GOLD_TABLES + (GOLD_VIEW_TABLES if pm else [])
        missing = [t for t in need if not (kpi / f"{t}.parquet").is_file()]
        if missing:
            log(f"GOLD not run: silver tables missing {missing}")
            return 1
        gold_dir.mkdir(parents=True, exist_ok=True)
        status_dir, fetched_dir = work / "silver_data_with_status", work / "fetched_db_tables"
        import shutil
        for d in (status_dir, fetched_dir):                       # never the tables of an earlier survey
            shutil.rmtree(d, ignore_errors=True)
        env = {"INPUT_DIR": str(kpi), "OUTPUT_DIR": str(gold_dir), "INTERMEDIATE_PATH": str(gold_dir / "final_intermediate.csv"),
               "KEEP_UPCS": "", "FILTER_BY_PROJECT_MASTER": "true" if pm else "false",
               "SILVER_STATUS_DIR": str(status_dir), "CHILD_DIR": str(kpi),
               "REPORTING_INTERVALS_TABLE": "master_reporting_interval", "CRACKING_TABLE": "master_cracking"}
        gold_log = gold_dir / "gold_run.log"

        # 1. silver_update: silver tables + defect_reference_id / current_status / final_status (from Postgres)
        db = gold_status_env(log, o.no_status_db)
        status_env = {**env, "OUTPUT_DIR": str(status_dir), "FETCHED_DIR": str(fetched_dir)}
        rc = 1
        if db:
            rc = run(log, "GOLD silver_update (defect status from Postgres)", [sys.executable, "run_all.py", "--only", "silver_update"],
                     GOLD, env={**status_env, **db, "REFRESH_DB_TABLES": "true"}, out_file=gold_dir / "gold_status_run.log")
        if rc != 0:
            log("  defect status: Postgres not used - every defect counts as Open (no status)")
            problems.append("gold: defect status not read from Postgres (udm_defect_reference / dlm_defect_case) - "
                            "all defects counted as Open, see gold_status_run.log")
            empty_status_tables(fetched_dir)
            if run(log, "GOLD silver_update (no status)", [sys.executable, "run_all.py", "--only", "silver_update"], GOLD,
                   env={**status_env, "REFRESH_DB_TABLES": "false"},
                   out_file=None if db else gold_dir / "gold_status_run.log") != 0:   # keep the Postgres error log
                log("GOLD failed (silver_update)")
                return 1

        # 2. ris_trends + intermediate (silver tables; intermediate takes ids / statuses from SILVER_STATUS_DIR)
        if pm:
            env.update(PROJECT_MASTER_PATH=pm, CM_PROJECT_MASTER_PATH=pm)
        if run(log, "GOLD ris_trends + intermediate", [sys.executable, "run_all.py", "--only", "ris_trends", "intermediate"],
               GOLD, env=env, out_file=gold_log) != 0:
            log("GOLD failed")
            return 1
        # 3. ris_view: defect tables WITH status (INPUT_DIR = silver_data_with_status), child 10m from silver
        if pm:
            if run(log, "GOLD ris_view", [sys.executable, "run_all.py", "--only", "ris_view"], GOLD,
                   env={**env, "INPUT_DIR": str(status_dir)}, out_file=gold_dir / "gold_ris_view_run.log") != 0:
                log("GOLD failed (ris_view)")
                return 1
        else:
            log("  no --project-master: ris_view is not run")

    # ---------------------------------------------------------------- WFM ids into silver (after gold)
    if "silver" in layers and o.report_id and silver_dir.is_dir():
        import wfm_ids
        details = wfm_ids.load_details(o.report_id, out)
        if details:
            try:
                wfm_ids.add_wfm_ids(silver_dir, details, log=log)
            except Exception as e:
                problems.append(f"wfm ids: {type(e).__name__}: {e}")
        else:
            problems.append(f"wfm ids: no metadata/{o.report_id}.json - survey_id / project_id / piu_id ... not in silver")

    # ---------------------------------------------------------------- POSTGRES PUSH (--pg-push / NSV_PG_PUSH=1)
    # Appends this survey's silver + gold to the EXISTING DHMS tables (wfm_postgres_push.py): schema
    # NSV_PG_TARGET_SCHEMA (public), gold -> ris_trends_kpi / ris_view_kpi, rutting + roughness all rows, other
    # defects breach rows, this survey's earlier rows replaced, rutting child parent_id NULL where the FK rejects it.
    # One transaction: a failure writes nothing and is a problem of the run (the run itself still finishes).
    if o.pg_push and "gold" in layers and o.report_id:
        log("======== POSTGRES PUSH ========")
        try:
            import wfm_postgres_push
            done = wfm_postgres_push.push_survey(
                o.report_id, out, wfm_postgres_push.TARGET_SCHEMA, {}, True, True, log=log, null_unmatched=True,
                folders=(silver_dir, gold_dir, {"upc": o.upc or upc.replace("-", "/"), "zone": o.zone,
                                                "phase": o.phase, "cycle": o.cycle, "status": "this run"}))
            ctx["pg_push"] = {t: {"deleted": d, "inserted": n} for t, (d, n) in done.items()}
        except Exception as e:
            ctx["pg_push"] = {"error": f"{type(e).__name__}: {e}"}
            problems.append(f"postgres push: {type(e).__name__}: {e} - nothing of this survey written")

    # ---------------------------------------------------------------- LATEST (wfm_lake.py)
    # every silver / gold table as one folder with every survey: latest/<layer>/<table>/part-<survey id>.parquet
    # -> S3 Silver/Latest/<table>/, Gold/Latest/<table>/ (a re-run replaces this survey's own part)
    if not o.no_latest and o.report_id and ("silver" in layers or "gold" in layers) and silver_dir.is_dir():
        log("======== LATEST ========")
        try:
            ctx["latest"] = wfm_lake.write_latest(
                silver_dir, gold_dir, {"survey_id": o.report_id, "upc": o.upc or upc.replace("-", "/"), "zone": o.zone,
                                       "phase": o.phase, "cycle": o.cycle, "run_date": o.run_date},
                out, o.output_s3, log=log)
        except Exception as e:
            ctx["latest"] = {"error": f"{type(e).__name__}: {e}"}
            problems.append(f"latest: {type(e).__name__}: {e}")

    # ---------------------------------------------------------------- upload
    if o.output_s3:
        log("======== UPLOAD ========")
        for layer, local in (("bronze", bronze_dir), ("silver", silver_dir), ("gold", gold_dir)):
            if layer in layers and local.is_dir():
                upload(log, local, f"{o.output_s3.rstrip('/')}/{s3_rel(local.relative_to(out))}")
        for batches in (out / "bronze" / "nsv" / "_batches", out / "silver" / "_batches"):
            if batches.is_dir():
                upload(log, batches, f"{o.output_s3.rstrip('/')}/{s3_rel(batches.relative_to(out))}")

    log("======== DONE ========")
    for layer, local in (("bronze", bronze_dir), ("silver", silver_dir), ("gold", gold_dir)):
        if layer in layers:
            log(f"  {layer:6} {local}")
    for p in problems:
        log(f"  PROBLEM  {p}")
    return 2 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
