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
      --force                                process although D1-D4 are not all approved / a lock is set
      --run-date 2026-11-10                  silver / gold folder run_date= (default: today)
      --project-master <xlsx|csv|s3://...>   gold ris_view needs it (without: ris_trends + intermediate only)
      --tracker <xlsx|s3://...>              NSV Activity Tracker for silver (optional)
      --output <folder>                      local output root (default ./output)
      --output-s3 s3://bucket/prefix         also upload every layer there (same layout)
      --from silver | --only gold            start at / run only one layer (bronze already in --output;
                                             give --report-id + --upc, or --zone/--phase/--cycle + --upc)
      --strict                               stop when bronze has problems or a silver DQ check fails

Input: settings.REPORTS_S3_ROOT of the bronze module (s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/)
       <report id>/D1..D4/<upload timestamp>-<file name>
Output (<part> = report=<report id>/<UPC>, or zone=A/phase=2/cycle=2/<UPC> when those are given):
    <output>/bronze/nsv/<part>/d1..d4/run=<time>/     bronze   (schema check + extraction)
    <output>/silver/<part>/run_date=<date>/dlm_kpi/ dlm_postgres/ dq/ run.log                silver
    <output>/gold/<part>/run_date=<date>/ris_trends_dw.csv final_intermediate.csv ris_view_dw.csv gold_run.log
    <part> from Postgres: zone=UNKNOWN/phase=<survey_phase>/cycle=<survey_cycle>/<UPC>
    <output>/logs/pipeline_<time>_<UPC>.log
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
BRONZE, SILVER, GOLD = ROOT / "bronze", ROOT / "silver", ROOT / "gold"
LAYERS = ["bronze", "silver", "gold"]
REPORT_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)
# silver writes nothing for an empty table, and gold stops on a missing file -> checked before gold
GOLD_TABLES = ["master_chainage", "master_reporting_interval", "dlm_threshold_mapping",
               "master_roughness_parent", "master_roughness_child_100m", "master_roughness_child_10m",
               "master_rutting_parent", "master_rutting_child_100m", "master_rutting_child_10m",
               "master_cracking_parent", "master_cracking_child_100m", "master_cracking"]
GOLD_VIEW_TABLES = ["master_pothole", "master_ravelling_parent"]


class Log:
    """Print and append to the pipeline log."""
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
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


def newest_bronze_batch(out):
    batches = sorted((out / "bronze" / "nsv" / "_batches").glob("batch=*.json"))
    return json.loads(batches[-1].read_text(encoding="utf-8")) if batches else None


def main(args):
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
    ap.add_argument("--output-s3")
    ap.add_argument("--from", dest="start", choices=LAYERS, default="bronze")
    ap.add_argument("--only", choices=LAYERS)
    ap.add_argument("--strict", action="store_true")
    ap.add_argument("--no-db", action="store_true", help="do not look up zone/phase/cycle/UPC in Postgres")
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
    if "bronze" in layers:
        cmd = [sys.executable, "main.py", o.source]
        if zpc:
            cmd += ["--zone", o.zone, "--phase", o.phase, "--cycle", o.cycle]
        if o.report_id:
            cmd += ["--report-id", o.report_id]
        if o.upc:
            cmd += ["--upc", o.upc]
        rc = run(log, "BRONZE (schema check + extraction)", cmd, BRONZE, env={"NSV_OUTPUT": str(out)})
        batch = newest_bronze_batch(out)
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

    # ---------------------------------------------------------------- silver
    if "silver" in layers:
        if not bronze_dir.is_dir():
            log(f"no bronze for {part} in {out / 'bronze' / 'nsv'} - stopped")
            return 1
        tracker = fetch(o.tracker, work / "tracker") if o.tracker else str(work / "tracker" / "none.xlsx")
        cmd = [sys.executable, "main.py", "--project", str(bronze_dir), "--out", str(out / "silver"),
               "--tracker", tracker, "--no-excel", "--csv"]
        rc = run(log, "SILVER", cmd, SILVER, env={"NSV_BRONZE_S3_ROOT": (o.output_s3.rstrip("/") + "/bronze/nsv")
                                                 if o.output_s3 else str(out / "bronze" / "nsv"),
                                                 "NSV_RUN_DATE": o.run_date})
        if not (silver_dir / "dlm_kpi").is_dir():
            log("SILVER wrote no tables - stopped")
            return 1
        if rc != 0:
            problems.append(f"silver: a table or DQ check failed - see silver/{dated}/run.log and dq/")
            if o.strict:
                return 2

    # ---------------------------------------------------------------- gold
    if "gold" in layers:
        kpi = silver_dir / "dlm_kpi"
        pm = fetch(o.project_master, work / "master") if o.project_master else None
        need = GOLD_TABLES + (GOLD_VIEW_TABLES if pm else [])
        missing = [t for t in need if not (kpi / f"{t}.parquet").is_file()]
        if missing:
            log(f"GOLD not run: silver tables missing {missing}")
            return 1
        gold_dir.mkdir(parents=True, exist_ok=True)
        env = {"INPUT_DIR": str(kpi), "OUTPUT_DIR": str(gold_dir), "INTERMEDIATE_PATH": str(gold_dir / "final_intermediate.csv"),
               "KEEP_UPCS": "", "FILTER_BY_PROJECT_MASTER": "true" if pm else "false"}
        cmd = [sys.executable, "run_all.py"]
        if pm:
            env.update(PROJECT_MASTER_PATH=pm, CM_PROJECT_MASTER_PATH=pm)
        else:
            cmd += ["--only", "ris_trends", "intermediate"]
            log("  no --project-master: ris_view is not run")
        if run(log, "GOLD (RIS KPI)", cmd, GOLD, env=env, out_file=gold_dir / "gold_run.log") != 0:
            log("GOLD failed")
            return 1

    # ---------------------------------------------------------------- upload
    if o.output_s3:
        log("======== UPLOAD ========")
        for layer, local in (("bronze", bronze_dir), ("silver", silver_dir), ("gold", gold_dir)):
            if layer in layers and local.is_dir():
                rel = local.relative_to(out).as_posix()
                upload(log, local, f"{o.output_s3.rstrip('/')}/{rel}")
        for batches in (out / "bronze" / "nsv" / "_batches", out / "silver" / "_batches"):
            if batches.is_dir():
                upload(log, batches, f"{o.output_s3.rstrip('/')}/{batches.relative_to(out).as_posix()}")

    log("======== DONE ========")
    for layer, local in (("bronze", bronze_dir), ("silver", silver_dir), ("gold", gold_dir)):
        if layer in layers:
            log(f"  {layer:6} {local}")
    for p in problems:
        log(f"  PROBLEM  {p}")
    return 2 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
