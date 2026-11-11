"""
wfm_status.py - one overview of the WFM pipeline on this machine: is the trigger on, what was processed, how
did the last run of each survey go, and where its files are.

    python wfm_status.py                      everything
    python wfm_status.py --survey <id>        one survey in detail (problems, CDN, folders)
"""
import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def line(title):
    print(f"\n==================== {title} ====================")


def cron_status():
    try:
        out = subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout
    except FileNotFoundError:
        return "crontab not available on this machine"
    job = [l for l in out.splitlines() if "wfm-trigger" in l]
    return job[0] if job else "NOT installed  ->  ./deploy/install_cron.sh 15"


def last_trigger_log(out, n=8):
    logs = sorted((out / "logs").glob("trigger_*.log"))
    if not logs:
        return ["(no trigger log yet - the cron job has not run)"]
    return [f"{logs[-1]}:"] + logs[-1].read_text(encoding="utf-8", errors="replace").splitlines()[-n:]


def latest_run_folder(out, upc_folder):
    runs = sorted(out.glob(f"logs/*/*/*/{upc_folder}/run=*")) + sorted(out.glob(f"logs/report=*/{upc_folder}/run=*"))
    return max(runs, key=lambda p: p.name) if runs else None


def survey_detail(out, sid, entry, s3):
    upc = (entry.get("upc") or "").replace("/", "-")
    print(f"\n{sid}  {entry.get('status', '?').upper()}  UPC {entry.get('upc')}  zone {entry.get('zone')}  "
          f"phase {entry.get('phase')}  cycle {entry.get('cycle')}  run {entry.get('run_date')}  "
          f"({entry.get('minutes')} min, exit {entry.get('exit_code')})")
    run = latest_run_folder(out, upc) if upc else None
    if not run:
        print("  no log folder found")
        return
    summary = json.loads((run / "run_summary.json").read_text()) if (run / "run_summary.json").is_file() else {}
    print(f"  logs        {run}")
    if s3:
        print(f"              {s3.rstrip('/')}/{run.relative_to(out).as_posix()}")
    if summary.get("migrated_to"):
        print(f"  migrated    {summary['migrated_to']}")
    for k, v in summary.get("folders", {}).items():
        print(f"  {k.replace('_dir', ''):11} {v}")
    report = run / "bronze_report.csv"
    if report.is_file():
        rows = list(csv.DictReader(open(report, encoding="utf-8-sig")))
        problems = [r for r in rows if r.get("status", "").isupper()]
        print(f"  bronze      {len(problems)} problem(s)")
        for r in problems[:12]:
            what = " / ".join(x for x in (r.get("workbook"), r.get("file"), r.get("sheet")) if x)
            print(f"              {r['status']:18} {r['deliverable']:7} {what[:70]}")
    cdn = run / "cdn_images.csv"
    if cdn.is_file():
        rows = list(csv.DictReader(open(cdn, encoding="utf-8-sig")))
        counts = {}
        for r in rows:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        print(f"  cdn         {counts or 'no images to upload'}")
        for r in [r for r in rows if r["status"] == "upload_failed"][:3]:
            print(f"              FAIL {r['display_defect_id']}: {r['error'][:100]}")
    silver_log = run / "silver_run.log"
    if silver_log.is_file():
        tail = [l for l in silver_log.read_text(encoding="utf-8", errors="replace").splitlines() if "checks pass" in l or "FAILED" in l]
        if tail:
            print(f"  silver      {tail[-1].strip()[:110]}")


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey")
    ap.add_argument("--output", default=str(ROOT / "output"))
    o = ap.parse_args(args)
    out = Path(o.output)
    s3 = os.environ.get("NSV_OUTPUT_S3")

    line("trigger (cron)")
    print(cron_status())
    for l in last_trigger_log(out):
        print("  " + l)

    state_file = out / "state" / "wfm_trigger_state.json"
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.is_file() else {}
    line(f"processed log ({len(state)} survey(s))  {state_file}" + (f"  +  {s3.rstrip('/')}/state/" if s3 else ""))
    counts = {}
    for v in state.values():
        counts[v.get("status")] = counts.get(v.get("status"), 0) + 1
    print(f"by status: {counts or '-'}")

    line("surveys")
    items = [(o.survey, state.get(o.survey, {}))] if o.survey else \
        sorted(state.items(), key=lambda kv: str(kv[1].get("started_at") or ""), reverse=True)
    if not items:
        print("nothing processed yet")
    for sid, entry in items[:20]:
        survey_detail(out, sid, entry, s3)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
