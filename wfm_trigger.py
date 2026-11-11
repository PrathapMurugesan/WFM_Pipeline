"""
wfm_trigger.py - process every WFM survey that is 'Survey Final Approved' and not processed yet. Run by cron.

    python wfm_trigger.py                      process every new final-approved survey
    python wfm_trigger.py --dry-run            only show what would be processed (and why the others are not)
    python wfm_trigger.py --survey <id>        only this survey (still checked against the rules)
    python wfm_trigger.py --survey <id> --force   process it again / although not final-approved
    python wfm_trigger.py --reprocess          every final-approved survey AGAIN (new logic), processed log ignored
    python wfm_trigger.py ... --export         after each survey: one zip (bronze, silver, gold, logs, no images)
                                               in output/exports/ (tools/export_surveys.py)
    passed on to run_pipeline.py: --output, --output-s3, --project-master, --tracker, --no-cdn, --cdn-dry-run

Each run:
  1. Postgres: every NSV survey (wfm_survey_schedule) + its wfm_report rows                (wfm_metadata.list_surveys)
  2. picked: current_status 'Survey Final Approved'                                        (wfm_metadata.eligibility)
  3. processed log: a survey that was processed (ok / problems) is NEVER processed again; a failed one is
     retried on the next run, up to MAX_ATTEMPTS times
  4. S3: the upload must be there - nhai-dhms-uat/.../reports/<survey id>/ with D1..D4
  5. run_pipeline.py: MIGRATE to dhms-new-stgae (unzip, no videos) -> BRONZE -> SILVER -> images to the CDN ->
     GOLD -> [Postgres push: not active yet] -> results + logs to <output-s3>
  6. processed log: output/state/wfm_trigger_state.json + processed_surveys.csv, also in <output-s3>/state/
Exit code: 0 = nothing failed, 1 = at least one survey failed (or the trigger could not run).
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "bronze"))
MAX_ATTEMPTS = 3                     # failed runs of a survey before it is left alone (rerun: --survey <id> --force)
DONE = ("ok", "problems")            # processed - never picked again
PIPELINE_STATUS = {0: "ok", 2: "problems", 3: "not_ready", 1: "failed"}


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(text):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S} {text}", flush=True)


# =============================================================================
# State (local file, mirrored to S3)
# =============================================================================
class State:
    def __init__(self, out, output_s3=None):
        self.path = out / "state" / "wfm_trigger_state.json"
        self.s3 = f"{output_s3.rstrip('/')}/state/wfm_trigger_state.json" if output_s3 else None
        self.data = {}
        if self.s3:
            try:
                bucket, _, key = self.s3[5:].partition("/")
                self.data = json.loads(_s3().get_object(Bucket=bucket, Key=key)["Body"].read())
            except Exception:
                self.data = {}
        if not self.data and self.path.is_file():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))

    def get(self, survey_id):
        return self.data.get(survey_id, {})

    def put(self, survey_id, **values):
        self.data[survey_id] = {**self.data.get(survey_id, {}), **values}
        self.save()

    CSV_COLUMNS = ["survey_id", "status", "upc", "zone", "phase", "cycle", "project_name", "ro", "state", "piu",
                   "started_at", "finished_at", "minutes", "exit_code", "attempts", "run_date"]

    def csv_text(self):
        import csv
        import io
        buf = io.StringIO()
        w = csv.DictWriter(buf, fieldnames=self.CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for sid, v in sorted(self.data.items(), key=lambda kv: str(kv[1].get("started_at") or "")):
            w.writerow({"survey_id": sid, **v})
        return buf.getvalue()

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)
        (self.path.parent / "processed_surveys.csv").write_text(self.csv_text(), encoding="utf-8")
        if self.s3:
            bucket, _, key = self.s3[5:].partition("/")
            _s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(self.data, indent=2).encode("utf-8"))
            _s3().put_object(Bucket=bucket, Key=key.rsplit("/", 1)[0] + "/processed_surveys.csv",
                             Body=self.csv_text().encode("utf-8"))


def _s3():
    import boto3
    return boto3.client("s3")


# =============================================================================
# S3: is the upload there, and what does it look like now?
# =============================================================================
def s3_upload(survey_id):
    """{'location', 'deliverables': ['d1', ...], 'files', 'newest'} of reports/<survey id>/ (None if nothing there)."""
    import settings                                   # bronze/settings.py: REPORTS_S3_ROOT
    from nsv.storage import DELIVERABLE_FOLDER
    location = settings.REPORTS_S3_ROOT.rstrip("/") + f"/{survey_id}/"
    bucket, _, prefix = location[5:].partition("/")
    files, newest, found = 0, None, set()
    for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            files += 1
            stamp = obj["LastModified"].isoformat()
            newest = max(newest or stamp, stamp)
            top = obj["Key"][len(prefix):].split("/")[0]
            m = DELIVERABLE_FOLDER.match(top)
            if m:
                found.add(f"d{m.group(1)}")
    return {"location": location, "deliverables": sorted(found), "files": files, "newest": newest} if files else None


# =============================================================================
# One trigger run
# =============================================================================
def run_survey(survey, details, o, out):
    """run_pipeline.py for one survey; returns its exit code."""
    cmd = [sys.executable, str(ROOT / "run_pipeline.py"), details["survey_id"], "--report-id", details["survey_id"],
           "--zone", details["zone"], "--phase", details["phase"], "--cycle", details["cycle"],
           "--upc", details["upc"], "--no-db", "--output", str(out)]
    for flag in ("output_s3", "project_master", "tracker"):
        if getattr(o, flag):
            cmd += [f"--{flag.replace('_', '-')}", getattr(o, flag)]
    cmd += [f for f, on in (("--no-cdn", o.no_cdn), ("--cdn-dry-run", o.cdn_dry_run)) if on]
    log(f"  RUN  {' '.join(cmd[1:])}")
    return subprocess.call(cmd, cwd=ROOT, env={**os.environ, "PYTHONIOENCODING": "utf-8"})


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey", help="only this survey id")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="with --survey: process again / although not final-approved")
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--output-s3", default=os.environ.get("NSV_OUTPUT_S3"),
                    help="s3://bucket/prefix (default: env NSV_OUTPUT_S3); state is kept there too")
    ap.add_argument("--reprocess", action="store_true",
                    help="process every final-approved survey again, also those already processed")
    ap.add_argument("--export", action="store_true", help="zip each processed survey into output/exports/")
    ap.add_argument("--project-master", default=os.environ.get("NSV_PROJECT_MASTER"),
                    help="gold ris_view (default: env NSV_PROJECT_MASTER)")
    ap.add_argument("--tracker")
    ap.add_argument("--no-cdn", action="store_true")
    ap.add_argument("--cdn-dry-run", action="store_true")
    o = ap.parse_args(args)
    if o.force and not o.survey:
        ap.error("--force only together with --survey")
    out = Path(o.output).resolve()

    import wfm_metadata
    log(f"WFM trigger: {'dry run' if o.dry_run else 'processing'}  output {out}" + (f"  s3 {o.output_s3}" if o.output_s3 else ""))
    try:
        surveys = wfm_metadata.list_surveys()
    except Exception as e:
        log(f"cannot read the surveys from Postgres: {type(e).__name__}: {e}")
        return 1
    if o.survey:
        surveys = [s for s in surveys if str(s["schedule"].get("id")) == o.survey]
        if not surveys:
            log(f"survey {o.survey} not found in wfm_survey_schedule")
            return 1
    state = State(out, o.output_s3)
    log(f"{len(surveys)} survey(s) in Postgres")

    counts, failed = {}, 0
    for item in surveys:
        sched, reports = item["schedule"], item["reports"]
        sid = str(sched.get("id"))
        ok, why = wfm_metadata.eligibility(sched, reports)
        if not ok and not o.force:
            counts["not_ready"] = counts.get("not_ready", 0) + 1
            if o.survey or o.dry_run:
                log(f"  skip {sid}: not ready - {'; '.join(why)}")
            continue
        last = state.get(sid)
        if not o.force and not o.reprocess and last.get("status") in DONE:
            counts["already_processed"] = counts.get("already_processed", 0) + 1
            continue
        if not o.force and last.get("status") == "failed" and last.get("attempts", 0) >= MAX_ATTEMPTS:
            counts["gave_up"] = counts.get("gave_up", 0) + 1
            log(f"  skip {sid}: failed {last['attempts']} times - fix and rerun with --survey {sid} --force")
            continue
        details = wfm_metadata.survey_details(sched)
        missing = [k for k in ("upc", "phase", "cycle") if not details.get(k)]
        if missing:
            counts["no_details"] = counts.get("no_details", 0) + 1
            log(f"  skip {sid}: {missing} empty in wfm_survey_schedule")
            continue
        try:
            upload = s3_upload(sid)
        except Exception as e:
            log(f"  skip {sid}: cannot list S3 ({type(e).__name__}: {e})")
            counts["s3_error"] = counts.get("s3_error", 0) + 1
            continue
        if not upload:
            counts["not_in_s3"] = counts.get("not_in_s3", 0) + 1
            log(f"  skip {sid}: final approved, but nothing in S3 yet")
            continue
        fp = wfm_metadata.fingerprint(sched, reports) + f"|s3:{upload['files']}:{upload['newest']}"
        reason = "forced" if o.force else "again" if o.reprocess and last.get("status") in DONE else (
            "retry" if last.get("status") == "failed" else "new")
        log(f"  {reason.upper():8} {sid}  UPC {details['upc']}  zone {details['zone']}  phase {details['phase']}  cycle {details['cycle']}  "
            f"S3 {upload['files']} file(s) {upload['deliverables']}")
        if o.dry_run:
            counts["would_run"] = counts.get("would_run", 0) + 1
            continue
        attempts = (last.get("attempts", 0) + 1) if last.get("status") == "failed" and not (o.force or o.reprocess) else 1
        state.put(sid, status="running", started_at=now(), fingerprint=fp, attempts=attempts, upc=details["upc"])
        started = datetime.now()
        code = run_survey(item, details, o, out)
        status = PIPELINE_STATUS.get(code, "failed")
        state.put(sid, status=status, exit_code=code, finished_at=now(), minutes=round((datetime.now() - started).seconds / 60, 1),
                  run_date=started.strftime("%Y-%m-%d"), zone=details["zone"], phase=details["phase"],
                  cycle=details["cycle"], ro=details.get("ro"), state=details.get("state"), piu=details.get("piu"),
                  project_name=details.get("project_name"))
        counts[status] = counts.get(status, 0) + 1
        failed += status == "failed"
        log(f"  DONE {sid}: {status} (exit code {code})")
        if o.export:
            subprocess.call([sys.executable, str(ROOT / "tools" / "export_surveys.py"), sid, "--no-images",
                             "--output", str(out)], cwd=ROOT)

    log(f"trigger finished: {counts}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
