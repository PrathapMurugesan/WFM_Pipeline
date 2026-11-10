"""
wfm_trigger.py - the incremental trigger: process every WFM survey that is ready and new / changed.

Run it on a schedule (cron, see deploy/install_cron.sh), e.g. every 30 minutes:
    python wfm_trigger.py                      check all surveys, process what is ready and new / changed
    python wfm_trigger.py --dry-run            only show what would be processed (and why others are not)
    python wfm_trigger.py --survey <id>        only this survey (still checked against the rules)
    python wfm_trigger.py --survey <id> --force   process it even if not ready / unchanged
    options passed on to run_pipeline.py: --output, --output-s3, --project-master, --tracker

Each run:
  1. Postgres: every NSV survey (wfm_survey_schedule) + its wfm_report rows                (wfm_metadata.list_surveys)
  2. ready = D1..D4 reports all approved, nothing locked                                  (wfm_metadata.eligibility)
  3. S3: the upload must be there - reports/<survey id>/ with the D1..D4 folders         (settings REPORTS_S3_ROOT)
  4. new or changed? fingerprint = report versions / status / update times + the S3 files (count, newest change);
     the same fingerprint as the last successful run -> skipped
  5. run_pipeline.py <survey id> with UPC / zone / phase / cycle from Postgres -> bronze, silver, gold
     zone from master_zone via nsv_zone_id (UNKNOWN if not found); silver / gold under run_date=<today>
  6. state: output/state/wfm_trigger_state.json (+ <output-s3>/state/ when --output-s3 is set), one entry per survey:
     fingerprint, status ok / problems / failed, exit code, run time, attempts. A failed run is retried on the next
     trigger, up to MAX_ATTEMPTS times per fingerprint.
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
MAX_ATTEMPTS = 3                     # failed runs of the same fingerprint before the survey is left alone
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

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self.path)
        if self.s3:
            bucket, _, key = self.s3[5:].partition("/")
            _s3().put_object(Bucket=bucket, Key=key, Body=json.dumps(self.data, indent=2).encode("utf-8"))


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
    log(f"  RUN  {' '.join(cmd[1:])}")
    return subprocess.call(cmd, cwd=ROOT, env={**os.environ, "PYTHONIOENCODING": "utf-8"})


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--survey", help="only this survey id")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="with --survey: process even if not ready / unchanged")
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--output-s3", default=os.environ.get("NSV_OUTPUT_S3"),
                    help="s3://bucket/prefix (default: env NSV_OUTPUT_S3); state is kept there too")
    ap.add_argument("--project-master")
    ap.add_argument("--tracker")
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
            log(f"  skip {sid}: approved, but nothing in S3 yet")
            continue
        fp = wfm_metadata.fingerprint(sched, reports) + f"|s3:{upload['files']}:{upload['newest']}"
        last = state.get(sid)
        if not o.force and last.get("fingerprint") == fp:
            if last.get("status") in ("ok", "problems"):
                counts["unchanged"] = counts.get("unchanged", 0) + 1
                continue
            if last.get("attempts", 0) >= MAX_ATTEMPTS:
                counts["gave_up"] = counts.get("gave_up", 0) + 1
                log(f"  skip {sid}: failed {last['attempts']} times on this version - fix and rerun with --survey {sid} --force")
                continue
        reason = "forced" if o.force else ("new" if not last else "changed" if last.get("fingerprint") != fp else "retry")
        log(f"  {reason.upper():8} {sid}  UPC {details['upc']}  zone {details['zone']}  phase {details['phase']}  cycle {details['cycle']}  "
            f"S3 {upload['files']} file(s) {upload['deliverables']}")
        if o.dry_run:
            counts["would_run"] = counts.get("would_run", 0) + 1
            continue
        attempts = (last.get("attempts", 0) + 1) if last.get("fingerprint") == fp else 1
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

    log(f"trigger finished: {counts}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
