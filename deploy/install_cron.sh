#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Install the WFM trigger as a cron job on the EC2 instance (run once, as the user that owns the repo).
#
#   ./deploy/install_cron.sh                          every 15 minutes, output in <repo>/output
#   ./deploy/install_cron.sh 15                       every 15 minutes
#   OUTPUT_S3=s3://bucket/NSV_Pipeline PROJECT_MASTER=s3://.../project_master.xlsx ./deploy/install_cron.sh
#   ./deploy/install_cron.sh --remove                 take the job out again
#
# The job runs wfm_trigger.py under flock, so a run that is still busy is never started twice.
# Log: <repo>/output/logs/trigger_<date>.log      State: <repo>/output/state/wfm_trigger_state.json
# Needs: postgres_creds.txt in the repo folder (or the DB_* environment variables) and the instance IAM role
#        for S3 (read nhai-dhms-uat/wfm_reports/*, write OUTPUT_S3 if set).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-$REPO/.venv/bin/python}"
TAG="# wfm-trigger"

if [[ "${1:-}" == "--remove" ]]; then
  { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; } | crontab -
  echo "WFM trigger removed from crontab"
  exit 0
fi

MINUTES="${1:-15}"
[[ -x "$PYTHON" ]] || { echo "ERROR: $PYTHON not found - create the venv first (see README)"; exit 2; }
[[ -f "$REPO/postgres_creds.txt" || -n "${DB_HOST:-}" ]] || echo "WARNING: no $REPO/postgres_creds.txt - the trigger cannot read Postgres"

ARGS=""
[[ -n "${OUTPUT_S3:-}" ]] && ARGS="$ARGS --output-s3 $OUTPUT_S3"
[[ -n "${PROJECT_MASTER:-}" ]] && ARGS="$ARGS --project-master $PROJECT_MASTER"
mkdir -p "$REPO/output/logs"

LINE="*/$MINUTES * * * * cd $REPO && flock -n /tmp/wfm_trigger.lock $PYTHON wfm_trigger.py$ARGS >> $REPO/output/logs/trigger_\$(date +\\%Y\\%m\\%d).log 2>&1 $TAG"
command -v crontab >/dev/null || { echo "ERROR: cron is not installed - run: apt install -y cron && systemctl enable --now cron"; exit 2; }
# a machine without a crontab yet: 'crontab -l' fails and grep finds nothing - neither may stop the script
{ { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; }; echo "$LINE"; } | crontab -
echo "installed (every $MINUTES min):"
crontab -l | grep "$TAG" || { echo "ERROR: the job is not in the crontab"; exit 1; }
systemctl is-active --quiet cron 2>/dev/null || echo "WARNING: the cron service is not running - run: systemctl enable --now cron"
