#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Install the WFM trigger as a cron job on the EC2 instance (run once, as the user that owns the repo).
#
#   ./deploy/install_cron.sh                          every 15 minutes, output in <repo>/output
#   ./deploy/install_cron.sh 15                       every 15 minutes
#   ./deploy/install_cron.sh --daily-ist 00:30        once a day at 00:30 IST (converted to the server's time zone)
#   OUTPUT_S3=s3://bucket/NSV_Pipeline PROJECT_MASTER=s3://.../project_master.xlsx ./deploy/install_cron.sh
#   PG_PUSH=1 ...                                     also push every processed survey to the DHMS Postgres
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

SCHEDULE=""
if [[ "${1:-}" == "--daily-ist" ]]; then
  IST="${2:-00:30}"
  [[ "$IST" =~ ^([01]?[0-9]|2[0-3]):([0-5][0-9])$ ]] || { echo "ERROR: bad time '$IST' (use HH:MM, e.g. 00:30)"; exit 2; }
  # IST is always UTC+05:30 (no daylight saving) -> server time = IST - 330 min + the server's own UTC offset
  OFF=$(date +%z); SIGN=${OFF:0:1}; OFFMIN=$(( 10#${OFF:1:2} * 60 + 10#${OFF:3:2} )); [[ $SIGN == "-" ]] && OFFMIN=$(( -OFFMIN ))
  T=$(( ( 10#${BASH_REMATCH[1]} * 60 + 10#${BASH_REMATCH[2]} - 330 + OFFMIN + 2880 ) % 1440 ))
  SCHEDULE="$(( T % 60 )) $(( T / 60 )) * * *"
  WHEN="daily at $IST IST = $(printf '%02d:%02d' $(( T / 60 )) $(( T % 60 ))) server time (UTC$OFF)"
else
  MINUTES="${1:-15}"
  SCHEDULE="*/$MINUTES * * * *"
  WHEN="every $MINUTES min"
fi
[[ -x "$PYTHON" ]] || { echo "ERROR: $PYTHON not found - create the venv first (see README)"; exit 2; }
[[ -f "$REPO/postgres_creds.txt" || -n "${DB_HOST:-}" ]] || echo "WARNING: no $REPO/postgres_creds.txt - the trigger cannot read Postgres"

ARGS=""
[[ -n "${OUTPUT_S3:-}" ]] && ARGS="$ARGS --output-s3 $OUTPUT_S3"
[[ -n "${PROJECT_MASTER:-}" ]] && ARGS="$ARGS --project-master $PROJECT_MASTER"
[[ "${PG_PUSH:-}" == "1" ]] && ARGS="$ARGS --pg-push"
mkdir -p "$REPO/output/logs"

LINE="$SCHEDULE cd '$REPO' && flock -n /tmp/wfm_trigger.lock '$PYTHON' wfm_trigger.py$ARGS >> '$REPO'/output/logs/trigger_\$(date +\\%Y\\%m\\%d).log 2>&1 $TAG"
command -v crontab >/dev/null || { echo "ERROR: cron is not installed - run: apt install -y cron && systemctl enable --now cron"; exit 2; }
# a machine without a crontab yet: 'crontab -l' fails and grep finds nothing - neither may stop the script
{ { crontab -l 2>/dev/null || true; } | { grep -v "$TAG" || true; }; echo "$LINE"; } | crontab -
echo "installed ($WHEN):"
crontab -l | grep "$TAG" || { echo "ERROR: the job is not in the crontab"; exit 1; }
systemctl is-active --quiet cron 2>/dev/null || echo "WARNING: the cron service is not running - run: systemctl enable --now cron"
