"""
wfm_migrate.py - copy one WFM survey upload to the stage bucket, every zip unzipped, without videos.

    python wfm_migrate.py 3433cb89-f583-438c-ab44-6e99455e1bf4
    python wfm_migrate.py <survey id> --dest s3://dhms-new-stgae/NSV_Pipeline/wfm_reports
    python wfm_migrate.py <local copy of a report> --report-id <id> --dest <local folder>      (testing)

  from:  s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<survey id>/D1..D4/<timestamp>-<file>   (+ zips)
  to:    <dest>/<survey id>/D1..D4/<file>          zips opened (folder = zip name), upload timestamps removed,
                                                    videos left out, damaged zip entries recovered (CRC-checked)
         <dest>/_manifests/<survey id>.json         every file: source, size, sha256 / skipped / unreadable

The bronze extraction then reads the migrated copy (run_pipeline.py does both).
Default destination: env NSV_STAGE_S3, else s3://dhms-new-stgae/NSV_Pipeline/wfm_reports
"""
import argparse
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "bronze"))

DEFAULT_DEST = os.environ.get("NSV_STAGE_S3", "s3://dhms-new-stgae/NSV_Pipeline/wfm_reports")
VIDEO_EXTENSIONS = (".mp4", ".avi", ".mov", ".mkv", ".wmv", ".m4v", ".mpg", ".mpeg", ".3gp", ".flv", ".webm")
WORKERS = 16


class Writer:
    """Write files under an s3:// prefix or a local folder."""
    def __init__(self, root):
        self.root = root.rstrip("/")
        self.s3 = root.startswith("s3://")
        if self.s3:
            import boto3
            self.client = boto3.client("s3")
            self.bucket, _, self.prefix = self.root[5:].partition("/")

    def put(self, rel, data):
        if self.s3:
            self.client.put_object(Bucket=self.bucket, Key=f"{self.prefix}/{rel}".lstrip("/"), Body=data)
        else:
            target = Path(self.root) / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)

    def url(self, rel=""):
        return f"{self.root}/{rel}".rstrip("/")


def migrate(source, survey_id, dest=DEFAULT_DEST, log=print):
    """Copy + unzip one survey. Returns the manifest (dict); manifest['location'] is the migrated copy."""
    from main import resolve_source                     # bronze/main.py: report id -> s3 location
    from nsv.storage import ReportStorage

    location, _ = resolve_source(source)
    upload = ReportStorage.open(location)
    if not upload.folders:
        raise RuntimeError(f"no deliverable folder (D1..D4) in {upload.describe()}")
    out = Writer(dest)
    files = upload.list_files()
    manifest = {"survey_id": survey_id, "source": upload.describe(), "location": out.url(survey_id),
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "deliverable_folders": dict(upload.folders), "zips": getattr(upload, "zips", []),
                "files": [], "skipped_videos": [], "unreadable": []}
    log(f"MIGRATE {upload.describe()} -> {out.url(survey_id)}  ({len(files)} file(s) after unzipping)")

    def copy(path):
        d, _, rest = path.partition("/")
        if PurePosixPath(rest).suffix.lower() in VIDEO_EXTENSIONS:
            return ("video", path, None)
        try:
            data = upload.read_bytes(path)
        except Exception as e:
            return ("unreadable", path, f"{type(e).__name__}: {e}")
        rel = f"{survey_id}/{d.upper()}/{rest}"
        out.put(rel, data)
        return ("ok", path, {"path": rel, "source": upload.full_path(path), "uploaded_name": upload.uploaded_name(path),
                             "size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        for status, path, info in pool.map(copy, files):
            if status == "ok":
                manifest["files"].append(info)
            elif status == "video":
                manifest["skipped_videos"].append(path)
            else:
                manifest["unreadable"].append({"path": path, "error": info})
                log(f"  cannot copy '{path}': {info}")
    manifest.update(finished_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    copied=len(manifest["files"]), videos_skipped=len(manifest["skipped_videos"]),
                    unreadable_count=len(manifest["unreadable"]),
                    zip_entries_recovered=getattr(upload, "zip_recovered", 0),
                    other_files_in_root=upload.other, superseded_uploads=upload.superseded)
    out.put(f"_manifests/{survey_id}.json", json.dumps(manifest, indent=2, default=str).encode("utf-8"))
    log(f"  copied {manifest['copied']} file(s), {manifest['videos_skipped']} video(s) left out, "
        f"{manifest['unreadable_count']} unreadable, {manifest['zip_entries_recovered']} zip entr(y/ies) recovered "
        f"-> {manifest['location']}")
    return manifest


def main(args):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("source", help="survey id, s3:// report folder or local folder")
    ap.add_argument("--report-id", help="survey id (when the source folder is not named by it)")
    ap.add_argument("--dest", default=DEFAULT_DEST)
    o = ap.parse_args(args)
    survey_id = o.report_id or o.source.rstrip("/\\").replace("\\", "/").split("/")[-1]
    m = migrate(o.source, survey_id, o.dest)
    return 1 if m["unreadable_count"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
