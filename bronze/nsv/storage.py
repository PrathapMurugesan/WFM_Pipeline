"""
storage.py - every file read and write goes through here.

READ - the uploaded report, seen as d1..d4 with clean file names:
    ReportStorage.open("s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/<report id>/")   the WFM upload (S3)
    ReportStorage.open("<local folder>")                                                  a local copy / UPC folder

    Upload layout:  <report id>/D1/1793260589326-LRMS_DataTemplate.xlsx
                    <report id>/D3/GIS Mapping -As Per CA/GIS Shape Files/Cracking/1793260591234-Cracking.shp
    seen as:        d1/LRMS_DataTemplate.xlsx
                    d3/GIS Mapping -As Per CA/GIS Shape Files/Cracking/Cracking.shp
    - deliverable folders 'D1'..'D4' or 'Deliverable 1 - RAMS Data' .. (directly or one level down) -> d1..d4
    - the upload timestamp in front of a file name ('1793260589326-') is removed; it is kept in uploaded_name()
    - the same file uploaded twice (two timestamps): the NEWEST is used, the older one is listed in .superseded

WRITE - LocalStorage(root): the bronze output folder.

Common methods: list_files(folder), read_bytes(path), size(path), full_path(path), describe()
Paths are relative, with forward slashes.
"""
import re
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

DELIVERABLE_FOLDER = re.compile(r"^(?:deliverable\s*[-_.]?\s*|d)(\d)(?![0-9])", re.IGNORECASE)   # D1, Deliverable 1 - ..
UPLOAD_PREFIX = re.compile(r"^(\d{10,13})[-_ ](?=.)")       # '1793260589326-LRMS_DataTemplate.xlsx' (epoch s / ms)


def split_upload_name(name):
    """'1793260589326-LRMS_DataTemplate.xlsx' -> ('LRMS_DataTemplate.xlsx', 1793260589326); no prefix -> (name, None)"""
    m = UPLOAD_PREFIX.match(name)
    return (name[m.end():], int(m.group(1))) if m else (name, None)


def upload_time(stamp):
    """1793260589326 -> '2026-10-29T10:16:29+00:00' (ms or s since 1970)."""
    if stamp is None:
        return None
    seconds = stamp / 1000 if stamp > 10 ** 11 else stamp
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat(timespec="seconds")


# =============================================================================
# Output
# =============================================================================
class LocalStorage:
    def __init__(self, root):
        self.root = Path(root)

    def describe(self):
        return f"local:{self.root}"

    def list_files(self, folder=""):
        base = self.root / folder
        if not base.is_dir():
            return []
        return sorted(p.relative_to(self.root).as_posix() for p in base.rglob("*") if p.is_file())

    def read_bytes(self, path):
        return (self.root / path).read_bytes()

    def size(self, path):
        return (self.root / path).stat().st_size

    def full_path(self, path):
        return str(self.root / path)

    def write_bytes(self, path, data):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


# =============================================================================
# Input: the uploaded report
# =============================================================================
class ReportStorage:
    """
    Built from the raw listing [(relative raw path, size)] under the report root. Subclasses read the bytes.
      .folders     {'d1': 'D1', ...}           raw deliverable folder per deliverable
      .duplicates  [('d2', 'Deliverable 2 x')] a second folder for the same deliverable (not read)
      .other       raw names directly under the root that are not deliverable folders
      .superseded  [(clean path, older raw path)] older uploads of the same file
    """

    @staticmethod
    def open(location):
        if str(location).startswith("s3://"):
            return S3ReportStorage(str(location))
        return LocalReportStorage(location)

    def _build(self, listing):
        self.folders, self.duplicates, self.other, self.superseded = {}, [], set(), []
        self._files, self._stamps, self._sizes = {}, {}, {}
        tops = sorted({p.split("/")[0] for p, _ in listing if "/" in p})
        roots = {}                                        # raw folder prefix -> 'dN'
        for top in tops:
            m = DELIVERABLE_FOLDER.match(top)
            subs = [] if m else sorted({p.split("/")[1] for p, _ in listing if p.startswith(top + "/") and p.count("/") >= 2})
            for raw, name in ([(top, top)] if m else [(f"{top}/{s}", s) for s in subs if DELIVERABLE_FOLDER.match(s)]):
                key = f"d{DELIVERABLE_FOLDER.match(name).group(1)}"
                if key in self.folders:
                    self.duplicates.append((key, raw))
                else:
                    self.folders[key], roots[raw] = raw, key
        for path, size in listing:
            root = next((r for r in roots if path.startswith(r + "/")), None)
            if root is None:                              # outside the deliverable folders
                if any(path.startswith(raw + "/") for _, raw in self.duplicates):
                    continue
                top = path.split("/")[0]
                wrapper = any(r.startswith(top + "/") for r in roots)     # road-name folder around D1..D4
                self.other.add(path if wrapper else top)
                continue
            rest = PurePosixPath(path[len(root) + 1:])
            clean_name, stamp = split_upload_name(rest.name)
            clean = f"{roots[root]}/{(rest.parent / clean_name).as_posix()}".replace("/./", "/")
            if clean in self._files:                      # uploaded again: keep the newest
                old_stamp = self._stamps[clean] or -1
                if (stamp or -1) > old_stamp:
                    self.superseded.append((clean, self._files[clean]))
                else:
                    self.superseded.append((clean, path))
                    continue
            self._files[clean], self._stamps[clean], self._sizes[clean] = path, stamp, size
        self.other = sorted(self.other)

    # ---- common API ----
    def list_files(self, folder=""):
        folder = folder.strip("/")
        return sorted(p for p in self._files if not folder or p == folder or p.startswith(folder + "/"))

    def size(self, path):
        return self._sizes[path]

    def uploaded_name(self, path):
        """The file name as uploaded (with the timestamp prefix)."""
        return PurePosixPath(self._files[path]).name

    def uploaded_at(self, path):
        return upload_time(self._stamps.get(path))


class LocalReportStorage(ReportStorage):
    def __init__(self, folder):
        root = Path(folder).resolve()
        if sys.platform == "win32" and not str(root).startswith("\\\\?\\"):
            root = Path("\\\\?\\" + str(root))            # vendor paths can exceed 260 characters
        self.root = root
        self._build([(p.relative_to(root).as_posix(), p.stat().st_size) for p in root.rglob("*") if p.is_file()])

    def describe(self):
        return f"local:{str(self.root).removeprefix(chr(92) * 2 + '?' + chr(92))}"

    def read_bytes(self, path):
        return (self.root / self._files[path]).read_bytes()

    def full_path(self, path):
        return str(self.root / self._files[path]).removeprefix("\\\\?\\")


class S3ReportStorage(ReportStorage):
    """s3://bucket/prefix/<report id>/ - listed once; files are read on demand. Credentials: the usual AWS
    chain (environment, ~/.aws, the instance's IAM role) - never in this code."""

    def __init__(self, url):
        import boto3                                      # only needed for S3
        self.bucket, _, prefix = url[len("s3://"):].partition("/")
        self.prefix = prefix if prefix.endswith("/") else prefix + "/"
        self.s3 = boto3.client("s3")
        listing = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    listing.append((obj["Key"][len(self.prefix):], obj["Size"]))
        if not listing:
            raise FileNotFoundError(f"nothing under s3://{self.bucket}/{self.prefix} (check the report id / access)")
        self._build(listing)

    def describe(self):
        return f"s3://{self.bucket}/{self.prefix}"

    def read_bytes(self, path):
        return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + self._files[path])["Body"].read()

    def full_path(self, path):
        return f"s3://{self.bucket}/{self.prefix}{self._files[path]}"
