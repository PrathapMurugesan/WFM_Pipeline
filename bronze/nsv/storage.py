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
    - a .zip is opened and read as the folder it stands for (folder name = zip name without the timestamp):
          D4/1794207928512-Crack_Images_DHMS.zip  [Crack Images_DHMS/L1/a.jpg]  ->  d4/Crack_Images_DHMS/L1/a.jpg
      (a single top folder inside the zip with the same name is not repeated; __MACOSX / ._ entries are skipped;
       on S3 the zip is downloaded once to a temporary folder). The zips are listed in .zips.

WRITE - LocalStorage(root): the bronze output folder.

Common methods: list_files(folder), read_bytes(path), size(path), full_path(path), describe()
Paths are relative, with forward slashes.
"""
import atexit
import re
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

DELIVERABLE_FOLDER = re.compile(r"^(?:deliverable\s*[-_.]?\s*|d)(\d)(?![0-9])", re.IGNORECASE)   # D1, Deliverable 1 - ..
UPLOAD_PREFIX = re.compile(r"^(\d{10,13})[-_ ](?=.)")       # '1793260589326-LRMS_DataTemplate.xlsx' (epoch s / ms)


def _norm(text):
    """'Crack Images_DHMS' == 'Crack_Images_DHMS' == 'crackimagesdhms'"""
    return re.sub(r"[^a-z0-9]", "", str(text).lower())


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
        self._expand_zips()

    # ---- zip archives: read as folders ----
    def _expand_zips(self):
        self.zips, self._open_zips = [], {}
        for clean in [p for p in self._files if p.lower().endswith(".zip")]:
            raw, stamp = self._files.pop(clean), self._stamps.pop(clean)
            self._sizes.pop(clean)
            base = clean[:-4]                                     # d4/Crack_Images_DHMS
            try:
                zf = zipfile.ZipFile(self._zip_file(raw))
            except Exception as e:
                self.zips.append({"zip": clean, "uploaded": raw, "error": f"{type(e).__name__}: {e}"})
                self._files[clean], self._stamps[clean], self._sizes[clean] = raw, stamp, 0   # listed, not readable
                continue
            members = [i for i in zf.infolist() if not i.is_dir()]
            names = [i.filename.replace("\\", "/") for i in members]
            keep = [(i, n) for i, n in zip(members, names)
                    if not n.startswith("__MACOSX/") and not PurePosixPath(n).name.startswith("._")]
            tops = {n.split("/")[0] for _, n in keep}
            strip = (len(tops) == 1 and all("/" in n for _, n in keep)
                     and _norm(next(iter(tops))) == _norm(PurePosixPath(base).name))
            for info, name in keep:
                rel = name.split("/", 1)[1] if strip else name
                rel = PurePosixPath(rel)
                path = f"{base}/{(rel.parent / split_upload_name(rel.name)[0]).as_posix()}".replace("/./", "/")
                self._files[path] = (raw, info)
                self._stamps[path], self._sizes[path] = stamp, info.file_size
            self._open_zips[raw] = zf
            self.zips.append({"zip": clean, "uploaded": raw, "files": len(keep), "folder": base,
                              "top_folder_dropped": strip})

    def _zip_file(self, raw):
        return self._raw_local(raw)

    def _read(self, path):
        entry = self._files[path]
        if isinstance(entry, tuple):                              # inside a zip
            raw, info = entry
            try:
                return self._open_zips[raw].read(info)
            except zipfile.BadZipFile as e:
                # Some zip tools write archives whose entries overlap; newer Pythons refuse them as a possible
                # 'zip bomb'. The size is checked when the zip is opened, so read the entry without that check.
                if "Overlapped" not in str(e) or not hasattr(info, "_end_offset"):
                    raise
                info._end_offset = None
                self.zip_warnings = getattr(self, "zip_warnings", 0) + 1
                return self._open_zips[raw].read(info)
        return self._read_raw(entry)

    # ---- common API ----
    def list_files(self, folder=""):
        folder = folder.strip("/")
        return sorted(p for p in self._files if not folder or p == folder or p.startswith(folder + "/"))

    def size(self, path):
        return self._sizes[path]

    def read_bytes(self, path):
        return self._read(path)

    def uploaded_name(self, path):
        """The file name as uploaded (with the timestamp prefix); inside a zip: '<zip as uploaded>!<entry>'."""
        entry = self._files[path]
        if isinstance(entry, tuple):
            return f"{PurePosixPath(entry[0]).name}!{entry[1].filename}"
        return PurePosixPath(entry).name

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

    def _raw_local(self, raw):
        return self.root / raw

    def _read_raw(self, raw):
        return (self.root / raw).read_bytes()

    def full_path(self, path):
        entry = self._files[path]
        if isinstance(entry, tuple):
            return f"{str(self.root / entry[0]).removeprefix(chr(92) * 2 + '?' + chr(92))}!{entry[1].filename}"
        return str(self.root / entry).removeprefix("\\\\?\\")


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

    def _raw_local(self, raw):
        """A zip is read many times: downloaded once to a temporary folder (removed when the run ends)."""
        if not hasattr(self, "_tmp"):
            self._tmp = tempfile.mkdtemp(prefix="wfm_zip_")
            atexit.register(shutil.rmtree, self._tmp, True)
        target = Path(self._tmp) / raw.replace("/", "__")
        if not target.exists():
            self.s3.download_file(self.bucket, self.prefix + raw, str(target))
        return target

    def _read_raw(self, raw):
        return self.s3.get_object(Bucket=self.bucket, Key=self.prefix + raw)["Body"].read()

    def full_path(self, path):
        entry = self._files[path]
        if isinstance(entry, tuple):
            return f"s3://{self.bucket}/{self.prefix}{entry[0]}!{entry[1].filename}"
        return f"s3://{self.bucket}/{self.prefix}{entry}"
