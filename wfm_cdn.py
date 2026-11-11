"""
wfm_cdn.py - push the survey's defect images to the image CDN and put the CDN links into the silver tables.

    python wfm_cdn.py <silver folder> <migrated survey folder>        (run_pipeline.py calls push_images)
    python wfm_cdn.py ... --dry-run                                  match images, upload nothing

  CDN:     POST <CDN_BASE_URL>/defects/images/upload   multipart  image=@<file>  defect_type=pothole|cracking
           -> {"success": true, "data": {"url": "https://..."}}       (as the earlier _upload_pothole_images.py)
           CDN_BASE_URL: env NSV_CDN_BASE_URL, default https://nhaidhms-stage-api.dic.org.in
  images:  every image of the migrated survey (D1..D4, any folder); 'crack' / 'pothole' in its folder path
           tells the kind (as the earlier Databricks notebook)
  tables:  silver dlm_kpi/ and dlm_postgres/ master_pothole* and master_cracking*: image_name (file names,
           comma separated) -> each image uploaded ONCE -> image_urls = the CDN links (same order).
           An image that is not found / fails to upload keeps its old value and is listed.
  record:  <silver folder>/dq/cdn_images.csv  (image, kind, source file, cdn url, status) and cdn_map.json
           (re-used on a rerun: an image already on the CDN is not uploaded again)
"""
import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path, PurePosixPath

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "bronze"))

CDN_BASE_URL = os.environ.get("NSV_CDN_BASE_URL", "https://nhaidhms-stage-api.dic.org.in").rstrip("/")
UPLOAD_PATH = "/defects/images/upload"
DEFECT_TYPE = {"pothole": "pothole", "cracking": "cracking"}      # form field defect_type per table family
TABLES = {"pothole": ["master_pothole"],
          "cracking": ["master_cracking", "master_cracking_child_100m", "master_cracking_parent"]}
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")
SEPARATOR = ", "
WORKERS, RETRIES, TIMEOUT = 8, 3, 120


def _kind(path):
    p = path.lower()
    return "cracking" if "crack" in p else "pothole" if "pothole" in p else None


def image_index(location):
    """{kind: {file name: path, stem: path}} of every image in the migrated survey."""
    from nsv.storage import ReportStorage
    store = ReportStorage.open(location)
    index = {"cracking": {}, "pothole": {}}
    for p in store.list_files():
        if PurePosixPath(p).suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        kind = _kind(str(PurePosixPath(p).parent))
        if kind:
            name = PurePosixPath(p).name
            index[kind].setdefault(name, p)
            index[kind].setdefault(PurePosixPath(p).stem, p)       # names written without .jpg
    return store, index


def upload(data, file_name, defect_type, base_url=CDN_BASE_URL):
    """One image -> its CDN url (raises after RETRIES failed attempts)."""
    import requests
    last = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.post(base_url + UPLOAD_PATH, files={"image": (file_name, data, "image/jpeg")},
                              data={"defect_type": defect_type}, timeout=TIMEOUT)
            body = r.json()
            if r.ok and body.get("success") and (body.get("data") or {}).get("url"):
                return body["data"]["url"]
            last = f"HTTP {r.status_code}: {str(body)[:300]}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(2 * attempt)
    raise RuntimeError(last)


def _names(cell):
    if cell is None or (isinstance(cell, float) and cell != cell):
        return []
    return [n.strip() for n in str(cell).split(",") if n.strip()]


def push_images(silver_dir, location, dry_run=False, base_url=CDN_BASE_URL, log=print):
    """Upload the images the silver tables refer to; rewrite image_urls with the CDN links. Returns a summary."""
    silver_dir = Path(silver_dir)
    cache_file = silver_dir / "dq" / "cdn_map.json"
    cache = json.loads(cache_file.read_text(encoding="utf-8")) if cache_file.is_file() else {}
    store, index = image_index(location)
    log(f"CDN {base_url}{UPLOAD_PATH}  images in the survey: cracking {len(set(index['cracking'].values()))}, "
        f"pothole {len(set(index['pothole'].values()))}{'  (dry run)' if dry_run else ''}")

    # ---- which images do the tables need? ----
    wanted = {}                                            # (kind, name) -> source path or None
    tables = []
    for kind, names in TABLES.items():
        for table in names:
            for sub in ("dlm_kpi", "dlm_postgres"):
                f = silver_dir / sub / f"{table}.parquet"
                if f.is_file():
                    df = pd.read_parquet(f)
                    if "image_name" in df.columns:
                        tables.append((kind, f, df))
                        for cell in df["image_name"]:
                            for n in _names(cell):
                                wanted[(kind, n)] = index[kind].get(n) or index[kind].get(PurePosixPath(n).stem)

    # ---- upload each image once ----
    def one(item):
        (kind, name), path = item
        key = f"{kind}/{name}"
        if key in cache and cache[key].get("url"):
            return key, {**cache[key], "status": "already_on_cdn"}
        if not path:
            return key, {"kind": kind, "image": name, "source": None, "url": None, "status": "image_not_found"}
        if dry_run:
            return key, {"kind": kind, "image": name, "source": path, "url": None, "status": "dry_run"}
        try:
            url = upload(store.read_bytes(path), PurePosixPath(path).name, DEFECT_TYPE[kind], base_url)
            return key, {"kind": kind, "image": name, "source": store.full_path(path), "url": url, "status": "uploaded"}
        except Exception as e:
            return key, {"kind": kind, "image": name, "source": store.full_path(path), "url": None,
                         "status": "upload_failed", "error": str(e)[:300]}

    results = dict(ThreadPoolExecutor(max_workers=WORKERS).map(one, wanted.items())) if wanted else {}
    cache.update({k: v for k, v in results.items() if v.get("url")})

    # ---- image_urls = the CDN links ----
    updated_rows = 0
    for kind, f, df in tables:
        urls = []
        for cell, old in zip(df["image_name"], df["image_urls"] if "image_urls" in df.columns else [None] * len(df)):
            names = _names(cell)
            if not names and old is not None and len(old) and not isinstance(old, str):
                # 100 m / 1 km rows: no image_name, image_urls = paths of their 10 m rows' images
                names = [str(p).replace("\\", "/").split("!")[-1].split("/")[-1] for p in old]
            links = [(results.get(f"{kind}/{n}") or cache.get(f"{kind}/{n}") or {}).get("url") for n in names]
            if names and all(links):
                urls.append(links)
                updated_rows += 1
            else:
                urls.append(old)                           # not (all) on the CDN: keep what was there
        df["image_urls"] = urls
        df.to_parquet(f, index=False)
        csv = f.with_suffix(".csv")
        if csv.is_file():
            df.assign(image_urls=[SEPARATOR.join(u) if isinstance(u, list) else u for u in urls]).to_csv(
                csv, index=False, encoding="utf-8-sig")

    # ---- record ----
    (silver_dir / "dq").mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(cache, indent=2), encoding="utf-8")
    report = pd.DataFrame(results.values(), columns=["kind", "image", "source", "url", "status", "error"])
    report.to_csv(silver_dir / "dq" / "cdn_images.csv", index=False, encoding="utf-8-sig")
    counts = report["status"].value_counts().to_dict() if len(report) else {}
    log(f"  images needed {len(wanted)}: {counts}; table rows with CDN links: {updated_rows}")
    return {"needed": len(wanted), "status": counts, "rows_updated": updated_rows,
            "failed": int(counts.get("upload_failed", 0)), "not_found": int(counts.get("image_not_found", 0))}


def main(args):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("silver_dir")
    ap.add_argument("location", help="the migrated survey folder (s3:// or local)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--cdn-url", default=CDN_BASE_URL)
    o = ap.parse_args(args)
    s = push_images(o.silver_dir, o.location, o.dry_run, o.cdn_url.rstrip("/"))
    return 1 if s["failed"] else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
