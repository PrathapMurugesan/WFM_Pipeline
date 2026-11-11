"""
wfm_cdn.py - defect images -> CDN, and the CDN link into the silver master_pothole / master_cracking tables.
Same logic as the Databricks 'bulk upload' notebook, applied to the silver output of one survey.

    python wfm_cdn.py <silver folder> <migrated survey folder>        (run_pipeline.py calls push_images)
    python wfm_cdn.py ... --dry-run                                  match images, upload nothing

  CDN      POST <CDN_BASE_URL>/defects/images/upload  multipart: image, defect_type, display_defect_id
           success = HTTP 201, link = response data.url.  CDN_BASE_URL: env NSV_CDN_BASE_URL,
           default https://nhaidhms-stage-api.dic.org.in.  4 workers, 2 attempts (1 s apart), 30 s timeout.
  images   every image of the migrated survey; 'crack' / 'pothole' in its folder path -> crack_idx / pot_idx
           (by file name and by name without extension)
  rows     master_pothole (defect_type pothole), master_cracking (defect_type cracking) in silver dlm_kpi/:
           image_name set and image_url empty -> one upload per row, display_defect_id = the row's id
           (sanitized: [^A-Za-z0-9._-] -> '-', max 128). File: pothole rows pot_idx[name] / pot_idx[name w/o .jpg] /
           crack_idx[name]; cracking rows the other way round.
  result   column image_url (one CDN link per row) in dlm_kpi/ and dlm_postgres/ (parquet + csv):
           matched by display_defect_id; rows without display_defect_id by image_name.
           <silver folder>/dq/cdn_images.csv: every row tried (id, image, file, link or error)
"""
import argparse
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path, PurePosixPath

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "bronze"))

CDN_BASE_URL = os.environ.get("NSV_CDN_BASE_URL", "https://nhaidhms-stage-api.dic.org.in").rstrip("/")
UPLOAD_PATH = "/defects/images/upload"
TABLES = {"master_pothole": "pothole", "master_cracking": "cracking"}
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".webp")
MAX_WORKERS, RETRY_LIMIT, RETRY_DELAY, TIMEOUT = 4, 2, 1, 30

_local = threading.local()


def sanitize_id(raw_id):
    return re.sub(r"[^A-Za-z0-9._-]", "-", str(raw_id))[:128]


def _session():
    import requests
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
        _local.session.headers.update({"Connection": "keep-alive"})
    return _local.session


def build_image_index(location):
    """(store, crack_idx, pot_idx): every image of the migrated survey, by file name and by stem."""
    from nsv.storage import ReportStorage
    store = ReportStorage.open(location)
    crack_idx, pot_idx = {}, {}
    for p in store.list_files():
        if PurePosixPath(p).suffix.lower() not in IMAGE_EXTENSIONS:
            continue
        folder, name, stem = str(PurePosixPath(p).parent).lower(), PurePosixPath(p).name, PurePosixPath(p).stem
        idx = crack_idx if "crack" in folder else pot_idx if "pothole" in folder else None
        if idx is not None:
            idx[name] = p
            idx.setdefault(stem, p)
    return store, crack_idx, pot_idx


def bulk_upload_one(store, path, defect_type, display_defect_id, base_url):
    """-> (display_defect_id, True, url) or (display_defect_id, False, error)"""
    fname = PurePosixPath(path).name
    err = None
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            resp = _session().post(base_url + UPLOAD_PATH,
                                   files={"image": (fname, store.read_bytes(path), "image/jpeg")},
                                   data={"defect_type": defect_type, "display_defect_id": display_defect_id},
                                   timeout=TIMEOUT)
            if resp.status_code == 201:
                url = (resp.json().get("data") or {}).get("url", "")
                if url:
                    return display_defect_id, True, url
                err = f"HTTP 201 without data.url: {resp.text[:200]}"
            else:
                err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            err = str(e)[:200]
        if attempt < RETRY_LIMIT:
            time.sleep(RETRY_DELAY)
    return display_defect_id, False, err


def _blank(v):
    return v is None or (isinstance(v, float) and v != v) or (isinstance(v, str) and not v.strip())


def _find(img, primary, other):
    """The notebook's lookup: own index by name, by name without .jpg, then the other index by name.
    A cell with several names ('a.jpg, b.jpg') falls back to its first name."""
    for name in (img, img.split(",")[0].strip()):
        path = primary.get(name) or primary.get(name.replace(".jpg", "")) or other.get(name)
        if path:
            return path
    return None


def push_images(silver_dir, location, dry_run=False, base_url=CDN_BASE_URL, log=print):
    silver_dir = Path(silver_dir)
    store, crack_idx, pot_idx = build_image_index(location)
    log(f"CDN {base_url}{UPLOAD_PATH}  images in the survey: cracking {len(set(crack_idx.values()))}, "
        f"pothole {len(set(pot_idx.values()))}{'  (dry run)' if dry_run else ''}")
    record, summary = [], {"tasks": 0, "uploaded": 0, "failed": 0, "not_found": 0, "rows_with_url": 0}

    for table, dtype in TABLES.items():
        kpi_file = silver_dir / "dlm_kpi" / f"{table}.parquet"
        if not kpi_file.is_file():
            continue
        df = pd.read_parquet(kpi_file)
        if "image_name" not in df.columns:
            continue
        if "image_url" not in df.columns:
            df["image_url"] = None
        primary, other = (pot_idx, crack_idx) if dtype == "pothole" else (crack_idx, pot_idx)

        # ---- tasks: rows with image_name and no image_url (one per display_defect_id) ----
        tasks, id_map, not_found = [], {}, 0
        for _, row in df.iterrows():
            img, did = row["image_name"], row.get("display_defect_id")
            if _blank(img) or not _blank(row["image_url"]) or _blank(did):
                continue
            path = _find(str(img), primary, other)
            if not path:
                not_found += 1
                record.append({"table": table, "display_defect_id": did, "image_name": img, "file": None,
                               "cdn_url": None, "status": "image_not_found", "error": None})
                continue
            safe = sanitize_id(did)
            if safe in id_map:
                continue
            id_map[safe] = (did, img, path)
            tasks.append((path, dtype, safe))
        log(f"  {table}: {int(df['image_name'].notna().sum())} rows with image_name -> {len(tasks)} matched, "
            f"{not_found} image not found")
        summary["tasks"] += len(tasks)
        summary["not_found"] += not_found

        # ---- upload ----
        urls = {}                                                      # display_defect_id -> url
        if tasks and not dry_run:
            ok = fail = 0
            t0 = time.time()
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = [pool.submit(bulk_upload_one, store, p, d, s, base_url) for p, d, s in tasks]
                for i, fut in enumerate(as_completed(futures), 1):
                    safe, success, info = fut.result()
                    did, img, path = id_map[safe]
                    if success:
                        ok += 1
                        urls[did] = info
                    else:
                        fail += 1
                        if fail <= 3:
                            log(f"    FAIL [{safe}]: {info}")
                    record.append({"table": table, "display_defect_id": did, "image_name": img,
                                   "file": store.full_path(path), "cdn_url": info if success else None,
                                   "status": "uploaded" if success else "upload_failed",
                                   "error": None if success else info})
                    if i % 100 == 0 or i == len(tasks):
                        log(f"    progress {i}/{len(tasks)}  ok {ok}  failed {fail}  ({time.time() - t0:.0f}s)")
            summary["uploaded"] += ok
            summary["failed"] += fail
        elif tasks:
            record += [{"table": table, "display_defect_id": d, "image_name": i, "file": store.full_path(p),
                        "cdn_url": None, "status": "dry_run", "error": None} for d, i, p in id_map.values()]

        # ---- image_url into dlm_kpi and dlm_postgres: by display_defect_id, else by image_name ----
        by_name = {}
        for did, url in urls.items():
            name = next((i for d, i, _ in id_map.values() if d == did), None)
            if name is not None:
                by_name.setdefault(str(name), url)
        for sub in ("dlm_kpi", "dlm_postgres"):
            f = silver_dir / sub / f"{table}.parquet"
            if not f.is_file():
                continue
            t = df if sub == "dlm_kpi" else pd.read_parquet(f)
            if "image_url" not in t.columns:
                t["image_url"] = None
            filled = []
            for did, img, cur in zip(t.get("display_defect_id", [None] * len(t)), t["image_name"], t["image_url"]):
                if not _blank(cur):
                    filled.append(cur)
                elif not _blank(did) and did in urls:
                    filled.append(urls[did])
                elif _blank(did) and not _blank(img) and str(img) in by_name:
                    filled.append(by_name[str(img)])
                else:
                    filled.append(None)
            t["image_url"] = filled
            if "image_urls" in t.columns:                              # keep image_url next to image_urls
                cols = [c for c in t.columns if c != "image_url"]
                cols.insert(cols.index("image_urls") + 1, "image_url")
                t = t[cols]
            t.to_parquet(f, index=False)
            csv = f.with_suffix(".csv")
            if csv.is_file():
                t.assign(**{c: t[c].map(lambda v: ", ".join(map(str, v)) if hasattr(v, "__len__") and not isinstance(v, str) else v)
                            for c in t.columns if t[c].dtype == object}).to_csv(csv, index=False, encoding="utf-8-sig")
            if sub == "dlm_kpi":
                summary["rows_with_url"] += int(pd.Series(filled).notna().sum())

    (silver_dir / "dq").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(record, columns=["table", "display_defect_id", "image_name", "file", "cdn_url", "status", "error"]) \
        .to_csv(silver_dir / "dq" / "cdn_images.csv", index=False, encoding="utf-8-sig")
    log(f"  CDN done: {summary}")
    return summary


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
