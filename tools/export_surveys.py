"""
export_surveys.py - one zip per survey with everything the pipeline wrote for it: bronze, silver, gold, logs,
metadata (Postgres rows) and the migration manifest. For checking the values somewhere else.

    python tools/export_surveys.py                         every survey in the processed log
    python tools/export_surveys.py <survey id> [<id> ...]  only these
    python tools/export_surveys.py ... --no-images         leave out images (much smaller zips)
    python tools/export_surveys.py ... --dest /tmp/exports

Zip: <dest>/<survey id>_<UPC>.zip, paths inside as in output/ (bronze/nsv/zone=.../<UPC>/..., silver/..., gold/...).
The UPC / zone / phase / cycle come from the processed log, else from output/metadata/<survey id>.json. Taken:
<layer>/zone=<z>/phase=<p>/cycle=<c>/<UPC> and <layer>/report=<survey id>/<UPC> under output/bronze/nsv, silver,
gold and logs (all run folders) - never the folders of another survey of the same UPC.
"""
import argparse
import json
import re
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMAGES = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
LAYERS = ["bronze/nsv", "silver", "gold", "logs"]


def survey_info(out, sid, state):
    """{'upc', 'zone', 'phase', 'cycle'} from the processed log, else from output/metadata/<survey id>.json."""
    info = {k: state.get(sid, {}).get(k) for k in ("upc", "zone", "phase", "cycle")}
    meta = out / "metadata" / f"{sid}.json"
    if meta.is_file() and not all(info.values()):
        text = meta.read_text(encoding="utf-8", errors="replace")
        for k in info:
            m = re.search(rf"[\"']{k}[\"']\s*:\s*[\"']([^\"']+)[\"']", text)
            if m and not info[k]:
                info[k] = m.group(1)
    return info


def main(args):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("surveys", nargs="*")
    ap.add_argument("--output", default=str(ROOT / "output"))
    ap.add_argument("--dest", default=str(ROOT / "output" / "exports"))
    ap.add_argument("--no-images", action="store_true")
    o = ap.parse_args(args)
    out, dest = Path(o.output), Path(o.dest)
    dest.mkdir(parents=True, exist_ok=True)

    state_file = out / "state" / "wfm_trigger_state.json"
    state = json.loads(state_file.read_text(encoding="utf-8")) if state_file.is_file() else {}
    surveys = o.surveys or list(state)
    if not surveys:
        print("no surveys given and none in the processed log")
        return 1

    failed = 0
    for sid in surveys:
        info = survey_info(out, sid, state)
        upc = info["upc"]
        if not upc:
            print(f"{sid}: UPC unknown (not in the processed log, no output/metadata/{sid}.json) - skipped")
            failed += 1
            continue
        folder = upc.replace("/", "-")
        parts = [f"report={sid}/{folder}"]
        if info["zone"] and info["phase"] and info["cycle"]:
            parts.insert(0, f"zone={str(info['zone']).upper()}/phase={info['phase']}/cycle={info['cycle']}/{folder}")
        dirs = [out / layer / p for layer in LAYERS for p in parts if (out / layer / p).is_dir()]
        extra = [f for f in (out / "metadata" / f"{sid}.json", out / "metadata" / f"{sid}_migration.json",
                             state_file, out / "state" / "processed_surveys.csv") if f.is_file()]
        if not dirs:
            print(f"{sid}: no output folders for UPC {upc} under {out} - skipped")
            failed += 1
            continue

        zip_path = dest / f"{sid}_{folder}.zip"
        n = skipped = 0
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
            for d in dirs:
                for f in sorted(d.rglob("*")):
                    if not f.is_file():
                        continue
                    if o.no_images and f.suffix.lower() in IMAGES:
                        skipped += 1
                        continue
                    z.write(f, f.relative_to(out).as_posix())
                    n += 1
            for f in extra:
                z.write(f, f.relative_to(out).as_posix())
                n += 1
        size = zip_path.stat().st_size / 1e6
        print(f"{sid}  UPC {upc}  {n} files" + (f" ({skipped} images left out)" if skipped else "")
              + f"  {size:.1f} MB  ->  {zip_path}")
        for d in dirs:
            print(f"    {d.relative_to(out).as_posix()}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
