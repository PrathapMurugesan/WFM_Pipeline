"""
main.py - check and ingest ONE uploaded report (one UPC) into bronze, against schema/nsv_schema.json.

    python main.py 3433cb89-f583-438c-ab44-6e99455e1bf4                     a WFM report id (UPC found in the data)
              = s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/3433cb89-.../   (settings.REPORTS_S3_ROOT)
    python main.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --upc N/02001/19004/DL   the UPC, when the data has several
    python main.py s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/3433cb89-.../
    python main.py "<local folder>" --report-id <id>                        a local copy
    ... --zone A --phase 2 --cycle 2                                       optional: the earlier zone/phase/cycle layout
    ... d2 d4                                                                     only these deliverables

Output: bronze/nsv/report=<report id>/<UPC>/d1..d4/run=<time>/  (zone/phase/cycle given: bronze/nsv/zone=/phase=/cycle=/<UPC>/)

The report holds the deliverable folders D1..D4 (or 'Deliverable 1 - RAMS Data' ...), directly or one level down.
File names may carry the upload timestamp ('1793260589326-LRMS_DataTemplate.xlsx'): it is removed before any
check; the same file uploaded twice -> the newest is used, the older one is listed (superseded_upload).
UPC: --upc, else the folder name if it is a UPC, else the one UPC written in the data (UPC columns of D1 / D4).

Every file and part is checked against the schema; what is described is extracted to bronze, everything else is
reported. Output (default ./data, or NSV_OUTPUT=<folder>):
    bronze/nsv/zone=A/phase=2/cycle=2/<UPC>/d1..d4/run=<timestamp>/   tables, meta JSON, inventory, manifest, log
    bronze/nsv/_batches/batch=<timestamp>_report.csv                    ONE ROW PER ITEM, problems in UPPER CASE
Exit code: 0 = the upload matches the schema, 2 = problems (see the report), 1 = could not run.
"""
import argparse
import io
import re
import warnings
import sys
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path

import pandas as pd

import ingest
import settings
from nsv import schema
from nsv.log import get_logger
from nsv.report import COLUMNS, PROBLEMS, report_rows
from nsv.storage import ReportStorage
from nsv.survey import Survey, upc_folder
from nsv.writer import csv_bytes, json_bytes

logger = get_logger("nsv")


def parse_args(args):
    parser = argparse.ArgumentParser(description="NSV upload check + bronze ingestion (schema/nsv_schema.json)",
                                     formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    parser.add_argument("source", help="report id, s3:// report folder, or local folder")
    parser.add_argument("deliverables", nargs="*", help="d1 d2 d3 d4 (default: all)")
    parser.add_argument("--report-id", help="report id for a local folder whose name is not the report id")
    parser.add_argument("--zone", help="optional (with --phase and --cycle): the earlier zone/phase/cycle layout")
    parser.add_argument("--phase")
    parser.add_argument("--cycle")
    parser.add_argument("--upc", help="e.g. N/02001/19004/DL (default: folder name, else the UPC in the data)")
    o = parser.parse_args(args)
    given = [x for x in (o.zone, o.phase, o.cycle) if x]
    if given and len(given) != 3:
        parser.error("--zone, --phase and --cycle go together (or leave all three out)")
    unknown = [d for d in o.deliverables if d not in schema.deliverable_keys()]
    if unknown:
        parser.error(f"unknown deliverable(s) {unknown}, choose from {schema.deliverable_keys()}")
    return o


REPORT_ID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)


def _last_part(location):
    return location.replace("\\", "/").rstrip("/").split("/")[-1]


def resolve_source(source):
    """(location to open, report id or None): a bare report id -> settings.REPORTS_S3_ROOT/<id>/."""
    source = source.strip()
    if REPORT_ID.match(source):
        return settings.REPORTS_S3_ROOT.rstrip("/") + f"/{source}/", source
    name = _last_part(source)
    return source, (name if REPORT_ID.match(name) else None)


def upc_in_data(upload):
    """The distinct values of 'UPC' columns in the D1 / D4 workbooks (first 200 rows of every sheet)."""
    import openpyxl
    found = set()
    for path in upload.list_files("d1") + upload.list_files("d4"):
        if not path.lower().endswith((".xlsx", ".xlsm")):
            continue
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                wb = openpyxl.load_workbook(io.BytesIO(upload.read_bytes(path)), read_only=True, data_only=True)
            for ws in wb.worksheets:
                col = None
                for row in ws.iter_rows(max_row=200, values_only=True):
                    if col is None:
                        cells = [str(v).strip().lower() if v is not None else "" for v in row]
                        col = cells.index("upc") if "upc" in cells else None
                    elif col < len(row) and row[col] and upc_folder(str(row[col])):
                        found.add(upc_folder(str(row[col])))
            wb.close()
        except Exception as e:
            logger.warning(f"UPC scan: cannot read '{path}': {e}")
    return sorted(found)


def _root_rows(survey, upload, base):
    """Files / folders directly in the UPC folder, outside the deliverable folders: allowed by the schema or not."""
    patterns = schema.load().get("upload_root", {}).get("allowed_files", [])
    rows = []
    for name in upload.other:
        if any(fnmatch(name.lower(), p.lower()) for p in settings.IGNORE_FILES):
            continue
        allowed = any(fnmatch(name.lower(), p.lower()) for p in patterns)
        rows.append({**base, "deliverable": "upload", "file": name,
                     "status": "root_file_allowed" if allowed else "NOT_IN_SCHEMA",
                     "reason": "" if allowed else "not a deliverable folder and not an allowed file - not extracted"})
    for key, name in upload.duplicates:
        rows.append({**base, "deliverable": key, "file": name, "status": "NOT_IN_SCHEMA",
                     "reason": f"a second folder for {key} - only '{upload.folders[key]}' was read"})
    for z in getattr(upload, "zips", []):
        if z.get("error"):
            rows.append({**base, "deliverable": z["zip"].split("/")[0], "file": z["uploaded"], "status": "READ_ERROR",
                         "reason": f"zip could not be opened: {z['error']}"})
        else:
            rows.append({**base, "deliverable": z["zip"].split("/")[0], "file": z["uploaded"], "status": "zip_opened",
                         "rows": z["files"], "reason": f"read as folder '{z['folder']}'"})
    for clean, raw in upload.superseded:
        rows.append({**base, "deliverable": clean.split("/")[0], "file": raw, "status": "superseded_upload",
                     "reason": f"older upload of '{clean}' - the newest one was used"})
    return rows


def _upc_rows(survey, manifests, base):
    """UPC columns in the data must hold the uploaded UPC."""
    rows, wanted = [], re.sub(r"[/_\-\s]", "", survey.upc).upper()
    for dkey, m in manifests.items():
        for wb_key, wb in m.get("workbooks", {}).items():
            for sheet_key, result in wb.get("sheets", {}).items():
                other = [v for v in result.get("upc_values", []) if re.sub(r"[/_\-\s]", "", v).upper() != wanted]
                if other:
                    rows.append({**base, "deliverable": dkey, "workbook": wb_key, "file": wb.get("file") or "",
                                 "sheet": result.get("sheet_name") or sheet_key, "status": "UPC_MISMATCH",
                                 "reason": f"UPC column holds {other[:3]}, uploaded as {survey.upc}"})
    return rows


def main(args):
    o = parse_args(args)
    location, report_id = resolve_source(o.source)
    report_id = o.report_id or report_id
    if not report_id and not o.zone:
        logger.error("no report id: give a WFM report id / its s3:// folder, or --report-id for a local folder")
        return 1
    if not location.startswith("s3://") and not Path(location).is_dir():
        logger.error(f"not a report id, s3:// folder or local folder: {o.source}")
        return 1
    try:
        upload = ReportStorage.open(location)
    except Exception as e:
        logger.error(f"cannot open {location}: {e}")
        return 1
    settings.INPUT = upload
    if not upload.folders:
        logger.error(f"no deliverable folder (D1..D4 / 'Deliverable N ...') in {upload.describe()}")
        return 1

    upc = upc_folder(o.upc) if o.upc else upc_folder(_last_part(location))
    if not upc and not o.upc:
        found = upc_in_data(upload)
        if len(found) == 1:
            upc = found[0]
            logger.info(f"UPC from the data: {upc.replace('-', '/')}")
        else:
            logger.error(f"give the UPC with --upc: the data holds {len(found)} UPC(s) {found}")
            return 1
    if not upc:
        logger.error(f"'{o.upc}' is not a UPC (like N/02001/19004/DL)")
        return 1
    settings.UPLOAD = {"report_id": report_id, "source": upload.describe(),
                       "deliverable_folders": dict(upload.folders), "superseded_uploads": len(upload.superseded),
                       "zips": getattr(upload, "zips", [])}
    survey = Survey(upc, report_id=report_id, zone=o.zone.upper() if o.zone else None,
                    phase=str(o.phase) if o.phase else None, cycle=str(o.cycle) if o.cycle else None)
    wanted = o.deliverables or schema.deliverable_keys()
    logger.info(f"upload {upload.describe()} (report {report_id or '-'}) -> {survey.label}; "
                f"deliverable folders: {upload.folders}")
    for key, raw in upload.folders.items():
        expected = schema.deliverable(key).get("folder_names", []) if key in schema.deliverable_keys() else []
        if expected and raw.split("/")[-1] not in expected:
            logger.warning(f"{key}: folder '{raw}' is not named as in the instruction sheet {expected}")

    # ---- every deliverable ----
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    results, report, manifests = [], [], {}
    for name in wanted:
        row = {**survey.as_dict(), "deliverable": name}
        if name not in upload.folders:
            logger.error(f"[{survey.label}] {name}: no {name.upper()} / 'Deliverable {name[1:]} ...' folder in the upload")
            results.append({**row, "status": "not_delivered"})
            report += report_rows(survey, name, "not_delivered")
            continue
        try:
            manifest = manifests[name] = ingest.run(survey, name)
            results.append({**row, "status": manifest["status"], "run_id": manifest["run_id"],
                            "output": manifest["output"], "summary": manifest["summary"]})
            report += report_rows(survey, name, manifest["status"], manifest)
            if manifest["status"] == "crashed":
                report += report_rows(survey, name, "crashed", error=manifest.get("error"))
        except Exception as e:
            logger.exception(f"[{survey.label}] {name}: crashed")
            results.append({**row, "status": "crashed", "error": str(e)})
            report += report_rows(survey, name, "crashed", error=str(e))

    # ---- the upload as a whole: files outside the deliverable folders, UPC in the data ----
    base = {c: "" for c in COLUMNS} | {"report_id": survey.report_id or "", "zone": survey.zone or "",
                                      "phase": survey.phase or "", "cycle": survey.cycle or "", "upc": survey.upc}
    report += _root_rows(survey, upload, base) + _upc_rows(survey, manifests, base)

    # ---- batch summary + report ----
    totals = {}
    for r in results:
        totals[r["status"]] = totals.get(r["status"], 0) + 1
    problems = [r for r in report if r["status"] in PROBLEMS]
    batch = {"batch_id": batch_id, "finished_at": datetime.now().isoformat(timespec="seconds"),
             "upload": settings.UPLOAD, "survey": survey.as_dict(), "deliverables": wanted,
             "accepted": not problems, "problems": len(problems), "totals": totals, "results": results}
    path = f"{settings.BATCH_FOLDER}/batch={batch_id}.json"
    settings.OUTPUT.write_bytes(path, json_bytes(batch))
    report_path = f"{settings.BATCH_FOLDER}/batch={batch_id}_report.csv"
    settings.OUTPUT.write_bytes(report_path, csv_bytes(pd.DataFrame(report, columns=COLUMNS)))

    logger.info("################ summary ################")
    for r in results:
        logger.info(f"  {survey.label}  {r['deliverable']}  {r['status']}")
    logger.info(f"################ problems: {len(problems)} ################")
    for r in problems:
        what = " / ".join(x for x in (r.get("workbook"), r.get("file"), r.get("sheet")) if x)
        logger.warning(f"  {r['status']:19} {r['deliverable']}  {what}  -> {r.get('reason')}")
    optional = [r for r in report if r["status"] == "optional_missing"]
    if optional:
        logger.info(f"################ optional, not delivered: {len(optional)} ################")
        for r in optional:
            logger.info(f"  {r['deliverable']}  {r.get('workbook')} {r.get('sheet')}")
    logger.info(f"UPLOAD {'ACCEPTED' if not problems else 'NOT ACCEPTED'} - report: "
                f"{settings.OUTPUT.describe()}/{report_path}")
    return 0 if not problems else 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
