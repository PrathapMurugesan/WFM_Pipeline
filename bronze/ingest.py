"""
ingest.py - ingest ONE deliverable of ONE survey into bronze, driven only by schema/nsv_schema.json.

    manifest = ingest.run(survey, "d2")

Steps, all from the deliverable's entry in the schema:
    1. documents      PDFs by name pattern, sha256 in the manifest                   (nsv/inventory.py)
       folders        Contract Agreement / Videos: file list only                    (nsv/inventory.py)
    2. D2 templates   "one_of": which template was delivered (Detailed / Single Pavement Type) - exactly one
       workbooks      find -> sheets -> header gate -> parquet + csv + meta JSON     (nsv/pipeline.py)
    4. gis            D3 shapefile sets + GPKG layers -> tables with WKT geometry     (nsv/gis.py)
    5. image folders  D4 crack / pothole images -> one row per image (EXIF + OCR)     (nsv/images.py)
    6. inventory      one row per raw file + what it was accepted as; the rest = NOT_IN_SCHEMA
Output: the same bronze layout as the NSV e2e pipeline (bronze/nsv/zone=/phase=/cycle=/<UPC>/<dN>/run=<ts>/).
"""
import hashlib
from pathlib import PurePosixPath

import settings
from nsv import schema
from nsv.excel import ExcelWorkbook
from nsv.gis import extract_gis
from nsv.inventory import check_documents, check_folders, write_inventory
from nsv.log import get_logger
from nsv.matching import identify_names, match_score, normalize_name
from nsv.pipeline import (_excel_files, extract_image_folders, extract_workbook_variants, extract_workbook,
                          find_workbooks)
from nsv.run import Run

logger = get_logger("nsv.ingest")


def run(survey, dkey):
    cfg = schema.deliverable(dkey)
    run = Run(dkey, survey)
    s = schema.load()
    run.manifest["schema"] = {"file": settings.SCHEMA_FILE.name, "name": s.get("schema_name"),
                              "version": s.get("schema_version"),
                              "sha256": hashlib.sha256(settings.SCHEMA_FILE.read_bytes()).hexdigest(),
                              "deliverable_title": cfg.get("title")}
    folder = run.input_folder
    check_nulls = cfg.get("check_nulls", False)

    # ---- 1. documents and inventory-only folders (first: recorded even when a workbook stops the run) ----
    if cfg.get("documents"):
        check_documents(run, folder, cfg["documents"])
    if cfg.get("folders"):
        check_folders(run, folder, cfg["folders"])

    # ---- 2. workbooks (0. D2 template) ----
    files = {}
    workbooks = _resolve_templates(run, folder, cfg.get("workbooks", {}))
    if workbooks:
        files = find_workbooks(run, folder, workbooks)
        if files is None:
            write_inventory(run, folder)
            return run.finish(status="rejected_missing_workbook")
        for wb_key in workbooks:
            if wb_key in files or run.manifest["workbooks"].get(wb_key, {}).get("status") == "split_into_variants":
                extract_workbook(run, files, wb_key, workbooks, check_nulls=check_nulls)
        extract_workbook_variants(run, check_nulls=check_nulls)      # several files for one workbook only

    # ---- 4. GIS (D3) ----
    if cfg.get("gis"):
        extract_gis(run, folder, cfg["gis"])

    # ---- 5. images (D4) ----
    if cfg.get("image_folders"):
        location = cfg.get("image_location")
        extract_image_folders(run, folder, cfg["image_folders"], files, location)

    # ---- 6. every file: accepted as what / not in the schema ----
    write_inventory(run, folder)
    return run.finish()


# =============================================================================
# "one_of": exactly one of several templates (instruction sheet point 3: D2 Detailed OR Single Pavement Type)
# =============================================================================
def _resolve_templates(run, folder, workbooks):
    """
    Returns the workbooks to look for, with every "one_of" entry replaced by the template that was delivered:
      one template found  -> extracted under the workbook key ('comparison'), template recorded in the manifest
      none found          -> the workbook is reported missing (all template names listed)
      several found       -> PROBLEM (the instruction allows one); each is extracted as '<key>__<template>'
    """
    resolved = {}
    for wb_key, wb in workbooks.items():
        if "one_of" not in wb:
            resolved[wb_key] = wb
            continue
        found = _detect_templates(folder, wb["one_of"])
        info = run.manifest.setdefault("templates", {})[wb_key] = {
            "allowed": list(wb["one_of"]), "found": {t: PurePosixPath(p).name for t, p in found.items()}}
        if len(found) == 1:
            (template, path), = found.items()
            resolved[wb_key] = {**wb["one_of"][template], "file_names": [PurePosixPath(path).name],
                                "required": wb.get("required", True)}
            info["used"] = template
            logger.info(f"[{wb_key}] template '{template}': {PurePosixPath(path).name}")
        elif not found:
            names = [n for t in wb["one_of"].values() for n in t["file_names"]]
            first = next(iter(wb["one_of"].values()))
            resolved[wb_key] = {**first, "file_names": names, "file_keywords": [], "identify_by_sheets": [],
                                "required": wb.get("required", True)}
            logger.error(f"[{wb_key}] none of the templates {list(wb['one_of'])} found")
        else:
            problem = (f"{wb_key}: {len(found)} templates delivered {info['found']} - the instruction sheet allows "
                       f"exactly one; each is extracted as '{wb_key}__<template>'")
            run.manifest.setdefault("template_problems", []).append(problem)
            logger.error(f"[{wb_key}] {problem}")
            for template, path in found.items():
                resolved[f"{wb_key}__{template}"] = {**wb["one_of"][template], "file_names": [PurePosixPath(path).name],
                                                     "required": True}
    return resolved


def _detect_templates(folder, templates):
    """{template key: file path} of the templates present. By file name first; a file whose name fits no
    template but has a template's 'identify_by_sheets' (and its 'file_keywords' in the name) counts too."""
    paths = {PurePosixPath(p).name: p for p in _excel_files(folder)}
    found, missing, left = identify_names(list(paths), {k: t["file_names"] for k, t in templates.items()},
                                          settings.FUZZY_THRESHOLD, label="template", quiet=True)
    result = {k: paths[n] for k, n in found.items()}
    for key in missing:                                   # renamed file: recognise it by its sheets
        t = templates[key]
        keywords = [normalize_name(k, is_file=False) for k in t.get("file_keywords", [])]
        for name in list(left):
            if keywords and not all(k in normalize_name(name) for k in keywords):
                continue
            try:
                wb = ExcelWorkbook(name, settings.INPUT.read_bytes(paths[name]))
                sheets = wb.sheet_names
                wb.close()
            except Exception:
                continue
            wanted = t.get("identify_by_sheets") or []
            if wanted and all(any(match_score(s, [w], is_file=False) >= 0.999 for s in sheets) for w in wanted):
                result[key] = paths[name]
                left.remove(name)
                logger.warning(f"[template {key}] '{name}' recognised by its sheets {wanted}")
                break
    return result
