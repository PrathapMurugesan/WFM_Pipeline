"""
schema.py - read the ONE schema file (settings.SCHEMA_FILE = schema/nsv_schema.json).

The schema describes everything a valid NSV delivery (D1-D4, NHAI instruction sheet of 6 Aug 2026) may
contain - and therefore everything that is accepted:

    deliverables.<dN>.folder_names        vendor folder names ('Deliverable 1 - RAMS Data')
    deliverables.<dN>.workbooks           Excel files -> sheets -> header layout + expected headers
                 "one_of": {...}           exactly one of several templates (D2 Detailed / Single Pavement Type)
    deliverables.<dN>.documents           PDFs: file name pattern ('NSV Report of *.pdf'), required or not
    deliverables.<dN>.folders             folders kept as an inventory only (Contract Agreement, Videos)
    deliverables.<dN>.gis                 D3: per standard (CA / IRC) the shapefile sets and GPKG files + fields
    deliverables.<dN>.image_folders       D4: crack / pothole image folders

    sheet  "headers": [{"name", "excel_column", "value_type", "aliases": [...]}]   = the header gate
    layer  "fields":  [same]                                                       = the field gate

Filled / refreshed from a known-good delivery with  python tools/build_schema.py "<template folder>".
"""
import json
from functools import lru_cache

import settings

DOC_KEYS = ("title", "folder_names", "workbooks", "documents", "folders", "gis", "image_folders",
            "image_location", "check_nulls")


@lru_cache(maxsize=1)
def load():
    """The schema as a dict (read once)."""
    with open(settings.SCHEMA_FILE, encoding="utf-8") as f:
        schema = json.load(f)
    problems = validate(schema)
    if problems:
        raise ValueError(f"schema {settings.SCHEMA_FILE} is not valid: " + "; ".join(problems))
    return schema


def deliverable(dkey):
    return load()["deliverables"][dkey]


def deliverable_keys():
    return list(load()["deliverables"])


def validate(schema):
    """Structural problems of the schema file (empty list = fine)."""
    problems = []
    for dkey, d in schema.get("deliverables", {}).items():
        for key in d:
            if key not in DOC_KEYS and not key.startswith("_"):
                problems.append(f"{dkey}: unknown key '{key}'")
        for wb_key, wb in d.get("workbooks", {}).items():
            for template_key, template in (wb.get("one_of") or {wb_key: wb}).items():
                if not template.get("file_names"):
                    problems.append(f"{dkey}/{wb_key}/{template_key}: no file_names")
                for sheet_key, sheet in template.get("sheets", {}).items():
                    if not sheet.get("sheet_names"):
                        problems.append(f"{dkey}/{wb_key}/{template_key}/{sheet_key}: no sheet_names")
                    for h in sheet.get("headers", []):
                        if not h.get("name"):
                            problems.append(f"{dkey}/{wb_key}/{template_key}/{sheet_key}: header without name")
    return problems


def header_spec(headers):
    """
    Schema headers -> (names, {name: [aliases]}, {name: {"excel_column", "value_type"}}) as the header
    matcher (nsv/headers.py) wants them; None when the schema lists no headers for this sheet.
    """
    if not headers:
        return None
    names, aliases, info = [], {}, {}
    for h in headers:
        name = str(h["name"]).strip()
        names.append(name)
        if h.get("aliases"):
            aliases[name] = [str(a).strip() for a in h["aliases"] if str(a).strip()]
        info[name] = {"excel_column": h.get("excel_column") or "", "value_type": h.get("value_type") or ""}
    return names, aliases, info
