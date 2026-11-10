"""
build_schema.py - fill the expected headers / fields of schema/nsv_schema.json from a known-good delivery.

    python tools/build_schema.py "../NSV Data New"            refresh every headers / fields / csv_headers list
    python tools/build_schema.py "../NSV Data New" --check    only show what would change, write nothing

The folder holds 'Deliverable 1 - RAMS Data' .. 'Deliverable 4 - ...' (as in the NHAI instruction sheet).
Everything else in the schema (files, sheets, header layout, required / optional, notes) is NOT touched:
edit those by hand. Aliases typed into a header are kept. Each sheet / layer is read exactly as the
ingestion reads it (same header options, same code), so the schema always matches what is extracted.
A sheet / layer / file that is not in the folder keeps its current list (reported at the end).
ALWAYS review the diff (git diff schema/nsv_schema.json) before using it: it becomes the gate.
"""
import hashlib
import json
import re
import sys
import tempfile
from datetime import date
from pathlib import Path, PurePosixPath

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import settings
from nsv import gis, schema
from openpyxl.utils import column_index_from_string

from nsv.excel import ExcelWorkbook
from nsv.headers import value_type
from nsv.matching import identify_names
from nsv.storage import DELIVERABLE_FOLDER as DELIVERABLE, LocalStorage


def _norm(text):
    return re.sub(r"[^a-z0-9%]", "", str(text).lower())


def _merge(old, new_names_types, letters=None):
    """New header list; aliases are kept. A header already in the schema under a spelling that differs only in
    case / spaces / punctuation ('NH Number' vs 'NHNumber') keeps the schema name (= the saved column name, as in
    the previous NSV bronze) and gets the file's spelling as an alias."""
    aliases = {h["name"]: h.get("aliases", []) for h in old or []}
    canonical = {_norm(h["name"]): h["name"] for h in old or []}
    out = []
    for i, (name, kind) in enumerate(new_names_types):
        keep = canonical.get(_norm(name))
        if keep and keep != name:
            aliases.setdefault(keep, [])
            if name not in aliases[keep]:
                aliases[keep] = aliases[keep] + [name]
            name = keep
        h = {"name": name}
        if letters:
            h["excel_column"] = letters[i]
        h["value_type"] = kind
        h["aliases"] = aliases.get(name, [])
        out.append(h)
    return out


def _changed(label, old, new, report):
    before = [h["name"] for h in old or []]
    after = [h["name"] for h in new]
    if before != after:
        gone, added = [n for n in before if n not in after], [n for n in after if n not in before]
        report.append(f"{label}: {len(before)} -> {len(after)} headers"
                      + (f", removed {gone[:5]}" if gone else "") + (f", added {added[:5]}" if added else ""))


def build_workbooks(dkey, folder, workbooks, files_seen, report, missing):
    excel = {PurePosixPath(p).name: p for p in settings.INPUT.list_files(folder)
             if p.lower().endswith((".xlsx", ".xlsm", ".xls")) and not PurePosixPath(p).name.startswith("~$")}
    for wb_key, wb in workbooks.items():
        for t_key, template in (wb.get("one_of") or {wb_key: wb}).items():
            label = f"{dkey}/{wb_key}" + (f"/{t_key}" if "one_of" in wb else "")
            found, _, _ = identify_names(list(excel), {t_key: template["file_names"]}, settings.FUZZY_THRESHOLD,
                                         quiet=True)
            if t_key not in found:
                missing.append(f"{label}: no file like {template['file_names']}")
                continue
            path = excel[found[t_key]]
            data = settings.INPUT.read_bytes(path)
            files_seen[path] = hashlib.sha256(data).hexdigest()
            book = ExcelWorkbook(found[t_key], data)
            for s_key, sheet in template["sheets"].items():
                if sheet.get("skip") or sheet.get("kind") == "cells":
                    continue
                got, _, _ = identify_names(book.sheet_names, {s_key: sheet["sheet_names"]}, settings.FUZZY_THRESHOLD,
                                           is_file=False, quiet=True)
                if s_key not in got:
                    missing.append(f"{label}/{s_key}: no sheet like {sheet['sheet_names']} in '{found[t_key]}'")
                    continue
                table = book.read_table(got[s_key], sheet)
                keep = range(len(table.columns))
                if sheet.get("header_gate_columns"):          # only these Excel columns become the gate
                    lo, hi = (column_index_from_string(x) for x in sheet["header_gate_columns"].split(":"))
                    keep = [i for i, c in enumerate(table.columns) if lo <= column_index_from_string(c["letter"]) <= hi]
                kinds = [value_type([row[i] for row in table.rows]) for i in keep]
                new = _merge(sheet.get("headers"), list(zip([table.names[i] for i in keep], kinds)),
                             [table.columns[i]["letter"] for i in keep])
                _changed(f"{label}/{s_key}", sheet.get("headers"), new, report)
                sheet["headers"] = new
            book.close()


def _fields(df):
    cols = [c for c in df.columns if c not in ("geometry_type", "geometry_wkt")]
    return [(c, value_type(df[c].tolist())) for c in cols]


def build_gis(dkey, folder, gis_cfg, files_seen, report, missing):
    files = settings.INPUT.list_files(folder)
    for std_key, std in gis_cfg.items():
        std_folder = gis._sub_folder(files, folder, std["folder_names"])
        if not std_folder:
            missing.append(f"{dkey}/gis/{std_key}: no folder like {std['folder_names']}")
            continue
        std_files = gis._below(files, std_folder)
        shp = std.get("shapefiles")
        shp_folder = gis._sub_folder(std_files, std_folder, shp["folder_names"]) if shp else None
        for key, layer in (shp or {}).get("layers", {}).items():
            label = f"{dkey}/gis/{std_key}/shp/{key}"
            lf = gis._sub_folder(gis._below(std_files, shp_folder), shp_folder, layer["folder_names"]) if shp_folder else None
            here = [p for p in std_files if lf and PurePosixPath(p).parent.as_posix() == lf]
            shp_path = next((p for p in here if p.lower().endswith(".shp")), None)
            if not shp_path:
                missing.append(f"{label}: no shapefile")
                continue
            stem = PurePosixPath(shp_path).stem
            parts = [p for p in here if PurePosixPath(p).stem == stem]
            for p in parts:
                files_seen[p] = hashlib.sha256(settings.INPUT.read_bytes(p)).hexdigest()
            with tempfile.TemporaryDirectory() as tmp:
                local = gis._local_copy([p for p in parts if not p.lower().endswith(".csv")], tmp)
                df, _ = gis.read_layer(local[PurePosixPath(shp_path).name])
            new = _merge(layer.get("fields"), _fields(df))
            _changed(label, layer.get("fields"), new, report)
            layer["fields"] = new
            csv_path = next((p for p in parts if p.lower().endswith(".csv")), None)
            if csv_path:
                df = gis.read_csv_bytes(settings.INPUT.read_bytes(csv_path))
                new = _merge(layer.get("csv_headers"), [(c, value_type(df[c].tolist())) for c in df.columns])
                _changed(f"{label}/csv", layer.get("csv_headers"), new, report)
                layer["csv_headers"] = new

        gp = std.get("gpkg")
        gpkg_files = {PurePosixPath(p).name: p for p in std_files if p.lower().endswith(".gpkg")}
        for f_key, fcfg in (gp or {}).get("files", {}).items():
            found, _, _ = identify_names(list(gpkg_files), {f_key: fcfg["file_names"]}, settings.FUZZY_THRESHOLD,
                                         quiet=True)
            if f_key not in found:
                missing.append(f"{dkey}/gis/{std_key}/gpkg/{f_key}: no file like {fcfg['file_names']}")
                continue
            path = gpkg_files[found[f_key]]
            files_seen[path] = hashlib.sha256(settings.INPUT.read_bytes(path)).hexdigest()
            ignore = set(fcfg.get("ignore_layers", gis.IGNORE_LAYERS_DEFAULT))
            with tempfile.TemporaryDirectory() as tmp:
                local = gis._local_copy([path], tmp)[found[f_key]]
                seen = set()
                for layer_name, _ in gis.list_layers(local):
                    if layer_name in ignore:
                        continue
                    lkey = gis.layer_key_for(layer_name, fcfg["layers"])
                    if lkey is None or lkey in seen:
                        report.append(f"{dkey}/gis/{std_key}/gpkg/{f_key}: layer '{layer_name}' fits no schema layer "
                                      f"- add it to the schema by hand if it belongs there")
                        continue
                    seen.add(lkey)
                    df, _ = gis.read_layer(local, layer=layer_name)
                    lcfg = fcfg["layers"][lkey]
                    new = _merge(lcfg.get("fields"), _fields(df))
                    _changed(f"{dkey}/gis/{std_key}/gpkg/{f_key}/{lkey}", lcfg.get("fields"), new, report)
                    lcfg["fields"] = new
                for lkey in fcfg["layers"]:
                    if lkey not in seen:
                        missing.append(f"{dkey}/gis/{std_key}/gpkg/{f_key}/{lkey}: no layer in '{found[f_key]}'")


def main(args):
    if not args or args[0].startswith("-"):
        print(__doc__)
        return 1
    root = Path(args[0]).resolve()
    check_only = "--check" in args
    settings.INPUT = LocalStorage(root)
    s = json.loads(settings.SCHEMA_FILE.read_text(encoding="utf-8"))

    folders = {f"d{DELIVERABLE.match(p.name).group(1)}": p.name for p in sorted(root.iterdir())
               if p.is_dir() and DELIVERABLE.match(p.name)}
    report, missing, files_seen = [], [], {}
    for dkey, d in s["deliverables"].items():
        if dkey not in folders:
            missing.append(f"{dkey}: no 'Deliverable {dkey[1:]} ...' folder in {root}")
            continue
        if d.get("workbooks"):
            build_workbooks(dkey, folders[dkey], d["workbooks"], files_seen, report, missing)
        if d.get("gis"):
            build_gis(dkey, folders[dkey], d["gis"], files_seen, report, missing)

    s["built_from"] = {"folder": root.name, "built_on": date.today().isoformat(),
                       "files": dict(sorted(files_seen.items()))}
    problems = schema.validate(s)
    print("\n".join(["changes:"] + [f"  {r}" for r in report] if report else ["no header changes"]))
    if missing:
        print("\nnot in the folder (lists kept as they are):\n" + "\n".join(f"  {m}" for m in missing))
    if problems:
        print("\nschema problems:\n" + "\n".join(f"  {p}" for p in problems))
        return 1
    if check_only:
        print("\n--check: nothing written")
        return 0
    settings.SCHEMA_FILE.write_text(json.dumps(s, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"\nwritten: {settings.SCHEMA_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
