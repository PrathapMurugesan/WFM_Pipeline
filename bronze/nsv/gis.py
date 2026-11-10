"""
gis.py - D3 GIS Mapping: shapefile sets and GeoPackage layers -> bronze tables.

Layout (instruction sheet, D3):
    GIS Mapping -As Per CA / -As per IRC          one folder per standard           (schema: gis.<std>)
        GIS Shape Files/<Layer>/<Layer>.cpg csv dbf prj qmd shp shx                (gis.<std>.shapefiles.layers)
        GPKG Files/<Name>-GIS.gpkg  -> one or more layers inside                   (gis.<std>.gpkg.files)

What is saved (every value as text, exactly as delivered, like the Excel sheets):
    d3_<std>_shp_<layer>.parquet + .csv           attributes of the .dbf + geometry_type + geometry_wkt
    d3_<std>_shp_<layer>_csv.parquet + .csv       the companion .csv (full column names; the .dbf cuts them at 10)
    d3_<std>_gpkg_<file>_<layer>.parquet + .csv   one table per GeoPackage layer
    meta/d3_<std>_schema.json / _profile.json     field check + profile per table; crs / geometry type per layer

Gates (same as Excel headers): a layer missing a schema field is REJECTED (not saved); a required layer / file /
shapefile part that is missing is a problem; a GPKG layer the schema does not describe is not extracted.
Needs: pip install pyogrio shapely   (GDAL is bundled in the pyogrio wheel)
"""
import io
import re
import tempfile
from pathlib import Path, PurePosixPath

import pandas as pd

import settings
from nsv.checks import check_headers, empty_sheet_report, load_schema
from nsv.headers import final_names, match_headers
from nsv.log import get_logger
from nsv.matching import match_score, normalize_name, safe_name
from nsv.profiler import profile_table

logger = get_logger("nsv.gis")

IGNORE_LAYERS_DEFAULT = ["layer_styles"]          # QGIS styling table inside a GeoPackage, not survey data
LAYER_SEPARATORS = re.compile(r"\s*[–—\-|:]\s+|\s+[–—\-|:]\s*")   # 'Project – Cracking'


# =============================================================================
# Reading (also used by tools/build_schema.py)
# =============================================================================
def _require_pyogrio():
    try:
        import pyogrio  # noqa: F401
        import shapely  # noqa: F401
    except ImportError as e:
        raise RuntimeError(f"GIS files need 'pyogrio' and 'shapely' (pip install pyogrio shapely): {e}")


def _local_copy(paths, tmp):
    """Copy files from settings.INPUT into a temp folder; returns {name: local path}."""
    out = {}
    for p in paths:
        target = Path(tmp) / PurePosixPath(p).name
        target.write_bytes(settings.INPUT.read_bytes(p))
        out[PurePosixPath(p).name] = target
    return out


def read_layer(local_path, layer=None):
    """(DataFrame of attributes as delivered + geometry_type + geometry_wkt, info dict)."""
    _require_pyogrio()
    import pyogrio
    import shapely

    info = pyogrio.read_info(str(local_path), layer=layer)
    meta, _, geometry, values = pyogrio.raw.read(str(local_path), layer=layer)
    df = pd.DataFrame({name: col for name, col in zip(meta["fields"], values)})
    if geometry is not None:
        shapes = shapely.from_wkb(geometry)
        df["geometry_type"] = [g.geom_type if g is not None else None for g in shapes]
        df["geometry_wkt"] = [g.wkt if g is not None else None for g in shapes]
    facts = {"crs": info.get("crs"), "geometry_type": info.get("geometry_type"),
             "features": int(info.get("features") or len(df)), "encoding": info.get("encoding")}
    return df, facts


def list_layers(local_path):
    _require_pyogrio()
    import pyogrio
    return [(str(name), gtype) for name, gtype in pyogrio.list_layers(str(local_path))]


def read_csv_bytes(data):
    try:
        return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False, encoding="latin-1")


def layer_key_for(layer_name, layers_cfg):
    """Which schema layer a GeoPackage layer is: keywords tried on the LAST part of the name first
    ('Palsit-Dhankuni – Cracking' -> cracking), then on the whole name. None = not in the schema."""
    parts = [p for p in LAYER_SEPARATORS.split(str(layer_name)) if p.strip()]
    for text in ([parts[-1]] if parts else []) + [str(layer_name)]:
        norm = normalize_name(text, is_file=False)
        hits = [key for key, cfg in layers_cfg.items()
                if any(normalize_name(k, is_file=False) in norm for k in cfg.get("keywords", [key]))]
        if len(hits) == 1:
            return hits[0]
    return None


# =============================================================================
# Finding folders / files
# =============================================================================
def _sub_folder(files, base, names):
    """The first-level folder under base whose name matches one of names (exact / normalized / fuzzy)."""
    base = base.strip("/")
    found = sorted({PurePosixPath(p).relative_to(base).parts[0] for p in files
                    if len(PurePosixPath(p).relative_to(base).parts) > 1})
    scored = sorted(((match_score(f, names, is_file=False), f) for f in found), reverse=True)
    if scored and scored[0][0] >= settings.FUZZY_THRESHOLD:
        return f"{base}/{scored[0][1]}"
    return None


def _below(files, folder):
    return [p for p in files if p.startswith(folder.rstrip("/") + "/")]


# =============================================================================
# Extraction
# =============================================================================
def extract_gis(run, folder, gis_cfg):
    files = settings.INPUT.list_files(folder)
    for std_key, std in gis_cfg.items():
        std_folder = _sub_folder(files, folder, std["folder_names"])
        meta = {"schema": {}, "profile": {}}
        if not std_folder:
            for kind, items in (("shp", std.get("shapefiles", {}).get("layers", {})),
                                ("gpkg", std.get("gpkg", {}).get("files", {}))):
                for key, cfg in items.items():
                    _record(run, std_key, kind, key, status="missing" if cfg.get("required", True) else
                            "optional_missing", reason=f"no folder like {std['folder_names']}")
            logger.error(f"[{std_key}] no folder like {std['folder_names']} in '{folder}'")
            continue
        logger.info(f"---------- [{std_key}] {std_folder} ----------")
        std_files = _below(files, std_folder)
        if std.get("shapefiles"):
            _extract_shapefiles(run, std_key, std_folder, std_files, std["shapefiles"], meta)
        if std.get("gpkg"):
            _extract_gpkg(run, std_key, std_folder, std_files, std["gpkg"], meta)
        for name, content in meta.items():
            if content:
                run.save_json(content, std_key, name)


def _record(run, std_key, kind, key, **values):
    entry = run.manifest["gis"].setdefault(f"{std_key}/{kind}/{key}", {"standard": std_key, "kind": kind})
    entry.update(values)
    return entry


def _gate_and_save(run, df, expected_fields, label, out_parts, meta, entry, first_row=1):
    """Field check (the gate) + profile + save. Returns True when saved."""
    data_cols = [c for c in df.columns if c not in ("geometry_type", "geometry_wkt")]
    schema = load_schema(expected_fields)
    expected, aliases, info = schema if schema else ([], {}, {})
    matched = match_headers(data_cols, expected, aliases, info,
                            values=[df[c].tolist() for c in data_cols])
    if schema is None:
        logger.warning(f"[{label}] no fields in the schema - not checked")
        entry["header_check"] = "no schema file"
    else:
        entry["header_check"] = check_headers(matched, label, gate=settings.CHECK_HEADERS)
    columns = matched["columns"]
    names = final_names(columns)
    meta["schema"][label] = {"status": "accepted", "missing_headers": matched["missing"],
                             "columns": [{"column": n, **c} for n, c in zip(names, columns)]}
    if isinstance(entry["header_check"], dict) and not entry["header_check"]["ok"]:
        meta["schema"][label]["status"] = "rejected"
        entry["status"] = "rejected_headers"
        return False

    df = df.rename(columns=dict(zip(data_cols, names)))
    df.insert(0, "source_row", range(first_row, len(df) + first_row))
    profile_cols = [{"name": n, "vendor_header": c["vendor_header"]} for n, c in zip(names, columns)]
    profile_cols += [{"name": c, "vendor_header": c} for c in df.columns if c in ("geometry_type", "geometry_wkt")]
    meta["profile"][label] = profile_table(df, profile_cols, header_excel_rows=[first_row - 1] if first_row > 1 else [])
    entry["empty_check"] = empty_sheet_report(df, label)
    entry["outputs"] = run.save_table(df, *out_parts)
    entry.update(status="saved", rows=len(df), columns=len(names))
    logger.info(f"[{label}] saved {len(df)} rows -> {run.file_name(*out_parts)}.parquet + .csv")
    return True


def _extract_shapefiles(run, std_key, std_folder, std_files, cfg, meta):
    shp_folder = _sub_folder(std_files, std_folder, cfg["folder_names"])
    layer_files = _below(std_files, shp_folder) if shp_folder else []
    used_folders = set()
    for key, layer in cfg["layers"].items():
        label = f"{std_key}/shp/{key}"
        required = layer.get("required", True)
        layer_folder = _sub_folder(layer_files, shp_folder, layer["folder_names"]) if shp_folder else None
        if not layer_folder:
            _record(run, std_key, "shp", key, status="missing" if required else "optional_missing",
                    reason=f"no folder like {layer['folder_names']} in {cfg['folder_names']}")
            (logger.error if required else logger.info)(f"[{label}] shapefile folder not delivered")
            continue
        used_folders.add(layer_folder)
        here = [p for p in layer_files if PurePosixPath(p).parent.as_posix() == layer_folder]
        stems = {PurePosixPath(p).stem for p in here if p.lower().endswith(".shp")}
        stem = max(stems, key=lambda s: match_score(s, layer["file_names"]), default=None)
        parts = {PurePosixPath(p).suffix.lower(): p for p in here if PurePosixPath(p).stem == stem}
        lacking = [ext for ext in layer.get("parts", [".shp", ".shx", ".dbf"]) if ext not in parts]
        entry = _record(run, std_key, "shp", key, folder=layer_folder, file_stem=stem,
                        parts_found=sorted(parts), parts_missing=lacking)
        for p in parts.values():
            run.accept(p, f"gis {label}")
        if lacking:
            logger.error(f"[{label}] shapefile set '{stem}' lacks {lacking}")
        if ".shp" not in parts:
            entry.update(status="missing", reason=f"no .shp in '{layer_folder}'")
            continue
        try:
            with tempfile.TemporaryDirectory() as tmp:
                local = _local_copy([p for ext, p in parts.items() if ext != ".csv"], tmp)
                df, facts = read_layer(local[PurePosixPath(parts[".shp"]).name])
            entry.update(facts)
            meta["schema"].setdefault("_layers", {})[label] = facts
            _gate_and_save(run, df, layer.get("fields"), label, (std_key, "shp", key), meta, entry)
        except Exception as e:
            logger.error(f"[{label}] could not read: {e}")
            entry.update(status="read_error", error=str(e))
        if lacking and entry.get("status") == "saved":
            entry["status_note"] = f"parts missing: {lacking}"

        # ---- the companion .csv (full column names) ----
        if ".csv" in parts:
            csv_entry = _record(run, std_key, "shp_csv", key, file=PurePosixPath(parts[".csv"]).name)
            try:
                df = read_csv_bytes(settings.INPUT.read_bytes(parts[".csv"]))
                df = df.astype(object).where(df != "", None)
                stem = safe_name(PurePosixPath(parts[".csv"]).stem)       # previous bronze name: d3_ca_cracking
                _gate_and_save(run, df, layer.get("csv_headers"), f"{label}_csv", (std_key, stem),
                               meta, csv_entry, first_row=2)
            except Exception as e:
                logger.error(f"[{label}_csv] could not read: {e}")
                csv_entry.update(status="read_error", error=str(e))

    if shp_folder:
        others = sorted({PurePosixPath(p).relative_to(shp_folder).parts[0] for p in layer_files
                         if len(PurePosixPath(p).relative_to(shp_folder).parts) > 1
                         and f"{shp_folder}/{PurePosixPath(p).relative_to(shp_folder).parts[0]}" not in used_folders})
        if others:
            logger.error(f"[{std_key}/shp] shapefile folder(s) not in the schema - NOT extracted: {others}")


def _extract_gpkg(run, std_key, std_folder, std_files, cfg, meta):
    gpkg_files = {PurePosixPath(p).name: p for p in std_files if p.lower().endswith(".gpkg")}
    from nsv.matching import identify_names
    found, missing, _ = identify_names(list(gpkg_files), {k: c["file_names"] for k, c in cfg["files"].items()},
                                       settings.FUZZY_THRESHOLD, label="gpkg", quiet=True)
    for key in missing:
        required = cfg["files"][key].get("required", True)
        _record(run, std_key, "gpkg", key, status="missing" if required else "optional_missing",
                reason=f"no GeoPackage like {cfg['files'][key]['file_names']}")
        (logger.error if required else logger.info)(
            f"[{std_key}/gpkg/{key}] {'' if required else 'optional '}GeoPackage not delivered "
            f"(expected like '{cfg['files'][key]['file_names'][0]}')")

    for file_key, name in found.items():
        fcfg = cfg["files"][file_key]
        path = gpkg_files[name]
        run.accept(path, f"gis {std_key}/gpkg/{file_key}")
        ignore = {normalize_name(n, is_file=False) for n in fcfg.get("ignore_layers", IGNORE_LAYERS_DEFAULT)}
        file_entry = _record(run, std_key, "gpkg", file_key, file=name, status="found")
        try:
            with tempfile.TemporaryDirectory() as tmp:
                local = _local_copy([path], tmp)[name]
                layers = list_layers(local)
                file_entry["layers_in_file"] = [n for n, _ in layers]
                seen = set()
                for layer_name, _ in layers:
                    if normalize_name(layer_name, is_file=False) in ignore:
                        continue
                    lkey = layer_key_for(layer_name, fcfg["layers"])
                    if lkey is None or lkey in seen:
                        why = "not in the schema" if lkey is None else f"a second layer for '{lkey}'"
                        logger.error(f"[{std_key}/gpkg/{file_key}] layer '{layer_name}' {why} - NOT extracted")
                        file_entry.setdefault("unexpected_layers", []).append(layer_name)
                        continue
                    seen.add(lkey)
                    label = f"{std_key}/gpkg/{file_key}/{lkey}"
                    entry = _record(run, std_key, "gpkg_layer", f"{file_key}/{lkey}", file=name, layer=layer_name)
                    try:
                        df, facts = read_layer(local, layer=layer_name)
                        entry.update(facts)
                        meta["schema"].setdefault("_layers", {})[label] = {"layer": layer_name, **facts}
                        _gate_and_save(run, df, fcfg["layers"][lkey].get("fields"), label,
                                       (std_key, "gpkg", file_key, lkey), meta, entry)
                    except Exception as e:
                        logger.error(f"[{label}] could not read layer '{layer_name}': {e}")
                        entry.update(status="read_error", error=str(e))
                for lkey, lcfg in fcfg["layers"].items():
                    if lkey not in seen:
                        required = lcfg.get("required", True)
                        _record(run, std_key, "gpkg_layer", f"{file_key}/{lkey}", file=name,
                                status="missing" if required else "optional_missing",
                                reason=f"no layer with {lcfg.get('keywords', [lkey])} in its name")
                        (logger.error if required else logger.info)(
                            f"[{std_key}/gpkg/{file_key}] {'' if required else 'optional '}layer '{lkey}' not in '{name}'")
        except Exception as e:
            logger.error(f"[{std_key}/gpkg/{file_key}] cannot open '{name}': {e}")
            file_entry.update(status="read_error", error=str(e))
