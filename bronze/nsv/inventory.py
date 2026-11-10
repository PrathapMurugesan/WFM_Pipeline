"""
inventory.py - the files of a deliverable that are kept as files, not as tables, and the full file list.

    check_documents(run, folder, documents)   PDFs named in the instruction sheet ('NSV Report of <Project>.pdf')
    check_folders(run, folder, folders)       folders recorded file by file only (Contract Agreement, Videos)
    write_inventory(run, folder)              ONE ROW PER FILE in the raw deliverable folder:
                                              d<N>_inventory_files.parquet + .csv
                                              uploaded_name / uploaded_at = as uploaded (timestamp prefix)
                                              accepted_as = what the schema accepted it as, or NOT_IN_SCHEMA

Anything in the folder that no part of the schema accepted is listed in manifest["unexpected_files"]:
it is never extracted and (settings.UNEXPECTED_IS_PROBLEM) makes the run 'completed_with_problems'.
"""
import hashlib
import re
from fnmatch import fnmatch
from pathlib import PurePosixPath

import pandas as pd

import settings
from nsv.log import get_logger
from nsv.matching import match_score

logger = get_logger("nsv.inventory")


def _ignored(path):
    name = PurePosixPath(path).name
    return any(fnmatch(name.lower(), pattern.lower()) for pattern in settings.IGNORE_FILES)


def _relative(path, folder):
    return PurePosixPath(path).relative_to(folder.strip("/")).as_posix()


def _norm_pattern(text):
    """'NSV Report of *.pdf' -> regex: the fixed words in order (case, spaces, _ - . ignored), * = anything."""
    chunks = [r"[\W_]*".join(re.findall(r"[A-Za-z0-9]+", chunk)) for chunk in text.split("*")]
    return re.compile(r"^[\W_]*" + r".*".join(chunks) + r"$", re.IGNORECASE)


def _size(path):
    size = getattr(settings.INPUT, "size", None)
    try:
        return size(path) if size else len(settings.INPUT.read_bytes(path))
    except Exception:
        return None


# =============================================================================
# Documents (PDF): found by file name pattern, anywhere directly in the deliverable folder
# =============================================================================
def check_documents(run, folder, documents):
    files = [p for p in settings.INPUT.list_files(folder) if not _ignored(p)]
    top = [p for p in files if "/" not in _relative(p, folder)]
    for key, cfg in documents.items():
        patterns = [_norm_pattern(p) for p in cfg["file_patterns"]]
        hits = [p for p in top if any(rx.match(PurePosixPath(p).name) for rx in patterns)]
        required = cfg.get("required", True)
        entry = run.manifest["documents"][key] = {"expected": cfg["file_patterns"], "required": required,
                                                  "files": [_relative(p, folder) for p in hits]}
        if not hits:
            entry["status"] = "missing" if required else "optional_missing"
            (logger.error if required else logger.info)(
                f"[document {key}] {'' if required else 'optional '}not delivered (expected like {cfg['file_patterns']})")
            continue
        entry["status"] = "found"
        if len(hits) > 1:
            logger.warning(f"[document {key}] {len(hits)} files fit: {entry['files']}")
        for p in hits:
            run.accept(p, f"document {key}")
            data = settings.INPUT.read_bytes(p)
            entry.setdefault("facts", []).append({"file": _relative(p, folder), "size_bytes": len(data),
                                                  "sha256": hashlib.sha256(data).hexdigest()})
            # the <Project Name> part of 'NSV Report of <Project Name>.pdf'
            if cfg.get("project_name_from") and "project_name" not in entry:
                m = re.match(cfg["project_name_from"], PurePosixPath(p).stem, re.IGNORECASE)
                if m:
                    entry["project_name"] = m.group(1).strip()
        logger.info(f"[document {key}] found: {entry['files']}"
                    + (f" (project: {entry['project_name']})" if entry.get("project_name") else ""))


# =============================================================================
# Inventory-only folders (Contract Agreement, Videos): every file listed, nothing extracted
# =============================================================================
def check_folders(run, folder, folders):
    files = [p for p in settings.INPUT.list_files(folder) if not _ignored(p)]
    subfolders = sorted({_relative(p, folder).split("/")[0] for p in files if "/" in _relative(p, folder)})
    for key, cfg in folders.items():
        required = cfg.get("required", True)
        scored = sorted(((match_score(f, cfg["folder_names"], is_file=False), f) for f in subfolders), reverse=True)
        name = scored[0][1] if scored and scored[0][0] >= settings.FUZZY_THRESHOLD else None
        entry = run.manifest["folders"][key] = {"expected": cfg["folder_names"], "required": required}
        inside = [p for p in files if name and _relative(p, folder).startswith(name + "/")]
        if not inside:
            entry["status"] = "missing" if required else "optional_missing"
            (logger.error if required else logger.info)(
                f"[folder {key}] {'' if required else 'optional '}not delivered or empty "
                f"(expected like {cfg['folder_names']})")
            continue
        extensions = [e.lower() for e in cfg.get("extensions", [])]
        ok = [p for p in inside if not extensions or PurePosixPath(p).suffix.lower() in extensions]
        wrong = [_relative(p, folder) for p in inside if p not in ok]
        for p in ok:
            run.accept(p, f"folder {key}")
        entry.update(status="found", folder=name, files=len(ok), file_types=sorted({PurePosixPath(p).suffix.lower() for p in ok}))
        if wrong:
            entry["wrong_file_types"] = wrong
            logger.error(f"[folder {key}] {len(wrong)} file(s) of a type the schema does not accept "
                         f"(allowed {extensions}): {wrong[:5]}")
        logger.info(f"[folder {key}] '{name}': {len(ok)} file(s) {entry['file_types']}")


# =============================================================================
# The full file list + what is not in the schema
# =============================================================================
def write_inventory(run, folder):
    rows, unexpected = [], []
    for p in settings.INPUT.list_files(folder):
        if _ignored(p):
            continue
        accepted = run.accepted.get(p)
        rel = _relative(p, folder)
        ext = PurePosixPath(p).suffix.lower()
        size = _size(p)
        digest = None
        if ext not in settings.IMAGE_EXTENSIONS and size is not None and size <= settings.HASH_MAX_BYTES:
            digest = hashlib.sha256(settings.INPUT.read_bytes(p)).hexdigest()
        uploaded = getattr(settings.INPUT, "uploaded_name", None)
        when = getattr(settings.INPUT, "uploaded_at", None)
        rows.append({"relative_path": rel, "folder": str(PurePosixPath(rel).parent), "file_name": PurePosixPath(p).name,
                     "uploaded_name": uploaded(p) if uploaded else PurePosixPath(p).name,
                     "uploaded_at": when(p) if when else None, "source": settings.INPUT.full_path(p),
                     "extension": ext, "size_bytes": size, "sha256": digest,
                     "accepted_as": accepted or "NOT_IN_SCHEMA"})
        if not accepted:
            unexpected.append(rel)
    run.manifest["unexpected_files"] = unexpected
    for rel in unexpected:
        (logger.error if settings.UNEXPECTED_IS_PROBLEM else logger.warning)(
            f"file not in the schema - NOT extracted: '{rel}'")
    df = pd.DataFrame(rows, columns=["relative_path", "folder", "file_name", "uploaded_name", "uploaded_at", "source",
                                     "extension", "size_bytes", "sha256", "accepted_as"])
    df.insert(0, "source_row", range(1, len(df) + 1))
    run.manifest["inventory"] = {"files": len(df), "not_in_schema": len(unexpected),
                                 "outputs": run.save_table(df, "inventory", "files")}
    logger.info(f"inventory: {len(df)} file(s), {len(unexpected)} not in the schema "
                f"-> {run.file_name('inventory', 'files')}.parquet + .csv")
