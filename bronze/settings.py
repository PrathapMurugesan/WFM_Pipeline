"""
settings.py - how the ingestion behaves. WHAT is accepted is in schema/nsv_schema.json, not here.
"""
import os
from pathlib import Path

from nsv.storage import LocalStorage

PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_FILE = Path(os.environ.get("NSV_SCHEMA_FILE", PROJECT_ROOT / "schema" / "nsv_schema.json"))

# Input: the uploaded report (set by main.py) - an s3:// report folder, a report id, or a local folder.
# WFM uploads: <REPORTS_S3_ROOT>/<report id>/D1..D4/<upload timestamp>-<file name>
REPORTS_S3_ROOT = os.environ.get("NSV_REPORTS_S3_ROOT", "s3://nhai-dhms-uat/wfm_reports/staging/wfm/reports/")
INPUT = None
UPLOAD = {}                 # report id / source of the run (main.py) -> every manifest

# Output: bronze, default ./data  (or NSV_OUTPUT=<folder>)
OUTPUT = LocalStorage(Path(os.environ.get("NSV_OUTPUT") or PROJECT_ROOT / "data"))

# bronze/nsv/zone=A/phase=2/cycle=2/N-02001-19004-DL/d2/run=20261106_101500/   (same layout silver reads)
BRONZE_LAYOUT = "bronze/nsv/zone={zone}/phase={phase}/cycle={cycle}/{upc}/{deliverable}"
RUN_FOLDER = "run={run_id}"
BATCH_FOLDER = "bronze/nsv/_batches"


# =============================================================================
# Strictness: only what the schema describes is accepted
# =============================================================================
def _env_flag(name, default):
    value = os.environ.get(name, "").lower()
    return default if not value else value in ("1", "true", "yes")


CHECK_HEADERS = _env_flag("NSV_CHECK_HEADERS", True)            # sheet / layer missing a schema header -> REJECTED
STOP_IF_WORKBOOK_MISSING = _env_flag("NSV_STOP_IF_WORKBOOK_MISSING", True)   # required workbook missing -> the
                                                                             # deliverable is not extracted
UNEXPECTED_IS_PROBLEM = _env_flag("NSV_UNEXPECTED_IS_PROBLEM", True)   # file / sheet / layer not in the schema
EXTRACT_UNMATCHED = False                                       # what the schema does not describe: never extracted
IGNORE_FILES = ["~$*", "Thumbs.db", "desktop.ini", ".DS_Store"]  # never reported
HASH_MAX_BYTES = 500 * 1024 * 1024                              # sha256 in the inventory up to this size


# =============================================================================
# Matching names, header detection
# =============================================================================
FUZZY_THRESHOLD = 0.85
HEADER_SCAN_ROWS = 20


# =============================================================================
# Defect-column null report (D2, D4) - report only, never rejects
# =============================================================================
NULL_LIMIT = 25
DEFECT_KEYWORDS = [
    "roughness", "rut", "crack", "ravelling", "raveling", "distress", "depth", "area", "severity",
    "chainage", "pothole", "defect", "lane", "carriageway", "image", "latitude", "longitude",
    "pci", "condition", "bi ", "iri",
]
NULL_IGNORE = ["remark", "structure", "limitation"]

PROFILE_EXAMPLES = 3
PROFILE_MAX_DISTINCT = 1000


# =============================================================================
# D4 survey images (see nsv/images.py). OCR needs tesseract installed:
#   Windows https://github.com/UB-Mannheim/tesseract/wiki   Linux: apt install tesseract-ocr
# =============================================================================
OCR_IMAGES = True
IMAGE_OCR = "when_no_gps"
TESSERACT_CMD = r"C:\Program Files\Tesseract-OCR\tesseract.exe" if os.name == "nt" else None
IMAGE_WORKERS = int(os.environ.get("NSV_IMAGE_WORKERS") or os.cpu_count() or 1)
os.environ.setdefault("OMP_THREAD_LIMIT", "1")
IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp")
IMAGE_TEXT_REGIONS = [("top banner", 0.0, 0.17)]
IMAGE_LOCATION_AGREE_M = 10
IMAGE_FIELDS = {
    "survey_id":    {"labels": ["Survey ID", "SurveyID", "Survey Id"]},
    "description":  {"labels": ["Description", "Section", "Road"]},
    "direction":    {"labels": ["Direction"]},
    "date":         {"labels": ["Date", "Survey Date", "Captured On"], "type": "date"},
    "survey_start": {"labels": ["Survey Start", "Start"], "type": "number"},
    "frame_no":     {"labels": ["Frame No", "Frame", "Frame Number"], "type": "number"},
    "latitude":     {"labels": ["Latitude", "Lat", "GPS Lat"], "type": "number", "range": (6, 38)},
    "longitude":    {"labels": ["Longitude", "Long", "Lon", "GPS Long"], "type": "number", "range": (68, 98)},
    "altitude":     {"labels": ["Altitude", "Alt"], "type": "number"},
    "distance":     {"labels": ["Distance"], "type": "number"},
    "chainage":     {"labels": ["Chainage KM", "Chainage", "Chainage (m)", "Ch"], "type": "number"},
    "speed":        {"labels": ["Speed Km/hr", "Speed", "Speed (km/h)"], "type": "number", "range": (0, 150)},
}
