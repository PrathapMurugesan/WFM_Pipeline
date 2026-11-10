"""
survey.py - one uploaded report = one UPC. Bronze paths are built from it.

WFM reports are stored per report id, not per zone / phase / cycle, so there are two layouts:
    report id only (normal):   bronze/nsv/report=3433cb89-.../N-02001-19004-DL/d2
    zone + phase + cycle given: bronze/nsv/zone=A/phase=2/cycle=2/N-02001-19004-DL/d2     (earlier NSV layout)

    survey = Survey("N-02001-19004-DL", report_id="3433cb89-...")
    survey.raw_path("d2")      -> d2            (read through ReportStorage: the D2 folder of the upload)
    survey.bronze_path("d2")   -> bronze/nsv/report=3433cb89-.../N-02001-19004-DL/d2
    survey.upc                 -> N/02001/19004/DL
"""
import re
from dataclasses import dataclass
from typing import Optional

import settings

UPC_PATTERN = re.compile(r"^[A-Z]+[/_\-]\d{5}[/_\-]\d{5}[/_\-][A-Z]{2}$", re.IGNORECASE)   # N/02001/19004/DL


def upc_folder(text):
    """'N/02001/19004/DL', 'N_02001_19004_DL', 'N-02001-19004-DL' -> 'N-02001-19004-DL' (None if not a UPC)."""
    text = text.strip()
    return re.sub(r"[/_\-]", "-", text).upper() if UPC_PATTERN.match(text) else None


@dataclass(frozen=True)
class Survey:
    upc_folder: str
    report_id: Optional[str] = None
    zone: Optional[str] = None
    phase: Optional[str] = None
    cycle: Optional[str] = None

    @property
    def upc(self):
        return self.upc_folder.replace("-", "/")

    @property
    def by_zone(self):
        return bool(self.zone and self.phase and self.cycle)

    @property
    def part(self):
        """report=<id>/<UPC>  or  zone=A/phase=2/cycle=2/<UPC>"""
        if self.by_zone:
            return f"zone={self.zone}/phase={self.phase}/cycle={self.cycle}/{self.upc_folder}"
        return f"report={self.report_id}/{self.upc_folder}"

    @property
    def label(self):
        return self.part

    def raw_path(self, deliverable):
        return deliverable

    def bronze_path(self, deliverable):
        return f"{settings.BRONZE_ROOT}/{self.part}/{deliverable}"

    def as_dict(self):
        return {"report_id": self.report_id, "zone": self.zone, "phase": self.phase, "cycle": self.cycle,
                "upc": self.upc, "upc_folder": self.upc_folder, "part": self.part}
