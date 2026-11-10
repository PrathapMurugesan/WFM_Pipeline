"""
survey.py - one uploaded UPC = zone + phase + cycle + UPC. Bronze paths are built from these four values.

    survey = Survey("A", "2", "2", "N-02001-19004-DL")
    survey.raw_path("d2")      -> d2            (read through UploadStorage: the 'Deliverable 2 ...' folder)
    survey.bronze_path("d2")   -> bronze/nsv/zone=A/phase=2/cycle=2/N-02001-19004-DL/d2
    survey.upc                 -> N/02001/19004/DL
"""
import re
from dataclasses import dataclass

import settings

UPC_PATTERN = re.compile(r"^[A-Z]+[/_\-]\d{5}[/_\-]\d{5}[/_\-][A-Z]{2}$", re.IGNORECASE)   # N/02001/19004/DL


def upc_folder(text):
    """'N/02001/19004/DL', 'N_02001_19004_DL', 'N-02001-19004-DL' -> 'N-02001-19004-DL' (None if not a UPC)."""
    text = text.strip()
    return re.sub(r"[/_\-]", "-", text).upper() if UPC_PATTERN.match(text) else None


@dataclass(frozen=True)
class Survey:
    zone: str
    phase: str
    cycle: str
    upc_folder: str

    @property
    def upc(self):
        return self.upc_folder.replace("-", "/")

    @property
    def label(self):
        return f"{self.zone}/{self.phase}/{self.cycle}/{self.upc_folder}"

    def raw_path(self, deliverable):
        return deliverable

    def bronze_path(self, deliverable):
        return settings.BRONZE_LAYOUT.format(zone=self.zone, phase=self.phase, cycle=self.cycle,
                                             upc=self.upc_folder, deliverable=deliverable)

    def as_dict(self):
        return {"zone": self.zone, "phase": self.phase, "cycle": self.cycle,
                "upc": self.upc, "upc_folder": self.upc_folder}
