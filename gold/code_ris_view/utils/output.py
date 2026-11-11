"""Final ris_view_dw table: per survey (upc / source_system / detected_on / phase / cycle) one row per
defect type (incl. 'Total') and status ('Open' / 'Closed'). No DB id columns or pavement type."""
from datetime import datetime

import pandas as pd

DW_RENAME = {
    "source_system": "Source_System",
    "upc": "UPC",
    "phase": "Phase",
    "cycle": "Cycle",
    "detected_on": "Defect_Detected_On",
    "defect_type": "Defect_Type",
    "status": "Status",
    "affected_length_ca_m": "Affected_length_CA(in_m)",
    "affected_length_irc_m": "Affected_Length_IRC(in_m)",
    "affected_length_ca_km": "Affected_length_CA(in_km)",
    "affected_length_irc_km": "Affected_Length_IRC(in_km)",
    "zone": "Zone",
}

DW_COLUMNS = [
    "Source_System", "UPC", "Phase", "Cycle", "Defect_Detected_On", "Defect_Type", "Status", "Zone",
    "UPC Length(in km)", "UPC Lane Length(in km)", "State", "RO", "PIU", "NH", "AE/IE", "Contractor",
    "Total_Defects_IRC", "Total_Defects_CA",
    "Affected_length_CA(in_m)", "Affected_Length_IRC(in_m)",
    "Affected_length_CA(in_km)", "Affected_Length_IRC(in_km)",
    "populated_on",
]


def build_ris_view_dw(defect_rows, total_rows):
    dw = pd.concat([defect_rows, total_rows], ignore_index=True)
    dw["populated_on"] = datetime.now()
    dw = dw.rename(columns=DW_RENAME)
    # Drop rows where Zone is missing
    return dw.dropna(subset=["Zone"])[DW_COLUMNS].reset_index(drop=True)
