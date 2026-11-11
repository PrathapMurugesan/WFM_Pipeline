"""Window values from the `measures` inputs of child rows (child 10m, or child 100m for 1000 m windows).

Each input is averaged over the source rows of the window — every row counts once, empty values are skipped (not
counted as 0) — and the defect's formula is applied to those averages:

  roughness: Lane IRI = (avg lwp_iri + avg rwp_iri) / 2;  BI = 630 x Lane IRI ^ 1.12
  rutting:   (avg rut_left_mm + avg rut_right_mm) / 2
  cracking:  avg crack_area_sqm / (3.5 x 10) x 100        (child 10m rows only)

A source row belongs to the window holding its start chainage, floor(start / interval) * interval; rows are never
split. threshold_value is the max over the window. A window without a value gets metric_value empty (flag 0).
"""
import json

import numpy as np
import pandas as pd

# defect -> (inputs averaged from measures, formula applied to the averages)
FORMULAS = {
    "roughness": (["lwp_iri", "rwp_iri"], lambda a: 630 * ((a["lwp_iri"] + a["rwp_iri"]) / 2) ** 1.12),
    "rutting": (["rut_left_mm", "rut_right_mm"], lambda a: (a["rut_left_mm"] + a["rut_right_mm"]) / 2),
    "cracking": (["crack_area_sqm"], lambda a: a["crack_area_sqm"] / (3.5 * 10) * 100),
}


def _load_json(text):
    try:
        value = json.loads(text)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _inputs(measures, keys):
    """Numeric columns `keys` read from the measures JSON (missing / null / non-numeric -> empty)."""
    parsed = [_load_json(text) for text in measures]
    return pd.DataFrame({
        key: pd.to_numeric(pd.Series([d.get(key) for d in parsed], index=measures.index, dtype=object),
                           errors="coerce")
        for key in keys
    })


def input_windows(rows, defect, interval, window_keys, table_key):
    """One row per window_keys + start_chainage / end_chainage window of `interval` m with metric_value (the formula
    on the averaged inputs) and threshold_value (max)."""
    if "measures" not in rows.columns:
        raise ValueError(f"{table_key} has no measures column — cannot compute {defect} windows of {interval} m")
    keys, formula = FORMULAS[defect]
    rows = rows.reset_index(drop=True)

    start = np.floor(np.fmin(rows["start_chainage"], rows["end_chainage"]) / interval) * interval
    data = pd.concat([rows[window_keys + ["threshold_value"]], _inputs(rows["measures"], keys)], axis=1)
    windows = (
        data.assign(start_chainage=start, end_chainage=start + interval)
        .groupby(window_keys + ["start_chainage", "end_chainage"], dropna=False)
        .agg(**{key: (key, "mean") for key in keys}, threshold_value=("threshold_value", "max"))
        .reset_index()
    )
    windows["metric_value"] = formula(windows)
    return windows.drop(columns=keys)
