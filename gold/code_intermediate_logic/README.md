# Affected Length — Intermediate Logic

Modular, local (pandas) version of `code_notebooks/Affected length intermediate logic v3 in_use.ipynb`, changed from
a fixed 10m grain to the smallest reporting interval of each survey.

## Run

```bash
cd code_intermediate_logic
pip install -r requirements.txt
python main.py
```

Output, written to `OUTPUT_DIR` (default `../Output_KPI_data`):

| File | Content |
|---|---|
| `final_intermediate.csv` | One row per smallest-reporting-interval window × flags × affected lengths × ids / statuses (read by `code_ris_view`) |

## Rows

Rows are not fixed 10m: each upc / zone / phase / cycle uses its `spine_interval`, the smallest filled reporting
interval over every defect (roughness, rutting, cracking, ravelling) and standard (CA, IRC); `DEFAULT_INTERVAL` when
all are empty. The 10m `master_chainage` rows are grouped per upc / zone / phase / cycle / lane / source_system /
defect_detected_on / pavement_type into windows on a fixed grid, `floor(chainage / spine_interval) * spine_interval`;
the first / last window of a run is cut to the chainage actually covered (e.g. 254530–254550 on a 50 m grid).
A window crossing a pavement_type change becomes two rows.

Columns: `upc`, `zone`, `source_system`, `defect_detected_on`, `start_chainage`, `end_chainage`, `phase`, `cycle`,
`lane`, `pavement_type`, `spine_interval`, `{roughness,cracking,rutting,ravelling}_interval_{ca,irc}`, the flags,
the lengths, then the ids and statuses (below).

## Flags

Per defect (roughness, rutting, cracking — ravelling is not flagged) and standard, the window comes from the first
source with "As per CA" / "As per IRC" rows for that survey (`utils/defect_report.py`):

| Defect | Interval | Source |
|---|---|---|
| roughness, rutting | 1000 m | parent (as-is) → child 100m inputs → 1000 m (~10 rows) → child 10m inputs → 1000 m (~100 rows) |
| roughness, rutting | 100 m | child 100m (as-is) → child 10m inputs → 100 m (~10 rows) |
| roughness, rutting | other (e.g. 50 m) | child 10m inputs → interval → child 100m (as-is) |
| cracking | any | child 10m inputs → interval only (no parent / child 100m) |

"Inputs" (`utils/measure_windows.py`): the inputs in the `measures` column are averaged over the source rows of each
window — every row counts once, empty values skipped (not counted as 0); a row belongs to the window holding its start
chainage, `floor(start / interval) × interval`, and is never split — then the formula is applied to the averages;
threshold = max over the window, a window without a value gets 0:

| Defect | Value |
|---|---|
| roughness | Lane IRI = (avg `lwp_iri` + avg `rwp_iri`) / 2; BI = 630 × IRI^1.12 |
| rutting | (avg `rut_left_mm` + avg `rut_right_mm`) / 2 |
| cracking | avg `crack_area_sqm` / (3.5 × 10) × 100 (child 10m rows) |

As-is sources keep their own segments and the provider's `metric_value`. A window is defected when
`metric_value > threshold_value`. A row gets `<defect>_<std>_flag = 1` when it lies fully inside a defected window
(e.g. a defected 1 km parent flags its ten 100 m rows); a row only partly covered gets 0.
`defect_<std>_flag` is 1 if any defect flag is 1.

## Lengths

`<defect>_<std>_length = <defect>_<std>_flag × spine_interval` and
`total_affected_length_<std> = defect_<std>_flag × spine_interval` (on a cut-off first / last row of a lane this is
the full interval, not end − start).

## Defect ids and statuses

Read from `SILVER_STATUS_DIR` (default `../silver_data_with_status`, written by `code_silver_update_status`, so run
that first). Matched on `upc`, `source_system`, `phase`, `cycle`, `lane` (not `pavement_type`), "As per CA" rows only:

| Column | Source | Rule |
|---|---|---|
| `roughness_parent_defect_id`, `rutting_parent_defect_id` | `master_<defect>_parent` | `parent_start_chainage <= start_chainage < parent_end_chainage` |
| `cracking_defect_id` | `CRACKING_TABLE` (10m child) | the child holding the row's `start_chainage` (also covers children offset from the 10m grid) |
| `roughness_status`, `rutting_status` | same parent row | its `final_status`: `Open`, `Closed` or empty |
| `cracking_status` | every cracking child overlapping the row (5 on a 50 m row, 10 on a 100 m row) | `Open` if any is Open, `Closed` if all are Closed, otherwise empty |

Silver ids restart per survey, so an id is only unique together with `upc` / `phase` / `cycle`.

## Not handled yet

Logged as warnings: duplicate upc / phase / cycle rows in the reporting-interval table keep the first row; a
`spine_interval` that is not a multiple of 10 (e.g. 12.5) gives windows that cut through 10m rows.

## Inputs (from `INPUT_DIR`)

| Source | Used for |
|---|---|
| `master_{roughness,rutting}_parent` | 1000 m windows (as-is) |
| `master_{roughness,rutting}_child_100m` | 100 m windows (as-is); 1000 m windows from `measures` when the parent has no rows |
| `master_{roughness,rutting}_child_10m` | windows from `measures` (50 m, and fallback for 100 / 1000 m) |
| `CRACKING_TABLE` (default `master_cracking`, 10m) | all cracking windows, from `measures` |
| `master_chainage` | 10m rows grouped into the row windows |
| `REPORTING_INTERVALS_TABLE` | Interval per upc / phase / cycle / standard (CA, IRC) and defect, plus zone; a missing standard or empty interval uses `DEFAULT_INTERVAL` |

## Layout

```
code_intermediate_logic/
├── main.py                    # pipeline entry point
├── .env                       # settings
└── utils/
    ├── config.py              # .env -> Settings
    ├── io_utils.py            # silver table reads, CSV writes
    ├── reporting_intervals.py # reporting intervals -> interval per defect / standard, zone, spine_interval
    ├── defect_report.py       # source routing + window averages + defect_flag
    ├── measure_windows.py     # formulas + measures inputs averaged into windows (child 10m / 100m)
    ├── interval_spine.py      # rows on the smallest reporting interval, flags, affected lengths
    ├── spine_flags.py         # window coverage check (covered / defected_windows); old 10m spine functions unused
    ├── defect_ids.py          # parent / cracking ids and statuses (attach_segment_ids); old 10m functions unused
    └── helpers.py             # UPC format, Spark-style casts
```
