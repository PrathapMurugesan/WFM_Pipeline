# RIS View

Modular, local (pandas) version of `code_notebooks/RIS View Pipeline_v2 in_use.ipynb`.

## Run

Run `code_intermediate_logic` first; this pipeline reads its `final_intermediate.csv`.

```bash
cd code_ris_view
pip install -r requirements.txt
python main.py
```

Outputs, written to `OUTPUT_DIR` (default `../Output_KPI_data`):

| File | Notebook equivalent |
|---|---|
| `ris_view_dw.csv` | `<OUTPUT_SCHEMA>.ris_view_dw` |

One row per survey (`UPC`, `Source_System`, `Defect_Detected_On`, `Phase`, `Cycle`), `Defect_Type`
(Cracking, Pothole, Ravelling, Roughness, Rutting, Total) and `Status`:

| Defect_Type | Status rows | Defect counts |
|---|---|---|
| Cracking, Pothole, Ravelling, Roughness, Rutting | `Open` and `Closed` (always both; 0 when none) | by `final_status` of the defect; empty `final_status` (no DB case) counts as `Open` (`NO_STATUS_AS` in `utils/defect_counts.py`) |
| Total | `Open` and `Closed` | sum of all defect types' rows of that status |

Affected lengths of Cracking / Roughness / Rutting are summed per status from `final_intermediate`, using the
10m row's `<defect>_status` (empty -> `NO_STATUS_AS`, as in the counts). Pothole / Ravelling lengths are 0.
`Total` lengths come from `total_affected_length_<std>` (10m rows with any of Roughness / Rutting / Cracking), split
per 10m row: `Closed` only when `roughness_status`, `rutting_status` and `cracking_status` are all Closed, otherwise
`Open` (empty status counts as Open — to be reviewed). Open + Closed = the survey's total affected length.

- `Total_Defects_IRC` of Ravelling / Roughness / Rutting comes from their child 10m tables (`CHILD_DIR`): the
  parent tables only have `As per CA` rows. Each child `As per IRC` row takes the `final_status` of its parent,
  matched on `parent_id`, `zone`, `phase`, `cycle`, `source_system`, `detected_on` (`PARENT_KEYS` in
  `utils/defect_counts.py`; `parent_id` alone repeats across cycles, and `pavement_type` is not used because the
  children of a `Hybrid` parent are `Asphalt` / `Cement Concrete`). Their `Total_Defects_CA` stays on the parent rows.
- Defect counts come from the silver defect tables; affected lengths are summed from `final_intermediate`
  on the same survey columns, so every affected length is counted. Defect counts add up to `Total`;
  lengths do not: `Total` is the length with any defect (a 10m row with several defects counts once).
- No pavement type: `final_intermediate` takes it from `master_chainage` (Asphalt / Cement Concrete),
  which does not match the defect tables (which also have Hybrid), so joining on it lost lengths.
- No DB id columns (`UPC_ID`, `Survey_ID`, `Survey_Source_ID`, `Defect_Type_ID`) and no
  `master_defect_type` lookup keys (`Defect_Asset_Type`, `Defect_Name`).

## Inputs

| Source | Setting | Notebook equivalent |
|---|---|---|
| `master_pothole`, `master_{ravelling,roughness,rutting}_parent` (with `final_status`) | `INPUT_DIR` (`../silver_data_with_status`, from `code_silver_update_status`) | `nhai_dhms.dlm_kpi.*` |
| Cracking (default `master_cracking`) | `CRACKING_TABLE` | `nhai_dhms.dlm_kpi.master_cracking` (10m) |
| `master_{ravelling,roughness,rutting}_child_10m` (IRC counts) | `CHILD_DIR` (`../input_silver_data`) | `nhai_dhms.dlm_kpi.*_child_10m` |
| `project_master.xlsx` | `PROJECT_MASTER_PATH`, `FILTER_BY_PROJECT_MASTER` | `nhai_dhms.contract_module.project_master_onetime` |
| `final_intermediate.csv` | `INTERMEDIATE_PATH` | `<OUTPUT_SCHEMA>.final_intermediate` |

Project-master columns used (see `utils/project_master.py`): `Length-km` → UPC Length(in km),
`Lane-km` → UPC Lane Length(in km), `State Name` → State, `RO`, `PIU`, `NH No.` → NH,
`AE/IE Name` → AE/IE, `Contractor Name` → Contractor.

## Layout

```
code_ris_view/
├── main.py                 # pipeline entry point
├── .env                    # settings
└── utils/
    ├── config.py           # .env -> Settings
    ├── io_utils.py         # silver / intermediate reads, CSV writes
    ├── project_master.py   # project details + project-master UPC filter
    ├── defect_counts.py    # defect rows -> breached-defect counts per grain
    ├── affected_length.py  # affected lengths per defect + 'Total' rows
    ├── output.py           # ris_view_dw
    └── helpers.py          # key / date normalisation
```
