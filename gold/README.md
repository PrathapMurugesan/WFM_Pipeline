# RIS KPI Status Logic

Local (pandas) versions of the NHAI RIS KPI Databricks notebooks. The code is split into four pipelines,
and `run_all.py` runs them in order.

| Step | Folder | Reads | Writes |
|---|---|---|---|
| `ris_trends` | [code_ris_trends](code_ris_trends) | `input_silver_data/`, `project_master.xlsx` | `Output_KPI_data/ris_trends_dw.csv` |
| `silver_update` | [code_silver_update_status](code_silver_update_status) | `input_silver_data/`, Postgres (`udm_defect_reference`, `dlm_defect_case`) | `silver_data_with_status/`, `fetched_db_tables/` |
| `intermediate` | [code_intermediate_logic](code_intermediate_logic) | `input_silver_data/`, `silver_data_with_status/` | `Output_KPI_data/final_intermediate.csv` |
| `ris_view` | [code_ris_view](code_ris_view) | `silver_data_with_status/`, `input_silver_data/`, `final_intermediate.csv`, `project_master.xlsx` | `Output_KPI_data/ris_view_dw.csv` |

Each folder has its own `main.py`, `utils/` package, `.env` settings and README with the details.

## Setup

1. **Python 3.11+** and the dependencies:

   ```bash
   python -m venv .venv
   .venv\Scripts\activate        # Windows  (macOS/Linux: source .venv/bin/activate)
   pip install -r requirements.txt
   ```

2. **Settings:** each `code_*` folder reads its settings from its own `.env`. These are committed,
   except `code_silver_update_status/.env`, which holds the Postgres credentials and is git-ignored.
   Create it from the template and fill in the `DB_*` values:

   ```bash
   cp code_silver_update_status/.env.example code_silver_update_status/.env
   ```

3. **Data:** none of it is in the repo (it's too large for GitHub, and confidential). Put these in place locally:
   - `input_silver_data/`: the silver tables (`master_*.csv`, `dlm_threshold_mapping.csv`, `upc_reporting_intervals.csv`)
   - `project_master.xlsx` in the repo root: the contract-module project master

   `silver_data_with_status/`, `fetched_db_tables/` and `Output_KPI_data/` are created when the pipelines run.

## Run

```bash
python run_all.py                              # ris_trends -> silver_update -> intermediate -> ris_view
python run_all.py --only intermediate ris_view # only some steps
python run_all.py --keep-going                 # continue independent steps after a failure
```

You can also run a single pipeline from inside its folder with `python main.py`.

## Layout

```
.
├── run_all.py                    # runs the pipelines in order
├── requirements.txt              # all dependencies
├── code_ris_trends/
├── code_silver_update_status/
├── code_intermediate_logic/
├── code_ris_view/
├── input_silver_data/            # input data (not committed)
└── project_master.xlsx           # reference master (not committed)
```
