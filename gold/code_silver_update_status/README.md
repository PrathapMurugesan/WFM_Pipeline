# Silver Status Update

Modular, local (pandas) version of `code_notebooks/silver_status_update.ipynb`.
Adds `defect_reference_id`, `current_status` and `final_status` to the silver defect tables.

## Run

```bash
cd code_silver_update_status
pip install -r requirements.txt
python main.py
```

## Flow

1. **DB tables:** `udm_defect_reference` and `dlm_defect_case` are fetched from Postgres (`DB_*` settings)
   into `FETCHED_DIR` (default `../fetched_db_tables`) as `<table>.csv`, then read from there.
   With `REFRESH_DB_TABLES=false` (default) a CSV already in `FETCHED_DIR` is reused and only missing
   tables are fetched; set it to `true` to re-fetch everything.
2. **Silver tables:** read from `INPUT_DIR`, filtered to `KEEP_UPCS` (empty = all UPCs).
3. **Status columns**, inserted right after `id`:

   | Column | Rule |
   |---|---|
   | `defect_reference_id` | `udm_defect_reference.id` where `defect_id` = silver `id` and `defect_collection` matches the table |
   | `current_status` | `dlm_defect_case.current_status` where `defect_reference_id` matches |
   | `final_status` | `Closed` for CLOSED / CANCELLED, `Open` for any other status, empty when there is no status |

   Both joins are left joins; a DB key that is not unique stops the run instead of duplicating rows.
4. **Output:** the updated tables are written to `OUTPUT_DIR` (default `../silver_data_with_status`) under
   the same names. Silver CSVs are read and written as text, so all existing values are unchanged.
5. **Check (notebook's last cell):** CLOSED / CANCELLED cases and whether their `defect_reference_id`
   exists in `udm_defect_reference`, logged per defect type / record level / status.

| Silver table | `defect_collection` |
|---|---|
| `master_roughness_parent` | `roughness_parent` |
| `master_rutting_parent` | `rutting_parent` |
| `master_ravelling_parent` | `ravelling_parent` |
| `master_pothole` | `pothole_child_ca` |
| `master_cracking` | `cracking_child_ca` |

The mapping is `STATUS_TABLES` in `utils/status.py`. The notebook also loaded
`master_cracking_child_100m` and `master_cracking_parent` but did not add status to them.

## Layout

```
code_silver_update_status/
├── main.py              # pipeline entry point
├── .env                 # settings (incl. DB credentials)
└── utils/
    ├── config.py        # .env -> Settings
    ├── db_fetch.py      # Postgres -> FETCHED_DIR/<table>.csv
    ├── io_utils.py      # silver reads (as text), CSV writes
    └── status.py        # defect_reference_id / current_status / final_status + closed-case check
```
