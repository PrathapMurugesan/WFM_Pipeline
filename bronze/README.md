# NSV bronze ingestion: upload check (D1–D4)

When a WFM report (one UPC) is uploaded, this module checks **every file and part** of the folder against **one schema file**,
[`schema/nsv_schema.json`](schema/nsv_schema.json). The schema is built from the NHAI *Instruction / FAQ Sheet – NSV Reports (D1–D4)*
(6 Aug 2026) and the template delivery in `../NSV Data New`.
What the schema describes is extracted into bronze. Everything else is reported and never extracted.

## Run
Normally through [`../run_pipeline.py`](../run_pipeline.py), which runs bronze, then silver, then gold. To run bronze on its own:
```bash
pip install -r requirements.txt     # + boto3 for S3
python main.py 3433cb89-f583-438c-ab44-6e99455e1bf4                  # WFM report id (UPC from the data)
python main.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --upc N/02001/19004/DL   # several UPCs in the data
python main.py "<local folder>" --report-id <id>          # a local copy; add --zone A --phase 2 --cycle 2 for the earlier layout
```
- **Input layout.** A report holds `D1`..`D4` (or `Deliverable 1 - RAMS Data` …) folders. File names may start with the upload timestamp
  (`1793260589326-LRMS_DataTemplate.xlsx`). That prefix is removed before matching, and kept in the inventory as `uploaded_name` / `uploaded_at`.
  If a file was uploaded twice, the newest copy is used and the older one is reported as `superseded_upload`.
- **UPC.** `--upc`. Without it: the folder name if it is a UPC, otherwise the single UPC found in the data.
- **Exit code.** **0** = the upload matches the schema, **2** = problems (see the report), **1** = bronze could not run.

## Output (default `./data`, or `NSV_OUTPUT=<folder>`)
```
bronze/nsv/report=<report id>/<UPC>/d1..d4/run=<timestamp>/   (with --zone/--phase/--cycle: zone=A/phase=2/cycle=2/<UPC>)
    <dN>_<workbook>_<sheet>.parquet + .csv       every value as text, exactly as delivered, + source_row
    d3_<ca|irc>_shp_<layer>, d3_<std>_gpkg_<file>_<layer>    GIS attributes + geometry_wkt
    d4_images_<crack|pothole>_images             one row per image (EXIF, OCR, lane)
    <dN>_inventory_files                         one row per raw file + what the schema accepted it as
    meta/*.json  <dN>_manifest.json  <dN>_run.log
  <dN>_latest.json
bronze/nsv/_batches/batch=<timestamp>_report.csv   ONE ROW PER ITEM - problems in UPPER CASE   <- start here
bronze/nsv/_batches/batch=<timestamp>.json         accepted true/false, totals
```

## What is checked
| Problem status | Meaning |
|---|---|
| `DELIVERABLE_MISSING` | no `Deliverable N ...` folder |
| `WORKBOOK_MISSING` / `SHEET_MISSING` | a required Excel file / sheet is missing (a missing workbook stops that deliverable) |
| `REJECTED_HEADERS` / `GIS_REJECTED` | a sheet / GIS layer lacks a schema column: not saved |
| `DOCUMENT_MISSING` / `FOLDER_MISSING` | PDF (NSV Report, Checklist, SOP) / Contract Agreement missing |
| `GIS_MISSING` / `GIS_PARTS_MISSING` | shapefile set, GPKG file or layer missing / a shapefile lacks one of cpg csv dbf prj qmd shp shx |
| `IMAGES_MISSING` | crack / pothole image folder missing |
| `TEMPLATE_PROBLEM` | D2: both comparison templates delivered (the instruction allows exactly one) |
| `NOT_IN_SCHEMA` | a file, sheet, GPKG layer or root item the schema does not describe: not extracted |
| `UPC_MISMATCH` | a UPC column in the data holds another UPC than the uploaded one |

Header matching: exact, then case/spaces, unit, `aliases`, and finally the same column position with the same kind of values (a warning).
Extra columns are saved and listed. Empty or sparse defect columns are only reported.
Lenient switches (environment): `NSV_CHECK_HEADERS=0`, `NSV_STOP_IF_WORKBOOK_MISSING=0`, `NSV_UNEXPECTED_IS_PROBLEM=0`.

## Changing what is accepted
- **A new template version.** Run `python tools/build_schema.py "<known-good delivery>"` (add `--check` for a dry run). It refreshes the
  header / field lists and keeps aliases. Review the diff.
- **A vendor spelling of the same column.** Add it to that header's `"aliases"`.
- **A new file, sheet, layer or folder, or a change to required/optional.** Edit `schema/nsv_schema.json` by hand.

## Code
```
main.py                 entry point: one uploaded UPC folder -> checks + bronze + report
ingest.py               one deliverable, driven by the schema
settings.py             output folder, strictness switches, image / OCR settings
schema/nsv_schema.json  THE schema
tools/build_schema.py   fill the schema's header / field lists from a template delivery
nsv/                    engine: storage (upload folder as d1..d4), excel, headers, checks, pipeline,
                        gis (D3), images (D4), inventory, run, report, profiler, formulas, writer
```
