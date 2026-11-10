"""
analyse_zone.py - find where the zone of a WFM survey lives (READ-ONLY: only SELECT / information_schema).

wfm_survey_schedule has no zone letter, only ids: nsv_zone_id, dams_zone_id, das_zone_id (and ro_id, state_id ...).
This script finds the tables those ids point to and shows the matching rows, so the zone mapping can be set up.

    python tools/analyse_zone.py 3433cb89-f583-438c-ab44-6e99455e1bf4
    python tools/analyse_zone.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --columns nsv_zone_id ro_id
    python tools/analyse_zone.py 3433cb89-f583-438c-ab44-6e99455e1bf4 --all-schemas      search every schema

Steps:
  1. foreign keys of wfm_survey_schedule      -> the table each *_id column references (exact, if FKs are defined)
  2. tables whose name has zone / region / ro / office / state  -> their columns + row count + a few rows
  3. the survey's id values (nsv_zone_id, dams_zone_id, das_zone_id, ro_id, state_id, piu_id) searched in the
     'id' column of every table (uuid / text id columns only) -> the rows that match
Result also saved to output/metadata/<survey id>_zone_analysis.json
Credentials: as wfm_metadata.py (postgres_creds.txt / DB_* environment variables). Run on the EC2 instance.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import wfm_metadata as wm

DEFAULT_COLUMNS = ["nsv_zone_id", "dams_zone_id", "das_zone_id", "ro_id", "state_id", "piu_id"]
NAME_HINTS = re.compile(r"zone|region|(^|_)ro(_|$)|office|state|piu", re.I)
SAMPLE_ROWS = 5


def q(cur, sql, args=()):
    cur.execute(sql, args)
    return wm._rows(cur)


def main(args):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("survey_id")
    ap.add_argument("--columns", nargs="+", default=DEFAULT_COLUMNS, help="id columns of wfm_survey_schedule to trace")
    ap.add_argument("--all-schemas", action="store_true", help="search every schema, not only the schedule's")
    o = ap.parse_args(args)
    from psycopg2 import sql

    creds = wm.load_creds()
    result = {"survey_id": o.survey_id}
    for db in wm.databases(creds):
        with wm.connect(creds, db) as conn, conn.cursor() as cur:
            conn.set_session(readonly=True)
            schema, cols = wm.find_table(cur, wm.SCHEDULE_TABLE, creds.get("DB_SCHEMA"))
            if not schema:
                continue
            result["database"], result["schema"] = db, schema
            print(f"database {db}, schema {schema}\n")

            # ---- the survey's id values ----
            rows = wm.fetch(cur, schema, wm.SCHEDULE_TABLE, "id", o.survey_id)
            if not rows:
                print(f"survey {o.survey_id} not in {schema}.{wm.SCHEDULE_TABLE}")
                return 1
            survey = rows[0]
            values = {c: survey.get(c) for c in o.columns if c in survey}
            print("1) the survey's id columns:")
            for c, v in values.items():
                print(f"     {c:15} = {v}")
            result["values"] = values

            # ---- 1. foreign keys ----
            fks = q(cur, """
                SELECT kcu.column_name, ccu.table_schema AS ref_schema, ccu.table_name AS ref_table,
                       ccu.column_name AS ref_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage ccu
                  ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s AND tc.table_name = %s
                ORDER BY kcu.column_name""", (schema, wm.SCHEDULE_TABLE))
            print(f"\n2) foreign keys of {wm.SCHEDULE_TABLE}: {len(fks) or 'none defined'}")
            for fk in fks:
                mark = "  <--" if fk["column_name"] in values else ""
                print(f"     {fk['column_name']:20} -> {fk['ref_schema']}.{fk['ref_table']}.{fk['ref_column']}{mark}")
            result["foreign_keys"] = fks

            # ---- 2. zone-like tables ----
            where = "" if o.all_schemas else "AND table_schema = %s"
            tables = q(cur, f"""SELECT table_schema, table_name FROM information_schema.tables
                                WHERE table_type IN ('BASE TABLE', 'VIEW')
                                  AND table_schema NOT IN ('pg_catalog', 'information_schema') {where}
                                ORDER BY table_schema, table_name""", () if o.all_schemas else (schema,))
            hinted = [t for t in tables if NAME_HINTS.search(t["table_name"])]
            print(f"\n3) tables named like zone / region / ro / office / state / piu: {len(hinted)}")
            result["zone_like_tables"] = []
            for t in hinted:
                tcols = q(cur, """SELECT column_name, data_type FROM information_schema.columns
                                  WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
                          (t["table_schema"], t["table_name"]))
                ident = sql.SQL("{}.{}").format(sql.Identifier(t["table_schema"]), sql.Identifier(t["table_name"]))
                try:
                    cur.execute(sql.SQL("SELECT count(*) FROM ") + ident)
                    count = cur.fetchone()[0]
                    cur.execute(sql.SQL("SELECT * FROM ") + ident + sql.SQL(" LIMIT %s"), (SAMPLE_ROWS,))
                    sample = wm._rows(cur)
                except Exception as e:
                    conn.rollback()
                    count, sample = f"error: {e}", []
                print(f"\n   {t['table_schema']}.{t['table_name']}  ({count} rows)")
                print("     columns: " + ", ".join(f"{c['column_name']} ({c['data_type']})" for c in tcols))
                for r in sample:
                    print("     " + json.dumps(r, ensure_ascii=False, default=str)[:300])
                result["zone_like_tables"].append({**t, "rows": count, "columns": tcols, "sample": sample})

            # ---- 3. which table holds each id value ----
            id_tables = q(cur, f"""SELECT table_schema, table_name, data_type FROM information_schema.columns
                                   WHERE column_name = 'id' AND data_type IN ('uuid', 'text', 'character varying')
                                     AND table_schema NOT IN ('pg_catalog', 'information_schema') {where}""",
                          () if o.all_schemas else (schema,))
            print(f"\n4) the id values searched in the 'id' column of {len(id_tables)} table(s):")
            result["matches"] = {}
            for col, value in values.items():
                if not value:
                    print(f"     {col}: empty")
                    continue
                hits = []
                for t in id_tables:
                    ident = sql.SQL("{}.{}").format(sql.Identifier(t["table_schema"]), sql.Identifier(t["table_name"]))
                    try:
                        cur.execute(sql.SQL("SELECT * FROM ") + ident + sql.SQL(" WHERE id::text = %s LIMIT 1"), (str(value),))
                        found = wm._rows(cur)
                    except Exception:
                        conn.rollback()
                        continue
                    if found:
                        hits.append({"table": f"{t['table_schema']}.{t['table_name']}", "row": found[0]})
                print(f"\n     {col} = {value}: found in {len(hits)} table(s)")
                for h in hits:
                    print(f"       {h['table']}: " + json.dumps(h["row"], ensure_ascii=False, default=str)[:400])
                result["matches"][col] = hits
            break

    target = ROOT / "output" / "metadata" / f"{o.survey_id}_zone_analysis.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"\nsaved: {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
