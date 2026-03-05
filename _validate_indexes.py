"""Valida indexes de grain en fct_trip/leg y cuenta filas actuales."""
import pyodbc, os
from dotenv import load_dotenv
load_dotenv()

conn = pyodbc.connect(
    f"DRIVER={{{os.getenv('SQLSERVER_DRIVER')}}};"
    f"SERVER={os.getenv('SQLSERVER_HOST')};"
    f"DATABASE={os.getenv('SQLSERVER_DB')};"
    "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
)
cur = conn.cursor()

print("=== Índices de grain ===")
idx_checks = [
    ("dw.fct_trip",     "UX_fct_trip_grain"),
    ("dw.fct_trip_leg", "UX_fct_trip_leg_grain"),
    # Los malos NO deben existir
    ("dw.fct_trip",     "UQ_fct_trip_grain"),
    ("dw.fct_trip_leg", "UQ_fct_trip_leg_grain"),
]
for tbl, idx in idx_checks:
    cur.execute(
        "SELECT i.name, i.is_unique, i.filter_definition "
        "FROM sys.indexes i WHERE i.object_id=OBJECT_ID(?) AND i.name=?",
        (tbl, idx),
    )
    row = cur.fetchone()
    if row:
        print(f"  ✔ {tbl}.{idx}  unique={row[1]}  filter={row[2]!r}")
    else:
        print(f"  ✘ {tbl}.{idx}  — NO EXISTE")

print("\n=== Columnas del índice UX_fct_trip_grain ===")
cur.execute("""
SELECT c.name, ic.key_ordinal, ic.is_descending_key
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
WHERE ic.object_id=OBJECT_ID(N'dw.fct_trip') AND ic.index_id=(
    SELECT index_id FROM sys.indexes WHERE object_id=OBJECT_ID(N'dw.fct_trip') AND name=N'UX_fct_trip_grain'
)
ORDER BY ic.key_ordinal
""")
for r in cur.fetchall():
    print(f"  col={r[0]}  ord={r[1]}  desc={r[2]}")

print("\n=== Columnas del índice UX_fct_trip_leg_grain ===")
cur.execute("""
SELECT c.name, ic.key_ordinal
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id
WHERE ic.object_id=OBJECT_ID(N'dw.fct_trip_leg') AND ic.index_id=(
    SELECT index_id FROM sys.indexes WHERE object_id=OBJECT_ID(N'dw.fct_trip_leg') AND name=N'UX_fct_trip_leg_grain'
)
ORDER BY ic.key_ordinal
""")
for r in cur.fetchall():
    print(f"  col={r[0]}  ord={r[1]}")

print("\n=== Filas actuales ===")
for tbl in ["dw.fct_trip", "dw.fct_trip_leg", "dw.fct_validation", "dw.fct_boardings_30m"]:
    cur.execute(f"SELECT COUNT(*) FROM {tbl}")
    print(f"  {tbl}: {cur.fetchone()[0]:,}")

print("\n=== Cuts cargados ===")
cur.execute("SELECT dataset_name, cut_id, cut_sk FROM dw.dim_cut ORDER BY cut_sk")
for r in cur.fetchall():
    print(f"  cut_sk={r[2]}  {r[0]}/{r[1]}")

conn.close()
