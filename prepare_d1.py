#!/usr/bin/env python3
"""Convert ONRC datasets to SQL for Cloudflare D1 import."""

import csv
import sys
from pathlib import Path

BASE = Path(__file__).parent
OUTPUT_DIR = BASE / "sql_chunks"
BATCH_SIZE = 500
CHUNK_SIZE = 80_000  # rows per SQL file


def escape(s):
    if not s:
        return ""
    return s.replace("'", "''")


def write_schema():
    schema = """\
DROP TABLE IF EXISTS firme;
DROP TABLE IF EXISTS stare_firma;
DROP TABLE IF EXISTS caen_autorizat;
DROP TABLE IF EXISTS n_stare;
DROP TABLE IF EXISTS n_caen;

CREATE TABLE firme (
    denumire TEXT NOT NULL,
    cui TEXT,
    cod_inmatriculare TEXT PRIMARY KEY,
    data_inmatriculare TEXT,
    euid TEXT,
    forma_juridica TEXT,
    judet TEXT,
    localitate TEXT,
    adresa TEXT,
    cod_postal TEXT,
    sector TEXT,
    web TEXT
);
CREATE INDEX idx_firme_denumire ON firme(denumire);
CREATE INDEX idx_firme_cui ON firme(cui);

CREATE TABLE stare_firma (
    cod_inmatriculare TEXT,
    cod INTEGER
);
CREATE INDEX idx_stare_cod ON stare_firma(cod_inmatriculare);

CREATE TABLE caen_autorizat (
    cod_inmatriculare TEXT,
    cod_caen TEXT
);
CREATE INDEX idx_caen_cod ON caen_autorizat(cod_inmatriculare);

CREATE TABLE n_stare (
    cod INTEGER PRIMARY KEY,
    denumire TEXT
);

CREATE TABLE n_caen (
    clasa TEXT PRIMARY KEY,
    denumire TEXT,
    sectiunea TEXT
);
"""
    (BASE / "schema.sql").write_text(schema)
    print("Schema written")


def chunked_writer(table, columns, prefix="firme"):
    """Generator-based chunked SQL writer."""
    chunk_num = 0
    rows_in_chunk = 0
    f_out = None
    batch = []
    total = 0
    cols = ",".join(columns)

    def flush_batch():
        nonlocal f_out, chunk_num, rows_in_chunk
        if not batch:
            return
        if f_out is None or rows_in_chunk >= CHUNK_SIZE:
            if f_out:
                f_out.close()
            chunk_num += 1
            f_out = open(OUTPUT_DIR / f"{prefix}_{chunk_num:03d}.sql", "w", encoding="utf-8")
            rows_in_chunk = 0
        f_out.write(f"INSERT INTO {table} ({cols}) VALUES\n")
        f_out.write(",\n".join(batch))
        f_out.write(";\n")
        rows_in_chunk += len(batch)
        batch.clear()

    class Writer:
        def add(self, values):
            nonlocal total
            batch.append("(" + ",".join(f"'{v}'" for v in values) + ")")
            total += 1
            if len(batch) >= BATCH_SIZE:
                flush_batch()
            if total % 200_000 == 0:
                sys.stdout.write(f"\r  {table}: {total:,} rows, chunk {chunk_num}")
                sys.stdout.flush()

        def finish(self):
            flush_batch()
            if f_out:
                f_out.close()
            print(f"\r  {table}: {total:,} rows across {chunk_num} chunks")

    return Writer()


def process_firme():
    print("Processing od_firme.csv...")
    w = chunked_writer("firme", [
        "denumire", "cui", "cod_inmatriculare", "data_inmatriculare", "euid",
        "forma_juridica", "judet", "localitate", "adresa", "cod_postal", "sector", "web"
    ], prefix="firme")

    with open(BASE / "od_firme.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)  # skip header
        for row in reader:
            if len(row) < 19:
                continue
            denumire = escape(row[0].strip())
            if not denumire:
                continue
            # Build address from parts
            parts = []
            if row[9].strip():
                parts.append(row[9].strip())
            if row[10].strip():
                parts.append(row[10].strip())
            if row[11].strip():
                parts.append(f"Bl. {row[11].strip()}")
            if row[12].strip():
                parts.append(f"Sc. {row[12].strip()}")
            if row[13].strip():
                parts.append(f"Et. {row[13].strip()}")
            if row[14].strip():
                parts.append(f"Ap. {row[14].strip()}")
            adresa = escape(", ".join(parts))

            w.add([
                denumire,
                escape(row[1].strip()),   # cui
                escape(row[2].strip()),   # cod_inmatriculare
                escape(row[3].strip()),   # data_inmatriculare
                escape(row[4].strip()),   # euid
                escape(row[5].strip()),   # forma_juridica
                escape(row[7].strip()),   # judet
                escape(row[8].strip()),   # localitate
                adresa,
                escape(row[15].strip()),  # cod_postal
                escape(row[16].strip()),  # sector
                escape(row[18].strip()),  # web
            ])
    w.finish()


def process_stare():
    print("Processing od_stare_firma.csv...")
    w = chunked_writer("stare_firma", ["cod_inmatriculare", "cod"], prefix="stare")

    with open(BASE / "od_stare_firma.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)
        for row in reader:
            if len(row) < 2:
                continue
            w.add([escape(row[0].strip()), escape(row[1].strip())])
    w.finish()


def process_caen():
    print("Processing od_caen_autorizat.csv...")
    w = chunked_writer("caen_autorizat", ["cod_inmatriculare", "cod_caen"], prefix="caen")

    with open(BASE / "od_caen_autorizat.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)
        for row in reader:
            if len(row) < 2:
                continue
            w.add([escape(row[0].strip()), escape(row[1].strip())])
    w.finish()


def process_nomenclators():
    print("Processing nomenclators...")

    # Status nomenclator
    lines = ["INSERT INTO n_stare (cod, denumire) VALUES"]
    with open(BASE / "n_stare_firma.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)
        vals = []
        for row in reader:
            if len(row) < 2:
                continue
            vals.append(f"({row[0].strip()},'{escape(row[1].strip())}')")
        lines.append(",\n".join(vals) + ";")
    (OUTPUT_DIR / "nom_stare.sql").write_text("\n".join(lines))
    print(f"  n_stare: {len(vals)} entries")

    # CAEN nomenclator
    lines = ["INSERT INTO n_caen (clasa, denumire, sectiunea) VALUES"]
    with open(BASE / "n_caen.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)
        vals = []
        for row in reader:
            if len(row) < 6:
                continue
            clasa = row[4].strip()
            if not clasa:
                continue
            vals.append(f"('{escape(clasa)}','{escape(row[5].strip())}','{escape(row[0].strip())}')")
        lines.append(",\n".join(vals) + ";")
    (OUTPUT_DIR / "nom_caen.sql").write_text("\n".join(lines))
    print(f"  n_caen: {len(vals)} entries")


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    # Clean old chunks
    for f in OUTPUT_DIR.glob("*.sql"):
        f.unlink()

    write_schema()
    process_firme()
    process_stare()
    process_caen()
    process_nomenclators()
    print("\nAll done!")


if __name__ == "__main__":
    main()
