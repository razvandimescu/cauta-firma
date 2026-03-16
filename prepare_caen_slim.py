#!/usr/bin/env python3
"""Extract max 5 CAEN codes per company to keep D1 under size limits."""

import csv
import sys
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent
OUTPUT_DIR = BASE / "sql_chunks"
BATCH_SIZE = 500
CHUNK_SIZE = 80_000
MAX_PER_COMPANY = 5


def escape(s):
    return s.replace("'", "''") if s else ""


def main():
    print("Processing od_caen_autorizat.csv (max 5 per company)...")
    counts = defaultdict(int)
    total = 0
    skipped = 0
    chunk_num = 0
    rows_in_chunk = 0
    f_out = None
    batch = []

    with open(BASE / "od_caen_autorizat.csv", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="^")
        next(reader)
        for row in reader:
            if len(row) < 2:
                continue
            cod_inm = row[0].strip()
            cod_caen = row[1].strip()
            if not cod_inm or not cod_caen:
                continue

            counts[cod_inm] += 1
            if counts[cod_inm] > MAX_PER_COMPANY:
                skipped += 1
                continue

            batch.append(f"('{escape(cod_inm)}','{escape(cod_caen)}')")
            total += 1
            rows_in_chunk += 1

            if len(batch) >= BATCH_SIZE:
                if f_out is None or rows_in_chunk >= CHUNK_SIZE:
                    if f_out:
                        f_out.close()
                    chunk_num += 1
                    f_out = open(OUTPUT_DIR / f"caen_{chunk_num:03d}.sql", "w", encoding="utf-8")
                    rows_in_chunk = len(batch)
                f_out.write("INSERT INTO caen_autorizat (cod_inmatriculare,cod_caen) VALUES\n")
                f_out.write(",\n".join(batch))
                f_out.write(";\n")
                batch = []

            if total % 200_000 == 0:
                sys.stdout.write(f"\r  {total:,} kept, {skipped:,} trimmed, chunk {chunk_num}")
                sys.stdout.flush()

    if batch:
        if f_out is None:
            chunk_num += 1
            f_out = open(OUTPUT_DIR / f"caen_{chunk_num:03d}.sql", "w", encoding="utf-8")
        f_out.write("INSERT INTO caen_autorizat (cod_inmatriculare,cod_caen) VALUES\n")
        f_out.write(",\n".join(batch))
        f_out.write(";\n")
    if f_out:
        f_out.close()

    print(f"\n  caen_autorizat: {total:,} rows kept, {skipped:,} trimmed, {chunk_num} chunks")


if __name__ == "__main__":
    main()
