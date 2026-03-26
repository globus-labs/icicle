#!/usr/bin/env python3
"""
Process an mmapplypolicy listing, coerce the relevant columns into strong types,
and export the results into sequential CSV batches without headers.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterator, List
from urllib.parse import unquote

import pandas as pd

from mode_process import sym_to_mode

N_COLS = 18
MAX_LINE_LENGTH = 2_000

COLUMN_ORDER = [
    "type",
    "mode",
    "user_id",
    "group_id",
    "file_size",
    "atime",
    "ctime",
    "mtime",
    "fileset_name",
    "filename",
]

INT_COLUMNS = (
    "mode",
    "user_id",
    "group_id",
    "file_size",
    "atime",
    "ctime",
    "mtime",
)
STRING_COLUMNS = tuple(col for col in COLUMN_ORDER if col not in INT_COLUMNS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-tsv",
        type=Path,
        required=True,
        help="Path to the mmapplypolicy output file.",
    )
    parser.add_argument(
        "--rows-per-csv",
        type=int,
        required=True,
        help="Maximum number of rows per CSV export before writing and resetting.",
    )
    parser.add_argument(
        "--outfile-path",
        type=Path,
        required=True,
        help="Directory where CSV exports are written.",
    )
    parser.add_argument(
        "--filename-prefix",
        type=str,
        required=True,
        help="Prefix to remove from filenames before writing.",
    )
    return parser.parse_args()


def normalize_filename(raw: str, filename_prefix: str) -> str:
    """Drop the provided prefix from the filename when present."""
    decoded = unquote(raw)
    if filename_prefix and decoded.startswith(filename_prefix):
        decoded = decoded[len(filename_prefix) :]
    return decoded


def parse_line(line: str, filename_prefix: str) -> Dict[str, str] | None:
    if len(line) > MAX_LINE_LENGTH:
        return None

    cols = line.strip().split(" ", N_COLS - 1)
    if len(cols) != N_COLS:
        return None

    mode_field = cols[11]
    if not mode_field:
        return None

    type_char = mode_field[0]
    if type_char not in "-l":
        return None

    entry_type = "f" if type_char == "-" else "l"
    filename = normalize_filename(cols[-1], filename_prefix)

    row = {
        "type": entry_type,
        "mode": sym_to_mode(mode_field),
        "user_id": int(cols[9]),
        "group_id": int(cols[10]),
        "file_size": int(cols[4]),
        "atime": int(cols[12]),
        "ctime": int(cols[13]),
        "mtime": int(cols[15]),
        "fileset_name": cols[5],
        "filename": filename,
    }
    return row


def rows_to_dataframe(rows: List[Dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=COLUMN_ORDER)
    for column in INT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="raise").astype("int64")
    for column in STRING_COLUMNS:
        df[column] = df[column].astype("string")
    return df


def write_batch(rows: List[Dict[str, object]], output_dir: Path, csv_index: int) -> int:
    if not rows:
        return csv_index

    batch_df = rows_to_dataframe(rows)
    csv_path = output_dir / f"{csv_index + 1}.csv"
    batch_df.to_csv(csv_path, index=False, header=False)
    print(f"Wrote {len(batch_df)} row(s) to {csv_path}.")
    return csv_index + 1


def iter_rows(path: Path, filename_prefix: str) -> Iterator[Dict[str, object]]:
    with path.open("r", buffering=1024 * 1024) as handle:
        for raw_line in handle:
            parsed = parse_line(raw_line, filename_prefix)
            if parsed is None:
                continue
            yield parsed


def main() -> None:
    args = parse_args()
    input_path: Path = args.input_tsv
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    rows_per_csv = args.rows_per_csv
    if rows_per_csv is None or rows_per_csv <= 0:
        raise SystemExit("--rows-per-csv must be a positive integer.")

    output_dir: Path = args.outfile_path
    output_dir.mkdir(parents=True, exist_ok=True)

    filename_prefix = args.filename_prefix or ""

    pending_rows: List[Dict[str, object]] = []
    csv_files_written = 0
    total_rows_processed = 0

    for row in iter_rows(input_path, filename_prefix):
        pending_rows.append(row)
        total_rows_processed += 1
        if len(pending_rows) >= rows_per_csv:
            csv_files_written = write_batch(pending_rows, output_dir, csv_files_written)
            pending_rows.clear()

    if pending_rows:
        csv_files_written = write_batch(pending_rows, output_dir, csv_files_written)

    print(
        f"Processed {total_rows_processed} row(s) "
        f"and wrote {csv_files_written} CSV file(s) to {output_dir}."
    )


if __name__ == "__main__":
    main()
