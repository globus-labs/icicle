#!/usr/bin/env python3
"""
Load the entries table from a SQLite database listing and build a combined
DataFrame containing selected columns.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterator, List, Optional, Sequence

import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)
pd.set_option("display.expand_frame_repr", False)
pd.set_option("display.colheader_justify", "left")

ENTRIES_COLUMNS: tuple[str, ...] = (
    "name",
    "type",
    "mode",
    "uid",
    "gid",
    "size",
    "atime",
    "ctime",
    "mtime",
)

STRING_COLUMNS: tuple[str, ...] = ("path", "type")

INT_COLUMNS: tuple[str, ...] = ("mode", "uid", "gid", "size", "atime", "ctime", "mtime")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-dbdb",
        type=Path,
        required=True,
        help="Path to the db listing file.",
    )
    parser.add_argument(
        "--rows-per-csv",
        type=int,
        help="Maximum number of rows per CSV export before writing and resetting.",
    )
    parser.add_argument(
        "--outfile-path",
        type=Path,
        help="Directory where CSV exports are written.",
    )
    parser.add_argument(
        "--filename-prefix",
        required=True,
        help="Expected prefix for database paths.",
    )
    parser.add_argument(
        "--filename-suffix",
        required=True,
        help="Expected suffix for database paths.",
    )
    return parser.parse_args()


def _contains_surrogate_characters(text: str) -> bool:
    return any("\udc80" <= char <= "\udcff" for char in text)


def read_db_listing(path: Path) -> Iterator[tuple[int, Path, int]]:
    # Use surrogateescape to tolerate stray non-UTF8 bytes while skipping bad lines entirely.
    with path.open(encoding="utf-8", errors="surrogateescape") as handle:
        for line_num, raw in enumerate(handle, start=1):
            if _contains_surrogate_characters(raw):
                print(f"WARN: Skipping line {line_num} in {path} due to undecodable bytes.")
                continue
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.rsplit(",", 1)
            if len(parts) != 2:
                raise ValueError(f"Malformed line {line_num} in {path}: {raw.rstrip()}")
            db_path_str, size_str = (part.strip() for part in parts)
            try:
                db_size = int(size_str)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid size value '{size_str}' on line {line_num} in {path}: {raw.rstrip()}"
                ) from exc
            yield line_num, Path(db_path_str), db_size


def ensure_string_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = df[column].astype("string")
    return df


def ensure_integer_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="raise").astype("int64")
    return df


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def fetch_table_rows(cursor: sqlite3.Cursor, table: str) -> tuple[list[str], list[tuple]]:
    column_list = ", ".join(quote_identifier(col) for col in ENTRIES_COLUMNS)
    query = f"SELECT {column_list} FROM {quote_identifier(table)}"
    cursor.execute(query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return columns, rows


def load_db_rows(
    db_path: Path, *, filename_prefix: str, filename_suffix: str, declared_size: Optional[int] = None
) -> List[dict]:
    """
    Read entries rows from a single SQLite database.
    Returns row dictionaries that can be converted into a DataFrame by the caller.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            entry_columns, db_entry_rows = fetch_table_rows(cursor, "entries")
    except sqlite3.Error as exc:
        raise sqlite3.Error(f"Failed to read entries table from {db_path}: {exc}") from exc

    missing_cols = [col for col in ENTRIES_COLUMNS if col not in entry_columns]
    if missing_cols:
        raise ValueError(f"Missing required columns {missing_cols} in entries table for {db_path}")

    path_str = str(db_path)
    assert path_str.startswith(filename_prefix) and path_str.endswith(
        filename_suffix
    ), f"Unexpected path format: {path_str}"
    suffix_end = len(path_str) - len(filename_suffix) if filename_suffix else len(path_str)
    display_path = path_str[len(filename_prefix) : suffix_end]

    if not db_entry_rows:
        # print(f"WARN: entries table empty for {db_path}")
        return []

    replacement_rows: List[dict] = []
    empty_name_count = 0
    for raw_row in db_entry_rows:
        row_data = dict(zip(entry_columns, raw_row))
        name_value = row_data.get("name")
        if name_value is None or name_value == "":
            empty_name_count += 1
            row_data["name"] = "null"
        replacement_rows.append(row_data)

    if empty_name_count:
        # print(f'WARN: Replacing {empty_name_count} empty name value(s) with "null".')
        pass

    for row_data in replacement_rows:
        for column in ENTRIES_COLUMNS:
            assert row_data.get(column) is not None, f"Null value found in column '{column}' for {db_path}"

    for row_data in replacement_rows:
        row_data["path"] = f"{display_path}/{row_data['name']}"
        row_data.pop("name", None)

    return replacement_rows


def build_dataframe(rows: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = ensure_string_columns(df, STRING_COLUMNS)
    df = ensure_integer_columns(df, INT_COLUMNS)

    ordered_columns = [
        column for column in (
            "type",
            "mode",
            "uid",
            "gid",
            "size",
            "atime",
            "ctime",
            "mtime",
            "path",
        )
        if column in df.columns
    ]
    ordered_columns += [column for column in df.columns if column not in ordered_columns]
    df = df[ordered_columns]

    for column in df.columns:
        assert not df[column].isna().any(), f"Null value(s) found in column '{column}'."

    # print(df)
    return df


def append_frames(frames: list[pd.DataFrame], new_rows: List[dict]) -> None:
    if not new_rows:
        return
    frames.append(build_dataframe(new_rows))


def main() -> None:
    args = parse_args()
    input_path = args.input_dbdb
    if input_path is None:
        raise SystemExit("--input-dbdb is required.")
    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    rows_per_csv = args.rows_per_csv if args.rows_per_csv and args.rows_per_csv > 0 else None
    if args.rows_per_csv is not None and args.rows_per_csv <= 0:
        raise SystemExit("--rows-per-csv must be positive when provided.")

    output_dir: Optional[Path] = args.outfile_path
    if rows_per_csv is not None and output_dir is None:
        raise SystemExit("--outfile-path is required when --rows-per-csv is provided.")

    filename_prefix = args.filename_prefix
    filename_suffix = args.filename_suffix
    if filename_prefix is None or filename_suffix is None:
        raise SystemExit("--filename-prefix and --filename-suffix are required.")

    entries_frames: list[pd.DataFrame] = []
    processed_with_rows = 0
    empty_tables = 0
    exception_count = 0
    csv_files_written = 0
    total_rows_loaded = 0
    current_batch_rows = 0
    should_print_db_info = True

    if rows_per_csv is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    for index, (line_num, db_path, declared_size) in enumerate(read_db_listing(input_path)):
        try:
            entry_rows = load_db_rows(
                db_path, filename_prefix=filename_prefix, filename_suffix=filename_suffix, declared_size=declared_size
            )
        except FileNotFoundError as exc:
            print(f"SKIP: {exc}")
            exception_count += 1
            continue
        except (sqlite3.Error, ValueError) as exc:
            print(f"WARN: {exc}")
            exception_count += 1
            continue

        if should_print_db_info:
            print(f"INFO: listing line {line_num} -> {db_path}")
            should_print_db_info = False

        if not entry_rows:
            empty_tables += 1
            continue

        processed_with_rows += 1
        append_frames(entries_frames, entry_rows)

        latest_frame = entries_frames[-1]
        chunk_rows = latest_frame.shape[0]
        current_batch_rows += chunk_rows
        total_rows_loaded += chunk_rows

        if rows_per_csv is not None and current_batch_rows >= rows_per_csv:
            batch_df = pd.concat(entries_frames, ignore_index=True)
            csv_path = output_dir / f"{csv_files_written + 1}.csv"
            batch_df.to_csv(csv_path, index=False, header=False)
            print(f"Wrote {len(batch_df)} row(s) to {csv_path}.")
            csv_files_written += 1
            entries_frames.clear()
            current_batch_rows = 0
            should_print_db_info = True

    if rows_per_csv is not None and entries_frames:
        batch_df = pd.concat(entries_frames, ignore_index=True)
        csv_path = output_dir / f"{csv_files_written + 1}.csv"
        batch_df.to_csv(csv_path, index=False, header=False)
        print(f"Wrote {len(batch_df)} row(s) to {csv_path}.")
        csv_files_written += 1
        entries_frames.clear()
        current_batch_rows = 0
        should_print_db_info = True

    if entries_frames:
        in_memory_df = pd.concat(entries_frames, ignore_index=True)
        print(
            f"Loaded {len(in_memory_df)} entry row(s) across {len(entries_frames)} chunk(s) "
            f"(pending write)."
        )
    else:
        print("No entry rows remain in memory (all written to CSV or no data).")

    print(
        f"Processed {processed_with_rows} database(s) with rows, "
        f"{empty_tables} empty entries table(s), "
        f"{exception_count} database(s) raised exceptions, "
        f"{total_rows_loaded} total row(s) loaded, "
        f"{csv_files_written} CSV file(s) written."
    )


if __name__ == "__main__":
    main()
