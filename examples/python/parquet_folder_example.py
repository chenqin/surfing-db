#!/usr/bin/env python3
"""
Example demonstrating Parquet folder read/write operations using PyArrow.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit("pyarrow is required: " + str(exc)) from exc


def read_parquet_folder(folder_path: str) -> List[pa.Table]:
    """
    Read all Parquet files from a directory.

    Args:
        folder_path: Path to directory containing Parquet files

    Returns:
        List of PyArrow Tables, one per Parquet file
    """
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Path does not exist or is not a directory: {folder_path}")

    tables = []
    parquet_files = sorted(folder.glob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in {folder_path}")
        return tables

    for parquet_file in parquet_files:
        table = pq.read_table(str(parquet_file))
        tables.append(table)
        print(f"Read {parquet_file.name}: {table.num_rows} rows, {table.num_columns} columns")

    return tables


def write_parquet_folder(tables: List[pa.Table], folder_path: str, prefix: str = "part") -> List[str]:
    """
    Write a list of PyArrow Tables to Parquet files in a directory.

    Args:
        tables: List of PyArrow Tables to write
        folder_path: Output directory path
        prefix: Filename prefix (default: "part")

    Returns:
        List of written file paths
    """
    folder = Path(folder_path)
    folder.mkdir(parents=True, exist_ok=True)

    written_files = []

    for i, table in enumerate(tables):
        if table is None or table.num_rows == 0:
            continue

        filename = f"{prefix}_{i:05d}.parquet"
        filepath = folder / filename

        pq.write_table(
            table,
            str(filepath),
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )

        written_files.append(str(filepath))
        print(f"Wrote {filename}: {table.num_rows} rows, {table.num_columns} columns")

    return written_files


def create_sample_table(num_rows: int, batch_id: int) -> pa.Table:
    """
    Create a sample PyArrow Table for demonstration.

    Args:
        num_rows: Number of rows to generate
        batch_id: Batch identifier

    Returns:
        PyArrow Table with sample data
    """
    import random

    data = {
        'id': list(range(batch_id * num_rows, (batch_id + 1) * num_rows)),
        'batch_id': [batch_id] * num_rows,
        'value': [random.random() * 100 for _ in range(num_rows)],
        'name': [f"item_{i}" for i in range(num_rows)],
    }

    return pa.Table.from_pydict(data)


def count_total_rows(folder_path: str) -> int:
    """
    Count total rows across all Parquet files in a folder.

    Args:
        folder_path: Path to directory containing Parquet files

    Returns:
        Total row count
    """
    folder = Path(folder_path)
    if not folder.exists() or not folder.is_dir():
        raise ValueError(f"Path does not exist or is not a directory: {folder_path}")

    total_rows = 0
    parquet_files = folder.glob("*.parquet")

    for parquet_file in parquet_files:
        parquet_metadata = pq.read_metadata(str(parquet_file))
        total_rows += parquet_metadata.num_rows

    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Parquet folder read/write example with PyArrow"
    )
    parser.add_argument(
        "--mode",
        choices=["write", "read", "roundtrip"],
        default="roundtrip",
        help="Operation mode (default: roundtrip)"
    )
    parser.add_argument(
        "--input",
        default="parquet_input",
        help="Input folder path (for read mode)"
    )
    parser.add_argument(
        "--output",
        default="parquet_output",
        help="Output folder path (for write mode)"
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=3,
        help="Number of Parquet files to generate (default: 3)"
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=1000,
        help="Rows per file (default: 1000)"
    )

    args = parser.parse_args()

    if args.mode == "write" or args.mode == "roundtrip":
        print(f"\n=== Writing {args.num_files} Parquet files to {args.output} ===")
        tables = []
        for i in range(args.num_files):
            table = create_sample_table(args.rows_per_file, i)
            tables.append(table)

        written_files = write_parquet_folder(tables, args.output)
        print(f"\nWrote {len(written_files)} files to {args.output}")

        total_rows = count_total_rows(args.output)
        print(f"Total rows: {total_rows}")

    if args.mode == "read" or args.mode == "roundtrip":
        input_folder = args.output if args.mode == "roundtrip" else args.input
        print(f"\n=== Reading Parquet files from {input_folder} ===")

        tables = read_parquet_folder(input_folder)
        print(f"\nRead {len(tables)} tables")

        if tables:
            # Concatenate all tables
            combined = pa.concat_tables(tables)
            print(f"Combined table: {combined.num_rows} rows, {combined.num_columns} columns")
            print(f"Schema:\n{combined.schema}")

            # Show sample data
            print(f"\nFirst 5 rows:")
            print(combined.slice(0, min(5, combined.num_rows)).to_pandas())

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
