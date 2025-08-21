# -*- coding: utf-8 -*-
"""
Homework Utilities — English-Commented Version
=============================================

This script contains two tasks from the file-handling homework:

Task 1 — Convert `purchase_log.txt` into a dictionary `purchases` of the form:
    { "1840e0b9d4": "Продукты", ... }

Notes:
- `purchase_log.txt` is a newline-delimited JSON (NDJSON) file.
- Its header line must be skipped.

Task 2 — Build a `funnel.csv` file by line-wise processing of `visit_log.csv`:
- For each `user_id` found in `visit_log.csv`, append the *purchase category* (if any)
  using the `purchases` dictionary from Task 1.
- **Do not** load the entire `visit_log.csv` into memory. Process it row by row.
- Write only the visits that resulted in a purchase to `funnel.csv` (as requested).

Why the design below?
- Clear separation into small, testable functions.
- Memory-safe streaming I/O for the large `visit_log.csv` (meets the requirement).
- Robust error handling with explicit exceptions and user-friendly messages.
- PEP 8–compliant, with type hints and English comments for GitHub.

Run (defaults assume files are in the current working directory):
    python files_homework_en.py         --purchase-log purchase_log.txt         --visit-log visit_log.csv         --output funnel.csv

The script prints a short summary and an example output row (if any).
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

# -----------------------------
# Task 1: purchases dictionary
# -----------------------------

def load_purchases_ndjson(path: Path) -> Dict[str, str]:
    """
    Load purchases from newline-delimited JSON file (purchase_log.txt).

    The file format is:
        <header line to skip>
        {"user_id": "...", "category": "..."}
        {"user_id": "...", "category": "..."}
        ...

    Parameters
    ----------
    path : Path
        Path to purchase_log.txt

    Returns
    -------
    Dict[str, str]
        Mapping user_id -> category

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If a JSON line is malformed or lacks required keys.
    """
    if not path.exists():
        raise FileNotFoundError(f"purchase log not found: {path}")

    purchases: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        # Skip header line
        header = f.readline()

        for ln, line in enumerate(f, start=2):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON at line {ln}: {e}") from e

            uid = data.get("user_id")
            cat = data.get("category")
            if uid is None or cat is None:
                raise ValueError(f"Missing keys at line {ln}: {data}")

            purchases[str(uid)] = str(cat)

    return purchases


# -------------------------------------
# Task 2: funnel.csv from visit_log.csv
# -------------------------------------

def build_funnel(
    visit_log_path: Path,
    purchases: Dict[str, str],
    output_csv_path: Path,
) -> Tuple[int, Optional[List[str]]]:
    """
    Create `funnel.csv` with visits that resulted in a purchase.

    Requirements satisfied:
    - Stream the `visit_log.csv` file line by line (no full-file loading).
    - Do **not** modify `visit_log.csv` itself.
    - Output only visits for which a purchase category exists.

    Input format (visit_log.csv):
        user_id,source
        <id_1>,<source_1>
        <id_2>,<source_2>
        ...

    Output format (funnel.csv):
        user_id,source,category

    Parameters
    ----------
    visit_log_path : Path
        Path to visit_log.csv
    purchases : Dict[str, str]
        Dict mapping user_id -> category, built from Task 1
    output_csv_path : Path
        Path to funnel.csv to write

    Returns
    -------
    Tuple[int, Optional[List[str]]]
        (rows_written, example_row_as_list_or_None)
    """
    if not visit_log_path.exists():
        raise FileNotFoundError(f"visit log not found: {visit_log_path}")

    rows_written = 0
    example_row: Optional[List[str]] = None

    with visit_log_path.open("r", encoding="utf-8", newline="") as fin,          output_csv_path.open("w", encoding="utf-8", newline="") as fout:

        reader = csv.reader(fin)
        writer = csv.writer(fout)

        # Expect the header: user_id,source
        try:
            header = next(reader)
        except StopIteration:
            # Empty file — write just the header and return
            writer.writerow(["user_id", "source", "category"])
            return (0, None)

        # Be defensive about header casing and whitespace
        header_norm = [h.strip().lower() for h in header]
        try:
            idx_user = header_norm.index("user_id")
            idx_source = header_norm.index("source")
        except ValueError:
            # If header is weird/missing, assume the first two columns
            idx_user, idx_source = 0, 1

        # Write our output header
        writer.writerow(["user_id", "source", "category"])

        for row in reader:
            if not row:
                continue
            # Guard against short lines
            if len(row) <= max(idx_user, idx_source):
                continue

            user_id = row[idx_user].strip()
            source = row[idx_source].strip()
            category = purchases.get(user_id)

            # Only write rows with a purchase
            if category:
                out_row = [user_id, source, category]
                writer.writerow(out_row)
                rows_written += 1
                if example_row is None:
                    example_row = out_row

    return (rows_written, example_row)


# -----------------
# Command-line main
# -----------------

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for paths; provide sane defaults."""
    p = argparse.ArgumentParser(description="Build purchases dict and funnel.csv (English-commented).")
    p.add_argument("--purchase-log", type=Path, default=Path("purchase_log.txt"),
                   help="Path to purchase_log.txt (NDJSON with header).")
    p.add_argument("--visit-log", type=Path, default=Path("visit_log.csv"),
                   help="Path to visit_log.csv (CSV with header 'user_id,source').")
    p.add_argument("--output", type=Path, default=Path("funnel.csv"),
                   help="Where to write funnel.csv.")
    return p.parse_args()


def main() -> None:
    """Orchestrate Task 1 and Task 2; print a short summary."""
    args = parse_args()

    try:
        purchases = load_purchases_ndjson(args.purchase_log)
    except Exception as e:
        print(f"[ERROR] Failed to load purchases: {e}")
        return

    try:
        rows_written, example = build_funnel(args.visit_log, purchases, args.output)
    except Exception as e:
        print(f"[ERROR] Failed to build funnel: {e}")
        return

    print(f"[OK] Wrote {rows_written} rows to: {args.output}")
    if example:
        print(f"[Example] {example}")
    else:
        print("[Info] No matches found; output contains only the header.")

if __name__ == "__main__":
    main()
