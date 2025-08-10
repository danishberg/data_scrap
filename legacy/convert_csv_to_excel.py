#!/usr/bin/env python3
"""CSV → Excel converter

Usage:
    python convert_csv_to_excel.py path\to\file1.csv [file2.csv ...]

Creates an .xlsx file next to each CSV (same base name).
"""
import sys
import pathlib
import pandas as pd

def convert(csv_path: pathlib.Path):
    if csv_path.suffix.lower() != ".csv":
        print(f"⚠️ Skipping {csv_path} (not a CSV)")
        return
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    xlsx_path = csv_path.with_suffix(".xlsx")
    try:
        df = pd.read_csv(csv_path)
        df.to_excel(xlsx_path, index=False)
        # Use str() to avoid pathlib.relative_to errors on Windows when
        # mixing relative/absolute paths.
        print(f"✅ Saved {xlsx_path}")
    except Exception as e:
        print(f"❌ Failed to convert {csv_path}: {e}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_csv_to_excel.py <csv_or_pattern1> [pattern2 ...]")
        sys.exit(1)

    import glob

    for pattern in sys.argv[1:]:
        matched = glob.glob(pattern)
        if not matched:
            print(f"❌ No files match pattern: {pattern}")
            continue
        for file_path in matched:
            convert(pathlib.Path(file_path))

if __name__ == "__main__":
    main() 