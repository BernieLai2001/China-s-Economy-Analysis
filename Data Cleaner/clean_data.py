#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import pandas as pd
import re
import argparse
import numpy as np

DATE_COLUMN = "date"
SUPPORTED_SUFFIXES = (".csv", ".xlsx", ".xls")
OUTPUT_DATA = "merged_clean_data.csv"


# =====================
# READ
# =====================

def read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path, header=None)


# =====================
# TIME UTIL
# =====================

def looks_like_time(x) -> bool:
    if pd.isna(x):
        return False
    s = str(x).strip()
    patterns = [
        r"^\d{4}$",
        r"^\d{4}[-/]\d{1,2}$",
        r"^\d{4}[-/]\d{1,2}[-/]\d{1,2}$",
        r"^\d{4}Q[1-4]$",
    ]
    return any(re.match(p, s) for p in patterns)


def normalize_time(x) -> str | None:
    if pd.isna(x):
        return None
    s = str(x).strip()

    if re.match(r"^\d{4}$", s):
        return f"{s}-01-01"
    if re.match(r"^\d{4}[-/]\d{1,2}$", s):
        y, m = re.split("[-/]", s)
        return f"{y}-{m.zfill(2)}-01"
    if re.match(r"^\d{4}Q[1-4]$", s):
        y = s[:4]
        q = int(s[-1])
        return f"{y}-{q*3:02d}-01"
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except:
        return None


# =====================
# CORE EXTRACT
# =====================

def extract_time_series(df: pd.DataFrame) -> pd.DataFrame | None:
    df = df.dropna(how="all")

    # ---------- Case 1: 第一列是时间 ----------
    first_col = df.iloc[:, 0]
    if first_col.map(looks_like_time).sum() >= 3:
        out = df.copy()
        out.rename(columns={out.columns[0]: DATE_COLUMN}, inplace=True)
        out[DATE_COLUMN] = out[DATE_COLUMN].map(normalize_time)
        out = out.dropna(subset=[DATE_COLUMN])

        # 保留数值列
        value_cols = out.columns.difference([DATE_COLUMN])
        out[value_cols] = out[value_cols].apply(
            pd.to_numeric, errors="coerce"
        )

        return out

    # ---------- Case 2: 第一行是时间 ----------
    first_row = df.iloc[0]
    if first_row.map(looks_like_time).sum() >= 3:
        dates = first_row.map(normalize_time)
        data = df.iloc[1:].T
        data.columns = df.iloc[1].astype(str)

        out = data.copy()
        out.insert(0, DATE_COLUMN, dates)
        out = out.dropna(subset=[DATE_COLUMN])

        out.iloc[:, 1:] = out.iloc[:, 1:].apply(
            pd.to_numeric, errors="coerce"
        )

        return out

    return None


# =====================
# MAIN
# =====================

def main(data_dir: Path):
    files = sorted(
        f for f in data_dir.iterdir()
        if f.is_file() and f.suffix.lower() in SUPPORTED_SUFFIXES
    )

    print(f"\n📂 Scanning directory: {data_dir.resolve()}")
    print(f"📄 Found {len(files)} data files\n")

    merged = None

    for file in files:
        print(f"➡️ Processing {file.name}")
        try:
            df_raw = read_file(file)
            ts = extract_time_series(df_raw)

            if ts is None or ts.empty:
                print(f"⚠️ Skipped {file.name}: could not detect time structure")
                continue

            # 防止 column 重名
            if merged is not None:
                overlap = set(ts.columns) & set(merged.columns) - {DATE_COLUMN}
                if overlap:
                    ts = ts.rename(
                        columns={
                            c: f"{c}__{file.stem}"
                            for c in overlap
                        }
                    )

            merged = ts if merged is None else pd.merge(
                merged, ts, on=DATE_COLUMN, how="outer"
            )

        except Exception as e:
            print(f"⚠️ Skipped {file.name}: {e}")

    if merged is None:
        raise RuntimeError("No data merged")

    merged = merged.sort_values(DATE_COLUMN)
    merged.to_csv(OUTPUT_DATA, index=False)

    print("\n✅ SUCCESS")
    print(f"📄 Output: {OUTPUT_DATA}")
    print(f"📊 Shape: {merged.shape}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", type=Path, default=Path("."))
    args = parser.parse_args()

    main(args.dir)

