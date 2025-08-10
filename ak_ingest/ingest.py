#!/usr/bin/env python3
import argparse
import os
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import yaml

try:
    import akshare as ak
except Exception as e:  # pragma: no cover
    print("[ERROR] AkShare not installed. Please install requirements first.")
    raise


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def to_datetime_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def safe_ak_call(func_name: str, kwargs: Dict[str, Any]) -> pd.DataFrame:
    func = getattr(ak, func_name)
    last_err: Optional[Exception] = None
    for attempt in range(5):
        try:
            df = func(**kwargs)
            if isinstance(df, pd.DataFrame):
                return df
            # Some APIs might return non-DataFrame structures
            return pd.DataFrame(df)
        except Exception as e:  # pragma: no cover
            last_err = e
            sleep_s = 1.0 + attempt * 1.0
            print(f"[WARN] {func_name} failed on attempt {attempt+1}: {e}. Sleep {sleep_s:.1f}s")
            time.sleep(sleep_s)
    assert last_err is not None
    raise last_err


def fetch_index_constituents(index_code: str) -> pd.DataFrame:
    df = safe_ak_call("index_stock_cons", {"symbol": index_code})
    # Standardize common column names, but keep raw as-is for traceability
    if "品种代码" in df.columns and "代码" not in df.columns:
        df = df.rename(columns={"品种代码": "代码"})
    df["index_code"] = index_code
    return df


def normalize_dataframe(
    df: pd.DataFrame,
    symbol: Optional[str],
    rename_map: Optional[Dict[str, str]],
    add_cols: Optional[Dict[str, Any]],
) -> pd.DataFrame:
    df = df.copy()
    if rename_map:
        df = df.rename(columns=rename_map)
    # Ensure symbol column
    if symbol is not None and "symbol" not in df.columns:
        df["symbol"] = symbol
    # Ensure date/datetime normalization if there is a 'date' column
    if "date" in df.columns:
        df["date"] = to_datetime_series(df["date"]).dt.tz_localize(None)
        df["year"] = df["date"].dt.year
    elif "datetime" in df.columns:
        df["datetime"] = to_datetime_series(df["datetime"]).dt.tz_localize(None)
        df["year"] = df["datetime"].dt.year
    if add_cols:
        for k, v in add_cols.items():
            df[k] = v
    return df


def write_partitioned_parquet(df: pd.DataFrame, base_path: str, partition_by: List[str]) -> None:
    ensure_dir(base_path)
    # Pandas partitioned write requires the partition columns to exist
    for col in partition_by:
        if col not in df.columns:
            if col == "year" and "date" in df.columns:
                df[col] = df["date"].dt.year
            else:
                df[col] = "unknown"
    df.to_parquet(
        base_path,
        engine="pyarrow",
        partition_cols=partition_by,
        compression="zstd",
        index=False,
    )


def expand_symbols_from_source(entry: Dict[str, Any]) -> List[str]:
    if entry.get("type") == "index_constituents":
        index_code = entry["index_code"]
        field = entry.get("field", "代码")
        cons = fetch_index_constituents(index_code)
        symbols = cons[field].dropna().astype(str).unique().tolist()
        return symbols
    raise ValueError(f"Unsupported symbols_source type: {entry}")


def render_params(params: Dict[str, Any], variables: Dict[str, str]) -> Dict[str, Any]:
    rendered: Dict[str, Any] = {}
    for k, v in params.items():
        if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
            key = v[2:-1]
            rendered[k] = variables.get(key, "")
        else:
            rendered[k] = v
    return rendered


def run_dataset(
    ds: Dict[str, Any],
    variables: Dict[str, str],
    global_defaults: Dict[str, Any],
    output_root_override: Optional[str],
) -> None:
    api_name: str = ds["api"]
    params: Dict[str, Any] = render_params(ds.get("params", {}), variables)
    rename_map: Dict[str, str] = ds.get("rename", {})
    add_cols: Dict[str, Any] = ds.get("add_cols", {})
    write_cfg: Dict[str, Any] = {**global_defaults.get("write", {}), **ds.get("write", {})}

    if output_root_override:
        write_cfg["path"] = write_cfg["path"].replace("${output_root}", output_root_override)
    else:
        write_cfg["path"] = write_cfg["path"].replace("${output_root}", variables.get("output_root", ""))

    symbols: List[str]
    if "symbols" in ds:
        symbols = [str(x) for x in ds["symbols"]]
    elif "symbols_source" in ds:
        symbols = expand_symbols_from_source(ds["symbols_source"])
    else:
        symbols = [None]  # APIs without symbol dimension

    # Index constituents dataset special-case: we write as-is
    if api_name == "index_stock_cons":
        index_code = params.get("symbol")
        df = fetch_index_constituents(index_code)
        write_partitioned_parquet(df, write_cfg["path"], write_cfg.get("partition_by", []))
        print(f"[OK] Wrote index constituents for {index_code} -> {write_cfg['path']}")
        return

    # Generic symbol loop
    for i, sym in enumerate(symbols, 1):
        call_kwargs = dict(params)
        if sym is not None:
            call_kwargs["symbol"] = sym
        df = safe_ak_call(api_name, call_kwargs)
        if df is None or df.empty:
            print(f"[WARN] No data for {api_name} symbol={sym}")
            continue
        ndf = normalize_dataframe(df, sym, rename_map, add_cols)
        write_partitioned_parquet(ndf, write_cfg["path"], write_cfg.get("partition_by", []))
        print(f"[OK] {api_name} symbol={sym} rows={len(ndf)} -> {write_cfg['path']}")
        time.sleep(0.4 + (i % 5) * 0.1)  # simple rate-limit


def main() -> None:
    parser = argparse.ArgumentParser(description="AkShare ingestion CLI")
    parser.add_argument("--config", required=True, help="Path to YAML config")
    parser.add_argument("--start", dest="start_date", required=False, default="20180101")
    parser.add_argument("--end", dest="end_date", required=False, default="")
    parser.add_argument("--output-root", dest="output_root", required=False, default="/workspace/data/ak_parquet")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    variables = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "output_root": cfg.get("output_root", args.output_root),
    }

    defaults = cfg.get("defaults", {})
    datasets: List[Dict[str, Any]] = cfg.get("datasets", [])

    for ds in datasets:
        try:
            run_dataset(ds, variables, defaults, args.output_root)
        except Exception as e:
            print(f"[ERROR] Dataset {ds.get('id')}: {e}")


if __name__ == "__main__":
    main()