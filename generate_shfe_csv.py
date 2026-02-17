#!/usr/bin/env python3
"""
SHFE Silver Time-Series CSV Generator
======================================

Scans all dated ``shfe_silver_YYYYMMDD.json`` files in ``comex_data/``
and produces a single CSV with one row per date.  Re-generates from scratch
each run — the dated JSON files are the authoritative source.

Usage:
    python generate_shfe_csv.py                     # default output
    python generate_shfe_csv.py -o my_output.csv    # custom output path

The output CSV is ready for plotting with pandas / matplotlib / Excel.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
from datetime import datetime
from typing import Any


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")
DEFAULT_OUTPUT = os.path.join(CACHE_DIR, "shfe_silver_timeseries.csv")

# SHFE AG contract = 15 kg
SHFE_CONTRACT_SIZE_KG = 15
TROY_OZ_PER_KG = 32.1507


# ---------------------------------------------------------------------------
#  CSV column definitions  (order matters — this is the header row)
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    # -- Date & source --
    "date",
    "trade_date",

    # -- Price (front month, USD/oz ex-VAT, indicative) --
    "silver_price_usd",
    "cny_usd",

    # -- Price (front month, native RMB/kg) --
    "front_settlement_rmb_kg",
    "front_close_rmb_kg",
    "front_change_rmb_kg",

    # -- Open Interest (aggregate, oz-primary) --
    "total_oi_oz",
    "total_oi_contracts",
    "total_oi_kg",
    "total_oi_tonnes",

    # -- Volume --
    "total_volume",
    "total_turnover_10k_rmb",

    # -- Active contracts count --
    "active_contracts_count",

    # -- Warehouse inventory (oz-primary) --
    "warehouse_registered_oz",
    "warehouse_eligible_oz",
    "warehouse_combined_oz",
    "warehouse_registered_kg",
    "warehouse_eligible_kg",
    "warehouse_combined_kg",
    "warehouse_registered_tonnes",
    "warehouse_eligible_tonnes",
    "warehouse_combined_tonnes",

    # -- Warehouse coverage ratio --
    "warehouse_coverage_pct",

    # -- Notional values (USD) --
    "warehouse_value_usd",

    # -- Per-depository --
    "depository_count",

    # -- Top 6 contracts by OI --
    "oi_rank_1_month", "oi_rank_1_contracts", "oi_rank_1_settle_usd_oz",
    "oi_rank_2_month", "oi_rank_2_contracts", "oi_rank_2_settle_usd_oz",
    "oi_rank_3_month", "oi_rank_3_contracts", "oi_rank_3_settle_usd_oz",
    "oi_rank_4_month", "oi_rank_4_contracts", "oi_rank_4_settle_usd_oz",
    "oi_rank_5_month", "oi_rank_5_contracts", "oi_rank_5_settle_usd_oz",
    "oi_rank_6_month", "oi_rank_6_contracts", "oi_rank_6_settle_usd_oz",

    # -- Front month detail (USD/oz + RMB/kg) --
    "front_month",
    "front_settle_usd_oz",
    "front_close_usd_oz",
    "front_high_usd_oz",
    "front_low_usd_oz",
    "front_settle_rmb_kg",
    "front_close_rmb_kg",
    "front_open_rmb_kg",
    "front_high_rmb_kg",
    "front_low_rmb_kg",
    "front_prev_settle_rmb_kg",
    "front_oi",
    "front_oi_oz",
    "front_volume",
    "front_turnover_10k_rmb",

    # -- SHFE meta --
    "meta_year_num",
    "meta_total_num",
    "meta_trade_day",
    "meta_weekday",
]


# ---------------------------------------------------------------------------
#  File discovery
# ---------------------------------------------------------------------------

def _find_shfe_jsons() -> list[tuple[str, str]]:
    """Return sorted list of (date_tag, filepath) for dated SHFE JSONs.

    Scans ``comex_data/shfe_silver_YYYYMMDD.json``.
    The *date_tag* is the YYYYMMDD from the filename.
    """
    pattern = os.path.join(CACHE_DIR, "shfe_silver_*.json")
    results: list[tuple[str, str]] = []
    for path in glob.glob(pattern):
        basename = os.path.basename(path)
        if "latest" in basename or "timeseries" in basename:
            continue
        m = re.search(r"shfe_silver_(\d{8})\.json$", basename)
        if m:
            results.append((m.group(1), path))
    results.sort(key=lambda x: x[0])
    return results


# ---------------------------------------------------------------------------
#  Row builder
# ---------------------------------------------------------------------------

def _fmt(val: Any, decimals: int = 0) -> str:
    """Format a numeric value for CSV; return '' for None/0."""
    if val is None:
        return ""
    if isinstance(val, float):
        return f"{val:.{decimals}f}" if val else ""
    return str(val) if val else ""


def _build_row(date_tag: str, data: dict) -> dict:
    """Build a single CSV row from a shfe_silver_YYYYMMDD.json file.

    Uses COMEX-aligned column names (registered/eligible/combined)
    and oz as the primary unit for warehouse & OI quantities.
    USD/oz prices from the fetch script (ex-VAT, indicative).
    """
    row: dict[str, Any] = {col: "" for col in CSV_COLUMNS}

    # -- Date --
    row["date"] = f"{date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:8]}"
    row["trade_date"] = data.get("trade_date", date_tag)

    # -- CNY/USD rate used at fetch time --
    cny_usd = data.get("cny_usd")
    row["cny_usd"] = f"{cny_usd:.4f}" if cny_usd else ""

    # -- Top-level silver_price_usd (front-month settle, USD/oz ex-VAT) --
    price_usd = data.get("silver_price_usd")
    row["silver_price_usd"] = f"{price_usd:.4f}" if price_usd else ""

    # -- Contracts --
    contracts = data.get("contracts", [])
    if not contracts:
        return row

    # Aggregates
    total_oi = 0
    total_volume = 0
    total_turnover = 0.0
    active_count = 0

    for c in contracts:
        oi = c.get("open_interest", 0) or 0
        vol = c.get("volume", 0) or 0
        turnover = c.get("turnover_10k_rmb", 0) or 0

        total_oi += oi
        total_volume += vol
        total_turnover += turnover
        if oi > 0:
            active_count += 1

    # Use pre-computed totals from JSON if available, else compute
    total_oi_oz = data.get("total_oi_oz") or round(total_oi * SHFE_CONTRACT_SIZE_KG * TROY_OZ_PER_KG)
    total_oi_kg = data.get("total_oi_kg") or (total_oi * SHFE_CONTRACT_SIZE_KG)
    total_oi_tonnes = total_oi_kg / 1000

    row["total_oi_oz"] = f"{total_oi_oz:.0f}"
    row["total_oi_contracts"] = total_oi
    row["total_oi_kg"] = total_oi_kg
    row["total_oi_tonnes"] = f"{total_oi_tonnes:.1f}"
    row["total_volume"] = total_volume
    row["total_turnover_10k_rmb"] = f"{total_turnover:.3f}"
    row["active_contracts_count"] = active_count

    # -- Front month (highest OI) --
    by_oi = sorted(contracts, key=lambda c: c.get("open_interest", 0) or 0,
                   reverse=True)

    if by_oi and (by_oi[0].get("open_interest", 0) or 0) > 0:
        front = by_oi[0]
        # Summary price columns (RMB/kg)
        row["front_settlement_rmb_kg"] = _fmt(front.get("settlement_price_rmb_kg"))
        row["front_close_rmb_kg"] = _fmt(front.get("close_price_rmb_kg"))
        row["front_change_rmb_kg"] = _fmt(front.get("change_vs_prev_settle"))

        # Front month detail — USD/oz
        row["front_month"] = front.get("month_label") or front.get("delivery_month", "")
        row["front_settle_usd_oz"] = _fmt(front.get("settlement_price_usd_oz"), 4)
        row["front_close_usd_oz"] = _fmt(front.get("close_price_usd_oz"), 4)
        row["front_high_usd_oz"] = _fmt(front.get("high_usd_oz"), 4)
        row["front_low_usd_oz"] = _fmt(front.get("low_usd_oz"), 4)

        # Front month detail — RMB/kg
        row["front_settle_rmb_kg"] = _fmt(front.get("settlement_price_rmb_kg"))
        row["front_close_rmb_kg"] = _fmt(front.get("close_price_rmb_kg"))
        row["front_open_rmb_kg"] = _fmt(front.get("open_price_rmb_kg"))
        row["front_high_rmb_kg"] = _fmt(front.get("high_rmb_kg"))
        row["front_low_rmb_kg"] = _fmt(front.get("low_rmb_kg"))
        row["front_prev_settle_rmb_kg"] = _fmt(front.get("prev_settlement_rmb_kg"))

        # Front month OI & volume
        front_oi = front.get("open_interest", 0) or 0
        row["front_oi"] = _fmt(front_oi)
        row["front_oi_oz"] = _fmt(front.get("open_interest_oz"))
        row["front_volume"] = _fmt(front.get("volume"))
        row["front_turnover_10k_rmb"] = _fmt(front.get("turnover_10k_rmb"), 3)

    # -- Top 6 contracts by OI --
    for i, c in enumerate(by_oi[:6]):
        oi = c.get("open_interest", 0) or 0
        if oi <= 0:
            break
        idx = i + 1
        row[f"oi_rank_{idx}_month"] = c.get("month_label") or c.get("delivery_month", "")
        row[f"oi_rank_{idx}_contracts"] = oi
        row[f"oi_rank_{idx}_settle_usd_oz"] = _fmt(c.get("settlement_price_usd_oz"), 4)

    # -- Warehouse stocks (COMEX-aligned names) --
    # SHFE: warrant=registered, non_warrant=eligible, cargo=combined
    wh = data.get("warehouse", {})
    totals = wh.get("totals", {})

    reg_oz = totals.get("warrant_oz", 0) or 0
    elig_oz = totals.get("non_warrant_oz", 0) or 0
    comb_oz = totals.get("cargo_oz", 0) or 0
    reg_kg = totals.get("warrant_kg", 0) or 0
    elig_kg = totals.get("non_warrant_kg", 0) or 0
    comb_kg = totals.get("cargo_kg", 0) or 0
    reg_t = totals.get("warrant_tonnes", 0) or 0
    elig_t = totals.get("non_warrant_tonnes", 0) or 0
    comb_t = totals.get("cargo_tonnes", 0) or 0

    row["warehouse_registered_oz"] = f"{reg_oz:.0f}" if reg_oz else ""
    row["warehouse_eligible_oz"] = f"{elig_oz:.0f}" if elig_oz else ""
    row["warehouse_combined_oz"] = f"{comb_oz:.0f}" if comb_oz else ""
    row["warehouse_registered_kg"] = _fmt(reg_kg)
    row["warehouse_eligible_kg"] = _fmt(elig_kg)
    row["warehouse_combined_kg"] = _fmt(comb_kg)
    row["warehouse_registered_tonnes"] = f"{reg_t:.2f}" if reg_t else ""
    row["warehouse_eligible_tonnes"] = f"{elig_t:.2f}" if elig_t else ""
    row["warehouse_combined_tonnes"] = f"{comb_t:.1f}" if comb_t else ""

    # Depository count
    deps = wh.get("depositories", [])
    row["depository_count"] = len(deps) if deps else ""

    # -- Coverage ratio: combined oz / total OI oz --
    if comb_oz > 0 and total_oi_oz > 0:
        row["warehouse_coverage_pct"] = f"{comb_oz / total_oi_oz * 100:.1f}"

    # -- Notional warehouse value in USD --
    if comb_oz > 0 and price_usd:
        row["warehouse_value_usd"] = f"{comb_oz * price_usd:.0f}"

    # -- SHFE meta --
    meta = data.get("meta", {})
    row["meta_year_num"] = meta.get("year_num", "")
    row["meta_total_num"] = meta.get("total_num", "")
    row["meta_trade_day"] = meta.get("trade_day", "")
    row["meta_weekday"] = meta.get("weekday", "")

    return row


# ---------------------------------------------------------------------------
#  Main generator
# ---------------------------------------------------------------------------

def generate_shfe_csv(output_path: str | None = None,
                      verbose: bool = True) -> str:
    """Generate the SHFE silver time-series CSV.

    Iterates over all ``shfe_silver_YYYYMMDD.json`` files in
    ``comex_data/`` and builds one row per date.

    Returns the output file path.
    """
    if output_path is None:
        output_path = DEFAULT_OUTPUT

    if verbose:
        print("SHFE Silver Time-Series CSV Generator")
        print("=" * 42)

    dated_files = _find_shfe_jsons()
    if not dated_files:
        if verbose:
            print("  No shfe_silver_YYYYMMDD.json files found in comex_data/.")
            print("  Run fetch_shfe_silver.py first to generate data.")
        return output_path

    if verbose:
        print(f"  Found {len(dated_files)} data file(s)")

    # De-duplicate by trade date.  If multiple files contain the same
    # trade_date the one with the latest ``generated`` timestamp wins.
    trade_date_map: dict[str, tuple[str, dict]] = {}
    for file_tag, filepath in dated_files:
        try:
            with open(filepath) as f:
                data = json.load(f)
        except Exception as e:
            if verbose:
                print(f"  Warning: Could not load {filepath}: {e}")
            continue

        td = data.get("trade_date") or file_tag
        generated = data.get("generated", "")

        prev = trade_date_map.get(td)
        if prev is None or generated > prev[0]:
            trade_date_map[td] = (generated, data)

    if verbose and len(trade_date_map) < len(dated_files):
        dup = len(dated_files) - len(trade_date_map)
        print(f"  De-duplicated {dup} file(s) with same trade date")

    rows = []
    for td in sorted(trade_date_map):
        _, data = trade_date_map[td]
        row = _build_row(td, data)
        rows.append(row)

    # Write CSV
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    if verbose:
        dates = [r["date"] for r in rows]
        date_range = f"{dates[0]} → {dates[-1]}" if dates else "none"
        print(f"  Wrote {len(rows)} rows ({date_range})")
        print(f"  Output: {output_path}")

    return output_path


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def _parse_args() -> Any:
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate SHFE silver time-series CSV from dated SHFE JSON files",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    generate_shfe_csv(output_path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
