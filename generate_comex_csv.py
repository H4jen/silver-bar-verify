#!/usr/bin/env python3
"""
COMEX Silver Time-Series CSV Generator
=======================================

Scans all dated ``silver_contracts_YYYYMMDD.json`` files in ``comex_data/``
and produces a single CSV with one row per date.  Re-generates from scratch
each run — the dated JSON files are the authoritative source.

Usage:
    python generate_comex_csv.py                     # default output
    python generate_comex_csv.py -o my_output.csv    # custom output path

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
DEFAULT_OUTPUT = os.path.join(CACHE_DIR, "comex_silver_timeseries.csv")

SILVER_CONTRACT_SIZE_OZ = 5000
TROY_OZ_PER_KG = 32.1507

MONTH_NAMES = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}
MONTH_NAME_TO_NUM = {v: k for k, v in MONTH_NAMES.items()}
MONTH_FIELDS = {
    "JAN": "deliveries_jan", "FEB": "deliveries_feb", "MAR": "deliveries_mar",
    "APR": "deliveries_apr", "MAY": "deliveries_may", "JUN": "deliveries_jun",
    "JUL": "deliveries_jul", "AUG": "deliveries_aug", "SEP": "deliveries_sep",
    "OCT": "deliveries_oct", "NOV": "deliveries_nov", "DEC": "deliveries_dec",
}


# ---------------------------------------------------------------------------
#  CSV column definitions  (order matters — this is the header row)
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    # -- Date & source --
    "date",
    "trade_date",

    # -- Price --
    "silver_price_usd",

    # -- Open Interest (aggregate) --
    "all_oi_contracts",
    "all_oi_oz",
    "target_oi_contracts",
    "target_oi_oz",
    "target_oi_tonnes",

    # -- Deliveries --
    "ytd_delivered_contracts",
    "ytd_delivered_oz",
    "current_month",
    "current_month_delivered_contracts",
    "current_month_delivered_oz",

    # -- Per-month delivery breakdown --
    "deliveries_jan", "deliveries_feb", "deliveries_mar",
    "deliveries_apr", "deliveries_may", "deliveries_jun",
    "deliveries_jul", "deliveries_aug", "deliveries_sep",
    "deliveries_oct", "deliveries_nov", "deliveries_dec",

    # -- Warehouse stocks --
    "warehouse_registered_oz",
    "warehouse_eligible_oz",
    "warehouse_combined_oz",
    "warehouse_registered_tonnes",
    "warehouse_eligible_tonnes",
    "warehouse_combined_tonnes",

    # -- Computed ratios --
    "warehouse_coverage_pct",

    # -- Notional values --
    "target_oi_value_usd",
    "ytd_delivered_value_usd",
    "warehouse_value_usd",

    # -- Per-contract OI (top 6 by OI) --
    "oi_month_1_label", "oi_month_1_contracts",
    "oi_month_2_label", "oi_month_2_contracts",
    "oi_month_3_label", "oi_month_3_contracts",
    "oi_month_4_label", "oi_month_4_contracts",
    "oi_month_5_label", "oi_month_5_contracts",
    "oi_month_6_label", "oi_month_6_contracts",

    # -- Front month detail --
    "front_month_label",
    "front_month_settle",
    "front_month_oi",
    "front_month_volume",
    "front_month_change",
    "front_month_high",
    "front_month_low",

    # -- All active contracts count --
    "active_contracts_count",
    "total_volume",
]


# ---------------------------------------------------------------------------
#  File discovery
# ---------------------------------------------------------------------------

def _find_contracts_jsons() -> list[tuple[str, str]]:
    """Return sorted list of (date_tag, filepath) for dated contracts JSONs.

    Scans ``comex_data/silver_contracts_YYYYMMDD.json``.
    Excludes ``_latest.json``.

    The *date_tag* is the YYYYMMDD from the filename.  Callers should
    prefer the ``trade_date`` field inside the JSON (the CME business
    date) over the filename tag when determining the row date.
    """
    pattern = os.path.join(CACHE_DIR, "silver_contracts_*.json")
    results: list[tuple[str, str]] = []
    for path in glob.glob(pattern):
        basename = os.path.basename(path)
        if "latest" in basename:
            continue
        m = re.search(r"silver_contracts_(\d{8})\.json$", basename)
        if m:
            results.append((m.group(1), path))
    results.sort(key=lambda x: x[0])
    return results


# ---------------------------------------------------------------------------
#  Target month computation (mirrors comex_silver_report2.py)
# ---------------------------------------------------------------------------

def _months_in_range(ref_date: datetime, num_months: int = 3) -> list[tuple[int, int]]:
    """Return (month, year) tuples for current + N months ahead."""
    result = []
    m, y = ref_date.month, ref_date.year
    for _ in range(num_months + 1):
        result.append((m, y))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


def _target_labels(ref_date: datetime) -> set[str]:
    """Return set of contract label prefixes for target months."""
    labels = set()
    for m, y in _months_in_range(ref_date, num_months=3):
        labels.add(f"{MONTH_NAMES[m]} {y}")
        labels.add(f"{MONTH_NAMES[m]} {str(y)[2:]}")
    return labels


def _is_target(label: str, target_labels: set[str]) -> bool:
    """Check if a contract label matches any target month."""
    lu = label.upper()
    return any(
        lu.startswith(tl.split()[0]) and lu.endswith(tl.split()[-1])
        if len(tl.split()) > 1 else lu.startswith(tl)
        for tl in target_labels
    )


# ---------------------------------------------------------------------------
#  Row builder — works directly from the contracts JSON
# ---------------------------------------------------------------------------

def _build_row(date_tag: str, data: dict) -> dict:
    """Build a single CSV row from a silver_contracts_YYYYMMDD.json file.

    The *date_tag* is the CME trade date (YYYYMMDD), which may come from
    either the JSON's ``trade_date`` field or the filename.
    """
    row: dict[str, Any] = {col: "" for col in CSV_COLUMNS}

    # -- Date (canonical CME trade date) --
    row["date"] = f"{date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:8]}"

    # -- Price --
    price = data.get("silver_price_usd")
    row["silver_price_usd"] = f"{price:.3f}" if price else ""

    # -- Contracts --
    contracts = data.get("contracts", [])
    if not contracts:
        return row

    # Determine trade date from first contract
    trade_date = ""
    for c in contracts:
        td = c.get("trade_date", "")
        if td:
            trade_date = td
            break
    row["trade_date"] = trade_date

    # Reference date for target month calculation
    try:
        ref_date = datetime.strptime(date_tag, "%Y%m%d")
    except ValueError:
        ref_date = datetime.now()

    targets = _target_labels(ref_date)

    # Aggregate OI
    all_oi = 0
    target_oi = 0
    total_volume = 0
    active_count = 0

    for c in contracts:
        oi = c.get("open_interest", 0) or 0
        vol = c.get("volume", 0) or 0
        label = c.get("month_label", "")

        all_oi += oi
        total_volume += vol
        if oi > 0:
            active_count += 1
        if _is_target(label, targets):
            target_oi += oi

    row["all_oi_contracts"] = all_oi
    row["all_oi_oz"] = all_oi * SILVER_CONTRACT_SIZE_OZ
    row["target_oi_contracts"] = target_oi
    target_oz = target_oi * SILVER_CONTRACT_SIZE_OZ
    target_tonnes = target_oz / TROY_OZ_PER_KG / 1000
    row["target_oi_oz"] = target_oz
    row["target_oi_tonnes"] = f"{target_tonnes:.1f}"
    row["active_contracts_count"] = active_count
    row["total_volume"] = total_volume

    # -- Delivery summary --
    ds = data.get("delivery_summary") or {}
    if isinstance(ds, dict) and ds.get("source") == "pdf":
        totals = ds.get("totals", {})

        # Current month = the month of the date_tag
        current_month_name = MONTH_NAMES.get(ref_date.month, "")
        row["current_month"] = current_month_name

        # Per-month breakdown
        ytd_contracts = 0
        current_month_contracts = 0
        for mon_name, num in totals.items():
            if not mon_name.startswith("PREV"):
                ytd_contracts += num
            if mon_name.upper() == current_month_name:
                current_month_contracts = num
            # Map to per-month column
            clean_name = mon_name.replace("PREV ", "")
            field = MONTH_FIELDS.get(clean_name.upper())
            if field:
                row[field] = num if num else ""

        row["ytd_delivered_contracts"] = ytd_contracts
        row["ytd_delivered_oz"] = ytd_contracts * SILVER_CONTRACT_SIZE_OZ
        row["current_month_delivered_contracts"] = current_month_contracts
        row["current_month_delivered_oz"] = current_month_contracts * SILVER_CONTRACT_SIZE_OZ

    # -- Warehouse stocks --
    wh = data.get("warehouse_stocks") or {}
    wh_reg = wh.get("total_registered_oz", 0) or 0
    wh_elig = wh.get("total_eligible_oz", 0) or 0
    wh_comb = wh.get("total_combined_oz", 0) or 0
    wh_reg_t = wh.get("total_registered_tonnes", 0) or 0
    wh_elig_t = wh.get("total_eligible_tonnes", 0) or 0
    wh_comb_t = wh.get("total_combined_tonnes", 0) or 0

    row["warehouse_registered_oz"] = f"{wh_reg:.0f}" if wh_reg else ""
    row["warehouse_eligible_oz"] = f"{wh_elig:.0f}" if wh_elig else ""
    row["warehouse_combined_oz"] = f"{wh_comb:.0f}" if wh_comb else ""
    row["warehouse_registered_tonnes"] = f"{wh_reg_t:.1f}" if wh_reg_t else ""
    row["warehouse_eligible_tonnes"] = f"{wh_elig_t:.1f}" if wh_elig_t else ""
    row["warehouse_combined_tonnes"] = f"{wh_comb_t:.1f}" if wh_comb_t else ""

    # -- Coverage ratio --
    if wh_comb > 0 and target_oz > 0:
        row["warehouse_coverage_pct"] = f"{wh_comb / target_oz * 100:.1f}"

    # -- Notional values --
    if price:
        if target_oz:
            row["target_oi_value_usd"] = f"{target_oz * price:.0f}"
        ytd_oz = row.get("ytd_delivered_oz")
        if ytd_oz:
            row["ytd_delivered_value_usd"] = f"{ytd_oz * price:.0f}"
        if wh_comb:
            row["warehouse_value_usd"] = f"{wh_comb * price:.0f}"

    # -- Top 6 contracts by OI --
    by_oi = sorted(contracts, key=lambda c: c.get("open_interest", 0) or 0,
                   reverse=True)
    for i, c in enumerate(by_oi[:6]):
        oi = c.get("open_interest", 0) or 0
        if oi <= 0:
            break
        idx = i + 1
        row[f"oi_month_{idx}_label"] = c.get("month_label", "")
        row[f"oi_month_{idx}_contracts"] = oi

    # -- Front month (highest OI) --
    if by_oi and (by_oi[0].get("open_interest", 0) or 0) > 0:
        front = by_oi[0]
        row["front_month_label"] = front.get("month_label", "")
        row["front_month_settle"] = front.get("settle_price", "")
        row["front_month_oi"] = front.get("open_interest", "")
        row["front_month_volume"] = front.get("volume", "")
        row["front_month_change"] = front.get("change", "")
        row["front_month_high"] = front.get("high", "")
        row["front_month_low"] = front.get("low", "")

    return row


# ---------------------------------------------------------------------------
#  Main generator
# ---------------------------------------------------------------------------

def generate_comex_csv(output_path: str | None = None,
                       verbose: bool = True) -> str:
    """Generate the COMEX silver time-series CSV.

    Iterates over all ``silver_contracts_YYYYMMDD.json`` files in
    ``comex_data/`` and builds one row per date.

    Returns the output file path.
    """
    if output_path is None:
        output_path = DEFAULT_OUTPUT

    if verbose:
        print("COMEX Silver Time-Series CSV Generator")
        print("=" * 42)

    dated_files = _find_contracts_jsons()
    if not dated_files:
        if verbose:
            print("  No silver_contracts_YYYYMMDD.json files found in comex_data/.")
            print("  Run comex_silver_report2.py first to generate data.")
        return output_path

    if verbose:
        print(f"  Found {len(dated_files)} data file(s)")

    # De-duplicate by CME trade date.  If multiple files contain the
    # same trade_date the one with the latest ``generated`` timestamp wins.
    trade_date_map: dict[str, tuple[str, dict]] = {}  # trade_date -> (generated, data)
    for file_tag, filepath in dated_files:
        try:
            with open(filepath) as f:
                data = json.load(f)
        except Exception as e:
            if verbose:
                print(f"  Warning: Could not load {filepath}: {e}")
            continue

        # Prefer the explicit trade_date field; fall back to filename tag
        td = data.get("trade_date") or file_tag
        generated = data.get("generated", "")

        prev = trade_date_map.get(td)
        if prev is None or generated > prev[0]:
            trade_date_map[td] = (generated, data)

    if verbose and len(trade_date_map) < len(dated_files):
        dup = len(dated_files) - len(trade_date_map)
        print(f"  De-duplicated {dup} file(s) with same CME trade date")

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
        description="Generate COMEX silver time-series CSV from dated contracts files",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help=f"Output CSV path (default: {DEFAULT_OUTPUT})",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    generate_comex_csv(output_path=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
