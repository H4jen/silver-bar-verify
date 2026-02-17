#!/usr/bin/env python3
"""
Silver ETC Time-Series CSV Generator
=====================================

Scans all dated verification JSONs, metrics files, and bar-history
databases in ``comex_data/`` and produces a single CSV with one row
per fund per date.  Re-generates from scratch each run.

Usage:
    python generate_csv.py                     # default: comex_data/silver_etcs_timeseries.csv
    python generate_csv.py -o my_output.csv    # custom output path
    python generate_csv.py --funds invesco     # single fund

The output CSV is ready for plotting with pandas / matplotlib / Excel.
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")


# ---------------------------------------------------------------------------
#  CSV column definitions  (order matters — this is the header row)
# ---------------------------------------------------------------------------

CSV_COLUMNS = [
    "date",
    "fund",
    "bar_count",
    "expected_oz",
    "physical_oz",
    "difference_oz",
    "difference_pct",
    "gross_oz_total",
    "fine_oz_total",
    "avg_bar_weight_gross_oz",
    "unique_refiners",
    "unique_vaults",
    "certificates_outstanding",
    "entitlement_oz_per_cert",
    "nav_usd",
    "total_assets_usd",
    "collateral_ratio_pct",
    "status",
    "delta_bars_added",
    "delta_bars_removed",
    "delta_bars_returned",
    "delta_vault_transfers",
    "delta_net_oz_change",
    "bars_re_entry_flag",
    "bars_seen_2_plus",
    "bars_seen_3_plus",
    "bars_seen_4_plus",
    "bars_seen_5_plus",
]


# ---------------------------------------------------------------------------
#  Discovery helpers
# ---------------------------------------------------------------------------

def _find_verification_jsons() -> list[tuple[str, str]]:
    """Return sorted list of (date_tag, filepath) for dated verification JSONs.

    Only scans ``comex_data/``.  Excludes ``_latest.json``.
    """
    pattern = os.path.join(CACHE_DIR, "etc_silver_inventory_verification_*.json")
    results: list[tuple[str, str]] = []
    for path in glob.glob(pattern):
        basename = os.path.basename(path)
        if "latest" in basename:
            continue
        m = re.search(r"_(\d{8})\.json$", basename)
        if m:
            results.append((m.group(1), path))
    results.sort(key=lambda x: x[0])
    return results


def _normalise_date_tag(raw_date: str | None) -> str:
    """Turn 'DD Month YYYY' or 'YYYY-MM-DD' into YYYYMMDD.  Returns '' on failure."""
    if not raw_date:
        return ""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw_date)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}"
    try:
        dt = datetime.strptime(raw_date, "%d %B %Y")
        return dt.strftime("%Y%m%d")
    except ValueError:
        pass
    return ""


def _extract_barlist_date(fund_result: dict[str, Any]) -> str:
    """Get the bar-list effective date (YYYYMMDD) from a fund result."""
    parse = fund_result.get("parse") or {}
    hm = parse.get("header_metadata") or {}
    return _normalise_date_tag(hm.get("as_of_date", ""))


def _build_metrics_index() -> dict[str, dict[str, dict[str, Any]]]:
    """Scan per-fund metrics files and index by data date.

    Filename convention: ``etc_fund_metrics_<fund>.json`` (canonical)
    and ``etc_fund_metrics_<fund>_YYYYMMDD.json`` (archives).
    Each file is a flat dict (no wrapper key).

    Returns ``{fund_key: {yyyymmdd: fund_metrics_dict}}``.
    When multiple files for the same fund + date exist, the last one
    (alphabetically) wins — the canonical file sorts after archives.
    """
    KNOWN_FUNDS = ("invesco", "wisdomtree")
    index: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

    for fund_key in KNOWN_FUNDS:
        canonical = os.path.join(CACHE_DIR, f"etc_fund_metrics_{fund_key}.json")
        name, ext = os.path.splitext(canonical)
        pattern = f"{name}_*{ext}"

        paths = sorted(glob.glob(pattern))
        if os.path.exists(canonical) and canonical not in paths:
            paths.append(canonical)

        for path in paths:
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except Exception:
                continue

            as_of = data.get("as_of", "")
            date_tag = as_of.replace("-", "")
            if len(date_tag) == 8 and date_tag.isdigit():
                index[fund_key][date_tag] = data

    return dict(index)


def _load_bar_history(fund_key: str) -> dict[str, Any]:
    """Load the bar-history DB for a fund (if it exists)."""
    path = os.path.join(CACHE_DIR, f"bar_history_{fund_key}.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"bars": {}, "snapshots": []}


# ---------------------------------------------------------------------------
#  Delta computation from bar lists (without requiring bar_history DB)
# ---------------------------------------------------------------------------

def _bar_key_from_dict(bar: dict) -> str:
    serial = bar.get("serial_number", "")
    refiner = bar.get("refiner") or ""
    return f"{serial}|{refiner}"


def _compute_delta_from_bars(
    prev_bars: list[dict],
    curr_bars: list[dict],
) -> dict[str, int | float]:
    """Compute add/remove/oz-change between two bar lists."""
    prev_keys = {_bar_key_from_dict(b) for b in prev_bars}
    curr_keys = {_bar_key_from_dict(b) for b in curr_bars}

    added = curr_keys - prev_keys
    removed = prev_keys - curr_keys

    prev_fine = sum(b.get("fine_oz") or b.get("gross_oz") or 0 for b in prev_bars)
    curr_fine = sum(b.get("fine_oz") or b.get("gross_oz") or 0 for b in curr_bars)

    return {
        "added": len(added),
        "removed": len(removed),
        "net_oz_change": round(curr_fine - prev_fine, 3),
    }


# ---------------------------------------------------------------------------
#  Re-entry / appearance counting from bar-history DB
# ---------------------------------------------------------------------------

def _count_reentry_stats(
    history: dict[str, Any],
    date_tag: str,
) -> dict[str, int]:
    """Count bars with N+ appearances up to *date_tag* that are present."""
    bars = history.get("bars", {})
    counts = {"re_entry_flag": 0, "seen_2": 0, "seen_3": 0, "seen_4": 0, "seen_5": 0}
    for entry in bars.values():
        if entry.get("status") != "present":
            # Only count bars present in the vault at this point
            # But since history is cumulative and we may not have per-date
            # snapshots, we count all bars whose last_seen <= date_tag
            if entry.get("last_seen", "99999999") > date_tag:
                continue
            # skip bars removed before this date
            continue

        n_appearances = len(entry.get("appearances", []))
        re_entries = entry.get("re_entries", 0)

        if re_entries > 0:
            counts["re_entry_flag"] += 1
        if n_appearances >= 2:
            counts["seen_2"] += 1
        if n_appearances >= 3:
            counts["seen_3"] += 1
        if n_appearances >= 4:
            counts["seen_4"] += 1
        if n_appearances >= 5:
            counts["seen_5"] += 1

    return counts


# ---------------------------------------------------------------------------
#  Main CSV row builder
# ---------------------------------------------------------------------------

def _build_row(
    date_tag: str,
    fund_key: str,
    fund_result: dict[str, Any],
    fund_metrics: dict[str, Any],
    delta: dict[str, int | float] | None,
    reentry_stats: dict[str, int],
    prev_result: dict[str, Any] | None,
) -> tuple[dict[str, Any], bool]:
    """Build a single CSV row dict for one fund on one date.

    *fund_metrics* is the already-resolved per-fund metrics dict
    (empty dict when no same-day match exists).

    Returns ``(row_dict, has_error)``.
    """
    ver = fund_result.get("verification") or {}
    aggr = fund_result.get("aggregates") or {}
    parse = fund_result.get("parse") or {}
    hm = parse.get("header_metadata") or {}

    bar_count = aggr.get("bar_count", 0)
    gross_oz = aggr.get("total_gross_oz")
    fine_oz = aggr.get("total_fine_oz")
    expected_oz = ver.get("expected_oz")
    physical_oz = ver.get("physical_oz_from_bar_list")

    # difference — use stored if available, else compute
    diff_oz = ver.get("difference_oz")
    diff_pct = ver.get("difference_pct")
    if diff_oz is None and expected_oz and physical_oz:
        diff_oz = round(physical_oz - expected_oz, 3)
    if diff_pct is None and expected_oz and physical_oz and expected_oz > 0:
        diff_pct = round((physical_oz - expected_oz) / expected_oz * 100, 6)

    avg_weight = round(gross_oz / bar_count, 3) if gross_oz and bar_count else None

    # Collateral ratio
    collateral_pct = None
    if physical_oz and expected_oz and expected_oz > 0:
        collateral_pct = round(physical_oz / expected_oz * 100, 4)

    # Fund metrics — already resolved by caller (strict same-day)
    has_metrics_error = not fund_metrics
    if has_metrics_error:
        print(f"  ERROR: No fund metrics for {fund_key} on "
              f"{date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:8]}  "
              f"— row will have status=insufficient_fund_metrics",
              file=sys.stderr)
    certs = fund_metrics.get("certificates_outstanding")
    entitlement = fund_metrics.get("entitlement_oz_per_certificate")
    nav = fund_metrics.get("nav_usd")
    aum = fund_metrics.get("total_assets_usd")

    # Delta
    d_added = delta["added"] if delta else None
    d_removed = delta["removed"] if delta else None
    d_net_oz = delta["net_oz_change"] if delta else None

    # Vault transfers — compute from bars if both present
    d_vault_xfr = None
    d_returned = None
    # These require bar-level comparison which we get from delta analysis
    # For now leave as None unless we can compute it

    # Format date
    date_str = f"{date_tag[:4]}-{date_tag[4:6]}-{date_tag[6:8]}"

    return {
        "date": date_str,
        "fund": fund_key,
        "bar_count": bar_count,
        "expected_oz": _round_or_none(expected_oz, 3),
        "physical_oz": _round_or_none(physical_oz, 3),
        "difference_oz": _round_or_none(diff_oz, 3),
        "difference_pct": _round_or_none(diff_pct, 6),
        "gross_oz_total": _round_or_none(gross_oz, 3),
        "fine_oz_total": _round_or_none(fine_oz, 3),
        "avg_bar_weight_gross_oz": avg_weight,
        "unique_refiners": aggr.get("unique_refiners"),
        "unique_vaults": aggr.get("unique_vaults"),
        "certificates_outstanding": certs,
        "entitlement_oz_per_cert": entitlement,
        "nav_usd": nav,
        "total_assets_usd": _round_or_none(aum, 2),
        "collateral_ratio_pct": collateral_pct,
        "status": ver.get("status"),
        "delta_bars_added": d_added,
        "delta_bars_removed": d_removed,
        "delta_bars_returned": d_returned,
        "delta_vault_transfers": d_vault_xfr,
        "delta_net_oz_change": d_net_oz,
        "bars_re_entry_flag": reentry_stats.get("re_entry_flag"),
        "bars_seen_2_plus": reentry_stats.get("seen_2"),
        "bars_seen_3_plus": reentry_stats.get("seen_3"),
        "bars_seen_4_plus": reentry_stats.get("seen_4"),
        "bars_seen_5_plus": reentry_stats.get("seen_5"),
    }, has_metrics_error


def _round_or_none(val: Any, digits: int) -> Any:
    if val is None:
        return None
    try:
        return round(float(val), digits)
    except (TypeError, ValueError):
        return val


# ---------------------------------------------------------------------------
#  Main generation loop
# ---------------------------------------------------------------------------

def generate_csv(
    output_path: str | None = None,
    funds: list[str] | None = None,
    verbose: bool = True,
) -> tuple[str, int]:
    """Scan comex_data/ verification JSONs and produce a time-series CSV.

    Returns ``(output_file_path, error_count)``.
    An *error_count* > 0 means at least one row had missing or
    mismatched day-to-day correlated data (e.g. no same-day fund metrics).
    """
    if output_path is None:
        output_path = os.path.join(CACHE_DIR, "silver_etcs_timeseries.csv")
    if funds is None:
        funds = ["invesco", "wisdomtree"]

    error_count = 0

    verification_files = _find_verification_jsons()
    if not verification_files:
        print("  ERROR: No dated verification JSONs found in comex_data/",
              file=sys.stderr)
        return output_path, 1

    if verbose:
        print(f"  Found {len(verification_files)} verification snapshot(s)")

    # Build a per-fund per-date metrics index
    metrics_index = _build_metrics_index()
    if verbose:
        total_entries = sum(len(v) for v in metrics_index.values())
        print(f"  Metrics index: {total_entries} fund-date entries across "
              f"{len(metrics_index)} fund(s)")

    # Load bar-history DBs for re-entry stats
    histories: dict[str, dict[str, Any]] = {}
    for fund in funds:
        histories[fund] = _load_bar_history(fund)

    # Process each date — keep previous result for delta computation
    rows: list[dict[str, Any]] = []
    prev_results: dict[str, dict[str, Any]] = {}  # fund_key → previous result

    for date_tag, filepath in verification_files:
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                report = json.load(fh)
        except Exception as exc:
            print(f"  ERROR: Could not read verification file {filepath}: {exc}",
                  file=sys.stderr)
            error_count += 1
            continue

        results = report.get("results", {})
        if not results:
            print(f"  ERROR: Verification file {os.path.basename(filepath)} "
                  f"contains no fund results", file=sys.stderr)
            error_count += 1
            continue

        for fund_key in funds:
            fund_result = results.get(fund_key)
            if not fund_result:
                continue

            # Determine the bar-list effective date for this fund
            barlist_date = _extract_barlist_date(fund_result)
            if not barlist_date:
                barlist_date = date_tag  # fall back to verification file date

            # Strict same-day metric lookup for this specific fund
            fund_metrics = metrics_index.get(fund_key, {}).get(barlist_date, {})
            if not fund_metrics:
                available = sorted(metrics_index.get(fund_key, {}).keys())
                avail_str = ", ".join(available) if available else "(none)"
                print(f"  ERROR: No same-day fund metrics for {fund_key} "
                      f"barlist date {barlist_date}. Available: {avail_str}",
                      file=sys.stderr)

            # Delta from previous bar list
            delta = None
            curr_bars = fund_result.get("bars", [])
            prev_fund = prev_results.get(fund_key)
            if prev_fund is not None:
                prev_bars = prev_fund.get("bars", [])
                if curr_bars and prev_bars:
                    delta = _compute_delta_from_bars(prev_bars, curr_bars)

            # Re-entry stats from history DB
            reentry = _count_reentry_stats(histories[fund_key], date_tag)

            row, row_has_error = _build_row(
                date_tag=date_tag,
                fund_key=fund_key,
                fund_result=fund_result,
                fund_metrics=fund_metrics,
                delta=delta,
                reentry_stats=reentry,
                prev_result=prev_fund,
            )
            if row_has_error:
                error_count += 1
            rows.append(row)
            prev_results[fund_key] = fund_result

    # Write CSV
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    if verbose:
        print(f"  Wrote {len(rows)} rows ({len(set(r['date'] for r in rows))} dates,"
              f" {len(set(r['fund'] for r in rows))} funds)")
        print(f"  Output: {output_path}")
        if error_count:
            print(f"  ERRORS: {error_count} data correlation error(s) detected",
                  file=sys.stderr)

    return output_path, error_count


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def _parse_args() -> Any:
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate time-series CSV from silver ETC verification data",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output CSV path (default: comex_data/silver_etcs_timeseries.csv)",
    )
    parser.add_argument(
        "--funds",
        nargs="+",
        choices=["invesco", "wisdomtree"],
        default=["invesco", "wisdomtree"],
        help="Which funds to include",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    print("Silver ETC Time-Series CSV Generator")
    print("=" * 40)
    _path, errors = generate_csv(output_path=args.output, funds=args.funds)
    if errors:
        print(f"\nFAILED: {errors} error(s) — see ERROR messages above.",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
