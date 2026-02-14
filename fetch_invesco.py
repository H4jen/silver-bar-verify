#!/usr/bin/env python3
"""
Invesco Physical Silver ETC – Fund Metrics Scraper
===================================================

Fetches fund metrics from the Invesco API (dng-api.invesco.com) which
backs the product page:
  https://www.invesco.com/se/en/financial-products/etfs/invesco-physical-silver-etc.html

Two API endpoints provide all the data:
  1. fundDetails    → entitlement, AUM, CV (NAV), fee, umbrella AUM
  2. generalSecurityInformation → certificates outstanding

Usage:
  python fetch_invesco.py                     # print metrics JSON to stdout
  python fetch_invesco.py --update-metrics    # merge into etc_fund_metrics.json
  python fetch_invesco.py -o output.json      # write raw scrape to file
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from curl_cffi import requests as cffi_requests

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")
METRICS_FILE = os.path.join(CACHE_DIR, "etc_fund_metrics.json")

ISIN = "IE00B43VDT70"
LOCALE = "en_SE"

# API endpoints discovered from the Invesco SPA page configuration
_BASE = f"https://dng-api.invesco.com/cache/v1/accounts/{LOCALE}/shareclasses/{ISIN}"
API_FUND_DETAILS = f"{_BASE}?expand=nav&idType=isin&variationType=fundDetails"
API_GENERAL_INFO = f"{_BASE}?expand=nav&idType=isin&variationType=generalSecurityInformation"


# ---------------------------------------------------------------------------
#  Fetch helpers
# ---------------------------------------------------------------------------

def _fetch_json(url: str) -> dict[str, Any]:
    """Fetch a JSON endpoint via curl_cffi.  Raises on failure."""
    resp = cffi_requests.get(
        url,
        impersonate="chrome",
        timeout=30,
        headers={
            "Accept": "application/json,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
        },
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"API returned HTTP {resp.status_code} for {url}\n{resp.text[:500]}"
        )
    return resp.json()


# ---------------------------------------------------------------------------
#  Metric extraction
# ---------------------------------------------------------------------------

def fetch_invesco_metrics() -> dict[str, Any]:
    """Fetch all Invesco Physical Silver ETC metrics from the API.

    Returns a flat dict ready to merge into etc_fund_metrics.json.
    """
    print("[invesco] Fetching fundDetails...", file=sys.stderr)
    fund = _fetch_json(API_FUND_DETAILS)

    print("[invesco] Fetching generalSecurityInformation...", file=sys.stderr)
    gen = _fetch_json(API_GENERAL_INFO)

    metrics: dict[str, Any] = {}

    # --- From fundDetails ---
    if "entitlementPerShare" in fund:
        metrics["entitlement_oz_per_certificate"] = fund["entitlementPerShare"]
    if "entitlementPerShareDate" in fund:
        metrics["entitlement_as_of"] = fund["entitlementPerShareDate"]
    if "totalAssetsOutstanding" in fund:
        metrics["total_assets_usd"] = fund["totalAssetsOutstanding"]
    if "umbrellaAum" in fund:
        metrics["umbrella_aum_usd"] = fund["umbrellaAum"]
    if "cvPerShare" in fund:
        metrics["nav_usd"] = fund["cvPerShare"]
    if "fixedCharge" in fund:
        metrics["fixed_fee_pct"] = fund["fixedCharge"]
    if "effectiveDate" in fund:
        metrics["as_of"] = fund["effectiveDate"]
    if "shareClassCurrency" in fund:
        metrics["currency"] = fund["shareClassCurrency"]

    # --- From generalSecurityInformation ---
    if "certificatesOutstanding" in gen:
        metrics["certificates_outstanding"] = gen["certificatesOutstanding"]
    elif "sharesOutstanding" in gen:
        metrics["certificates_outstanding"] = gen["sharesOutstanding"]

    return metrics


# ---------------------------------------------------------------------------
#  Metrics file update
# ---------------------------------------------------------------------------

def _archive_metrics(metrics_path: str) -> str | None:
    """Save a date-stamped copy of the metrics file before overwriting.

    Returns the archive path, or None if no archive was needed.
    """
    if not os.path.exists(metrics_path):
        return None

    with open(metrics_path, "rb") as f:
        old_data = f.read()

    old_hash = hashlib.sha256(old_data).hexdigest()

    # Use the as_of date from the existing metrics if available
    try:
        existing = json.loads(old_data)
        # Find the earliest as_of from any provider
        dates = [v.get("as_of", "") for v in existing.values() if isinstance(v, dict)]
        dates = [d for d in dates if d]
        tag = min(dates).replace("-", "") if dates else datetime.now().strftime("%Y%m%d")
    except Exception:
        tag = datetime.now().strftime("%Y%m%d")

    name, ext = os.path.splitext(metrics_path)
    archive_path = f"{name}_{tag}{ext}"

    # Don't create duplicate archives
    if os.path.exists(archive_path):
        with open(archive_path, "rb") as f:
            if hashlib.sha256(f.read()).hexdigest() == old_hash:
                return archive_path
        # Same date, different content — add counter
        counter = 1
        while os.path.exists(archive_path):
            archive_path = f"{name}_{tag}_{counter}{ext}"
            counter += 1

    with open(archive_path, "wb") as f:
        f.write(old_data)
    print(f"  [archive] Saved previous metrics → {os.path.basename(archive_path)}", file=sys.stderr)
    return archive_path


def update_metrics_file(
    scraped: dict[str, Any],
    metrics_path: str = METRICS_FILE,
) -> bool:
    """Merge scraped Invesco metrics into the existing etc_fund_metrics.json."""
    if not scraped:
        print("No metrics to update — scrape was not successful.", file=sys.stderr)
        return False

    existing: dict[str, Any] = {}
    if os.path.exists(metrics_path):
        _archive_metrics(metrics_path)
        with open(metrics_path, "r", encoding="utf-8") as f:
            existing = json.load(f)

    inv = existing.get("invesco", {})

    for key in (
        "certificates_outstanding",
        "entitlement_oz_per_certificate",
        "total_assets_usd",
        "umbrella_aum_usd",
        "nav_usd",
        "fixed_fee_pct",
    ):
        if key in scraped:
            inv[key] = scraped[key]

    inv["as_of"] = scraped.get("as_of", datetime.now().strftime("%Y-%m-%d"))
    inv["source_note"] = (
        f"Auto-scraped from Invesco API ({inv['as_of']}) "
        f"via dng-api.invesco.com"
    )

    existing["invesco"] = inv

    os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)
        f.write("\n")

    print(f"Updated {metrics_path}", file=sys.stderr)
    return True


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape Invesco Physical Silver ETC fund metrics from API"
    )
    p.add_argument(
        "-o", "--output",
        help="Write full scrape result (JSON) to this file",
    )
    p.add_argument(
        "--update-metrics",
        action="store_true",
        help=f"Merge scraped values into {METRICS_FILE}",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    os.makedirs(CACHE_DIR, exist_ok=True)

    try:
        metrics = fetch_invesco_metrics()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    result: dict[str, Any] = {
        "fetch_utc": datetime.now(timezone.utc).isoformat(),
        "api_endpoints": [API_FUND_DETAILS, API_GENERAL_INFO],
        "metrics": metrics,
        "success": len(metrics) > 0,
    }

    print(f"\nMetrics extracted: {len(metrics)}", file=sys.stderr)
    for k, v in sorted(metrics.items()):
        print(f"  {k}: {v}", file=sys.stderr)

    json_output = json.dumps(result, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_output)
            f.write("\n")
        print(f"\nSaved scrape result: {args.output}", file=sys.stderr)
    else:
        print(json_output)

    if args.update_metrics:
        updated = update_metrics_file(metrics)
        if not updated:
            print("WARNING: Metrics file was NOT updated.", file=sys.stderr)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
