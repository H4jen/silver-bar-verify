#!/usr/bin/env python3
"""
WisdomTree Physical Silver ETC – Product Page Scraper
=====================================================

Fetches fund metrics from the Cloudflare-protected WisdomTree product page:
  https://www.wisdomtree.eu/en-gb/products/ucits-etfs-unleveraged-etps/commodities/wisdomtree-physical-silver

Extraction targets:
  - Certificates (securities) outstanding
  - NAV per certificate (USD)
  - Total assets under management (USD)
  - Silver spot price (USD/oz)
  - Metal entitlement per certificate (oz)
  - Total physical silver (oz)

Strategies (tried in order):
  1. curl_cffi with browser TLS impersonation  (fast, no browser needed)
  2. Selenium + Brave headless                  (heavier, but reliable)

Usage:
  python fetch_wisdomtree.py                     # print metrics JSON to stdout
  python fetch_wisdomtree.py --update-metrics    # merge into etc_fund_metrics.json
  python fetch_wisdomtree.py -o output.json      # write raw scrape to file
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Any

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")
METRICS_FILE = os.path.join(CACHE_DIR, "etc_fund_metrics_wisdomtree.json")

PRODUCT_URL = (
    "https://www.wisdomtree.eu/en-gb/products/"
    "ucits-etfs-unleveraged-etps/commodities/wisdomtree-physical-silver"
)

BAR_LIST_URL = (
    "https://dataspanapi.wisdomtree.com/pdr/documents/METALBAR/MSL/UK/EN-GB/JE00B1VS3333/"
)

METAL_ENTITLEMENT_URL = (
    "https://dataspanapi.wisdomtree.com/pdr/documents/ME/MSL/UK/EN-GB/JE00B1VS3333/"
)

# Brave browser search paths (same as fetch_and_verify_barlists.py)
BRAVE_DEFAULT = "/usr/bin/brave-browser-stable"
_BRAVE_CANDIDATES = [
    "/usr/bin/brave-browser-stable",
    "/usr/bin/brave-browser",
    "/usr/bin/brave",
    "/snap/bin/brave",
    "/opt/brave.com/brave/brave-browser",
]


# ---------------------------------------------------------------------------
#  Utility helpers
# ---------------------------------------------------------------------------

def _clean_number(raw: str) -> float | None:
    """Parse a number string, stripping commas / currency symbols / whitespace."""
    if not raw:
        return None
    token = re.sub(r"[,$£€%\s]", "", raw).strip()
    if token in {"", "-", "--", "N/A", "n/a"}:
        return None
    try:
        return float(token)
    except ValueError:
        return None


def _normalise_as_of_date(raw: str) -> str:
    """Turn a date like '13 Feb 2026' or '2026-02-13' into ISO 'YYYY-MM-DD'."""
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")
    # Already ISO
    m = re.match(r"(\d{4}-\d{2}-\d{2})", raw.strip())
    if m:
        return m.group(1)
    # WisdomTree style: "13 Feb 2026" or "13 February 2026"
    for fmt in ("%d %b %Y", "%d %B %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return datetime.now().strftime("%Y-%m-%d")


def _find_brave() -> str:
    """Locate the Brave browser binary."""
    env = os.environ.get("BRAVE_BINARY")
    if env and os.path.isfile(env):
        return env
    for path in _BRAVE_CANDIDATES:
        if os.path.isfile(path):
            return path
    for name in ("brave-browser-stable", "brave-browser", "brave"):
        found = shutil.which(name)
        if found:
            return found
    return ""


# ---------------------------------------------------------------------------
#  Strategy 1 – curl_cffi  (impersonates browser TLS fingerprint)
# ---------------------------------------------------------------------------

def _fetch_with_curl_cffi(url: str, retries: int = 3) -> str | None:
    """Fetch page HTML using curl_cffi's browser impersonation."""
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError:
        print("  [curl_cffi] Not installed — skipping.", file=sys.stderr)
        return None

    # Try multiple browser impersonation profiles
    impersonates = ["chrome", "chrome110", "chrome120", "safari", "safari17_0"]

    for attempt in range(retries):
        for browser in impersonates:
            try:
                resp = cffi_requests.get(
                    url,
                    impersonate=browser,
                    timeout=30,
                    headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language": "en-GB,en;q=0.9",
                    },
                    allow_redirects=True,
                )
                if resp.status_code == 200 and len(resp.text) > 5000:
                    # Cloudflare challenge pages are typically small or contain
                    # "Checking your browser" / "Just a moment"
                    if "just a moment" in resp.text.lower()[:2000]:
                        print(f"  [curl_cffi] {browser}: got Cloudflare challenge page", file=sys.stderr)
                        continue
                    print(f"  [curl_cffi] Success with impersonate={browser}", file=sys.stderr)
                    return resp.text
                else:
                    print(
                        f"  [curl_cffi] {browser}: HTTP {resp.status_code}, "
                        f"body={len(resp.text)} bytes",
                        file=sys.stderr,
                    )
            except Exception as exc:
                print(f"  [curl_cffi] {browser}: {exc}", file=sys.stderr)

        if attempt < retries - 1:
            wait = 3 * (attempt + 1)
            print(f"  [curl_cffi] Retrying in {wait}s...", file=sys.stderr)
            time.sleep(wait)

    return None


# ---------------------------------------------------------------------------
#  Strategy 2 – Selenium + Brave headless
# ---------------------------------------------------------------------------

def _fetch_with_selenium(url: str, wait_seconds: int = 15) -> str | None:
    """Fetch page HTML with Selenium driving headless Brave.

    Waits for JS to execute so Cloudflare challenge can resolve and the
    SPA can render fund data into the DOM.
    """
    brave = _find_brave()
    if not brave:
        print("  [selenium] Brave browser not found — skipping.", file=sys.stderr)
        return None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait
    except ImportError:
        print("  [selenium] selenium not installed — skipping.", file=sys.stderr)
        return None

    print(f"  [selenium] Using {brave}", file=sys.stderr)
    driver = None
    try:
        opts = Options()
        opts.binary_location = brave
        for flag in (
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
            "--window-size=1920,1080",
        ):
            opts.add_argument(flag)

        # Reduce Selenium detection surface
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        driver = webdriver.Chrome(options=opts)

        # Remove webdriver flag from navigator
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    // Overwrite the `plugins` property to use a custom getter.
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    // Overwrite the `languages` property.
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-GB', 'en'],
                    });
                """
            },
        )

        print(f"  [selenium] Navigating to {url}", file=sys.stderr)
        driver.get(url)

        # Wait for Cloudflare challenge to resolve + SPA to render
        # Look for a sign that the real page content has loaded
        try:
            WebDriverWait(driver, wait_seconds).until(
                lambda d: len(d.page_source) > 10000
                and "just a moment" not in d.page_source.lower()[:2000]
            )
        except Exception:
            print("  [selenium] Timed out waiting for Cloudflare bypass", file=sys.stderr)

        # Extra wait for SPA JS to populate fund data
        time.sleep(5)

        html = driver.page_source
        if html and len(html) > 5000:
            print(f"  [selenium] Got {len(html):,} bytes of HTML", file=sys.stderr)
            return html

        print(f"  [selenium] Page too small ({len(html)} bytes)", file=sys.stderr)
        return None

    except Exception as exc:
        print(f"  [selenium] Error: {exc}", file=sys.stderr)
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ---------------------------------------------------------------------------
#  HTML parsing – extract fund metrics
# ---------------------------------------------------------------------------
#
#  The WisdomTree product page renders a <table> inside section#fund-nav
#  with this structure per row:
#
#    <tr>
#      <td>Label</td>
#      <td><span class="value currency positive">$76.146</span></td>
#    </tr>
#
#  Or for non-currency rows:
#    <td>Shares Outstanding</td>  <td>49,029,815</td>
#    <td>Ounces</td>              <td><div>Silver 44,703,654 troy oz</div></td>
#    <td>Metal Entitlement</td>   <td><div>Silver 0.911765 troy oz</div></td>
#
#  The NAV date is in the <thead>:
#    <th>Net Asset Value</th> <th> 12 Feb 2026 </th>
# ---------------------------------------------------------------------------

# Strip all HTML tags, returning inner text only
_TAG_RE = re.compile(r"<[^>]+>")


def _text(html_fragment: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    return _TAG_RE.sub(" ", html_fragment).strip()


def _extract_table_rows(html: str) -> list[tuple[str, str]]:
    """Extract (label, value) pairs from <td>…</td><td>…</td> rows.

    Works on the raw HTML — doesn't need an HTML parser dependency.
    Returns pairs with tags stripped from both label and value.
    """
    pairs: list[tuple[str, str]] = []
    # Match <tr> blocks that contain at least two <td> elements
    for tr_match in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE):
        tr_html = tr_match.group(1)
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr_html, re.DOTALL | re.IGNORECASE)
        if len(tds) >= 2:
            label = _text(tds[0])
            value = _text(tds[1])
            pairs.append((label, value))
    # Also check <th> pairs (used for the date header)
    for tr_match in re.finditer(r"<thead[^>]*>(.*?)</thead>", html, re.DOTALL | re.IGNORECASE):
        thead_html = tr_match.group(1)
        ths = re.findall(r"<th[^>]*>(.*?)</th>", thead_html, re.DOTALL | re.IGNORECASE)
        if len(ths) >= 2:
            label = _text(ths[0])
            value = _text(ths[1])
            pairs.append((label, value))
    return pairs


def _extract_metrics_from_html(html: str) -> dict[str, Any]:
    """Extract fund metrics from the rendered WisdomTree product page HTML.

    Targets the exact table structure of the WisdomTree product page.
    Returns a dict with the raw extracted values.
    """
    metrics: dict[str, Any] = {}

    rows = _extract_table_rows(html)

    for label, value in rows:
        label_lower = label.lower().strip()
        value_clean = value.strip()

        # ── NAV date (from <thead>): "Net Asset Value" / "12 Feb 2026" ──
        if "net asset value" in label_lower:
            metrics["as_of_date"] = value_clean

        # ── NAV: "$76.146" ──
        elif label_lower == "nav":
            n = _clean_number(value_clean)
            if n and 0.01 < n < 100_000:
                metrics["nav_usd"] = n

        # ── Total AUM of fund: "$3,733,425,641" ──
        elif "total aum" in label_lower or "total assets" in label_lower:
            n = _clean_number(value_clean)
            if n and n > 100_000:
                metrics["total_assets_usd"] = n

        # ── Issuer AUM: "$22,063,199,623" (separate field) ──
        elif "issuer aum" in label_lower:
            n = _clean_number(value_clean)
            if n and n > 100_000:
                metrics["issuer_aum_usd"] = n

        # ── Shares Outstanding: "49,029,815" ──
        elif "shares outstanding" in label_lower or "certificates outstanding" in label_lower or "securities outstanding" in label_lower:
            n = _clean_number(value_clean)
            if n and n > 1000:
                metrics["certificates_outstanding"] = int(n)

        # ── Ounces: "Silver 44,703,654 troy oz" ──
        elif label_lower == "ounces":
            m = re.search(r"([\d,]+(?:\.\d+)?)\s*troy\s*oz", value_clean, re.IGNORECASE)
            if m:
                n = _clean_number(m.group(1))
                if n and n > 10_000:
                    metrics["wisdomtree_reported_oz"] = int(n)

        # ── Metal Entitlement: "Silver 0.911765 troy oz" ──
        elif "metal entitlement" in label_lower or "entitlement" in label_lower:
            m = re.search(r"([\d.]+)\s*troy\s*oz", value_clean, re.IGNORECASE)
            if m:
                n = _clean_number(m.group(1))
                if n and 0.0001 < n < 100:
                    metrics["entitlement_oz_per_certificate"] = n

        # ── MER: "0.49%" ──
        elif label_lower == "mer":
            n = _clean_number(value_clean)
            if n is not None and 0 < n < 10:
                metrics["mer_pct"] = n

        # ── Daily Change ──
        elif "daily change" in label_lower:
            n = _clean_number(value_clean)
            if n is not None:
                metrics["daily_change_usd"] = n

        # ── Daily Return ──
        elif "daily return" in label_lower:
            n = _clean_number(value_clean)
            if n is not None:
                metrics["daily_return_pct"] = n

    return metrics


# ---------------------------------------------------------------------------
#  Main fetch orchestration
# ---------------------------------------------------------------------------

def fetch_wisdomtree_page(url: str = PRODUCT_URL) -> tuple[str | None, str]:
    """Try all strategies to fetch the WisdomTree product page.

    Returns (html, method) where method indicates which strategy succeeded.
    """
    print(f"Fetching: {url}", file=sys.stderr)

    # Strategy 1: curl_cffi
    print("\n[Strategy 1] curl_cffi with browser impersonation...", file=sys.stderr)
    html = _fetch_with_curl_cffi(url)
    if html:
        return html, "curl_cffi"

    # Strategy 2: Selenium + Brave
    print("\n[Strategy 2] Selenium + Brave headless...", file=sys.stderr)
    html = _fetch_with_selenium(url)
    if html:
        return html, "selenium_brave"

    print("\nAll strategies failed.", file=sys.stderr)
    return None, "failed"


def scrape_wisdomtree_metrics() -> dict[str, Any]:
    """Fetch the WisdomTree product page and extract fund metrics.

    Returns a dict ready to merge into etc_fund_metrics.json under the
    "wisdomtree" key.
    """
    html, method = fetch_wisdomtree_page()

    result: dict[str, Any] = {
        "fetch_method": method,
        "fetch_utc": datetime.now(timezone.utc).isoformat(),
        "url": PRODUCT_URL,
        "html_length": len(html) if html else 0,
        "metrics": {},
        "success": False,
    }

    if not html:
        result["error"] = "all_fetch_strategies_failed"
        return result

    # Save raw HTML for debugging
    debug_html_path = os.path.join(CACHE_DIR, "wisdomtree_product_page.html")
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(debug_html_path, "w", encoding="utf-8") as f:
        f.write(html)
    result["debug_html_saved"] = debug_html_path

    metrics = _extract_metrics_from_html(html)
    result["metrics"] = metrics
    result["success"] = len(metrics) > 0

    return result


def _archive_metrics(metrics_path: str, data_date: str) -> str | None:
    """Archive the current metrics file tagged by *data_date* (YYYY-MM-DD).

    Returns the archive path, or None if no archive was needed.
    """
    if not os.path.exists(metrics_path):
        return None

    with open(metrics_path, "rb") as f:
        old_data = f.read()

    old_hash = hashlib.sha256(old_data).hexdigest()
    tag = data_date.replace("-", "")

    name, ext = os.path.splitext(metrics_path)
    archive_path = f"{name}_{tag}{ext}"

    # Don't create duplicate archives
    if os.path.exists(archive_path):
        with open(archive_path, "rb") as f:
            if hashlib.sha256(f.read()).hexdigest() == old_hash:
                return archive_path
        counter = 1
        while os.path.exists(archive_path):
            archive_path = f"{name}_{tag}_{counter}{ext}"
            counter += 1

    with open(archive_path, "wb") as f:
        f.write(old_data)
    print(f"  [archive] Saved previous metrics → {os.path.basename(archive_path)}", file=sys.stderr)
    return archive_path


def update_metrics_file(scraped: dict[str, Any], metrics_path: str = METRICS_FILE) -> bool:
    """Write scraped WisdomTree metrics to the per-fund metrics file.

    File: ``etc_fund_metrics_wisdomtree.json``  (flat dict, no wrapper key).
    Before overwriting, the old file is archived as
    ``etc_fund_metrics_wisdomtree_YYYYMMDD.json`` using the old data date.
    """
    if not scraped.get("success") or not scraped.get("metrics"):
        print("No metrics to update — scrape was not successful.", file=sys.stderr)
        return False

    new_metrics = scraped["metrics"]

    # Archive the old file using its data date
    if os.path.exists(metrics_path):
        try:
            with open(metrics_path, "r", encoding="utf-8") as f:
                old = json.load(f)
            old_date = old.get("as_of", "")
        except Exception:
            old_date = ""
        if old_date:
            _archive_metrics(metrics_path, old_date)

    data: dict[str, Any] = {}
    for key in (
        "certificates_outstanding",
        "entitlement_oz_per_certificate",
        "total_assets_usd",
        "issuer_aum_usd",
        "silver_price_usd",
        "nav_usd",
        "wisdomtree_reported_oz",
        "mer_pct",
        "daily_change_usd",
        "daily_return_pct",
    ):
        if key in new_metrics:
            data[key] = new_metrics[key]

    today = datetime.now().strftime("%Y-%m-%d")
    raw_as_of = new_metrics.get("as_of_date", "")
    data_date = _normalise_as_of_date(raw_as_of) if raw_as_of else today
    data["as_of"] = data_date
    data["scraped_utc"] = datetime.now(timezone.utc).isoformat()
    data["source_note"] = (
        f"Auto-scraped from WisdomTree product page "
        f"(data date: {data_date}, scraped: {today}) "
        f"via {scraped['fetch_method']}."
    )

    os.makedirs(os.path.dirname(metrics_path) or ".", exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
        f.write("\n")

    print(f"Updated {metrics_path}", file=sys.stderr)
    return True


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape WisdomTree Physical Silver ETC product page (Cloudflare-protected)"
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
    p.add_argument(
        "--url",
        default=PRODUCT_URL,
        help="Override the product page URL",
    )
    p.add_argument(
        "--strategy",
        choices=["curl_cffi", "selenium", "all"],
        default="all",
        help="Force a specific fetch strategy (default: try all)",
    )
    p.add_argument(
        "--save-html",
        action="store_true",
        help="Save the fetched HTML to comex_data/ for debugging",
    )
    p.add_argument(
        "--selenium-wait",
        type=int,
        default=15,
        help="Seconds to wait for Cloudflare challenge in Selenium (default: 15)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    os.makedirs(CACHE_DIR, exist_ok=True)

    print(f"Target: {args.url}", file=sys.stderr)
    print(f"Strategy: {args.strategy}", file=sys.stderr)
    print(file=sys.stderr)

    html: str | None = None
    method = "failed"

    if args.strategy in ("curl_cffi", "all"):
        print("[Strategy] curl_cffi with browser impersonation...", file=sys.stderr)
        html = _fetch_with_curl_cffi(args.url)
        if html:
            method = "curl_cffi"

    if html is None and args.strategy in ("selenium", "all"):
        print("[Strategy] Selenium + Brave headless...", file=sys.stderr)
        html = _fetch_with_selenium(args.url, wait_seconds=args.selenium_wait)
        if html:
            method = "selenium_brave"

    result: dict[str, Any] = {
        "fetch_method": method,
        "fetch_utc": datetime.now(timezone.utc).isoformat(),
        "url": args.url,
        "html_length": len(html) if html else 0,
        "metrics": {},
        "success": False,
    }

    if html:
        if args.save_html:
            html_path = os.path.join(CACHE_DIR, "wisdomtree_product_page.html")
            with open(html_path, "w", encoding="utf-8") as f:
                f.write(html)
            result["debug_html_saved"] = html_path
            print(f"\nSaved HTML: {html_path}", file=sys.stderr)

        metrics = _extract_metrics_from_html(html)
        result["metrics"] = metrics
        result["success"] = len(metrics) > 0

        print(f"\nFetch method: {method}", file=sys.stderr)
        print(f"HTML length:  {len(html):,} bytes", file=sys.stderr)
        print(f"Metrics extracted: {len(metrics)}", file=sys.stderr)
        if metrics:
            for k, v in sorted(metrics.items()):
                print(f"  {k}: {v}", file=sys.stderr)
    else:
        result["error"] = "all_fetch_strategies_failed"
        print("\nERROR: Could not fetch the product page.", file=sys.stderr)
        print("Possible reasons:", file=sys.stderr)
        print("  - Cloudflare is blocking all automated access", file=sys.stderr)
        print("  - Network connectivity issue", file=sys.stderr)
        print("  - Missing dependencies (curl_cffi, selenium, brave)", file=sys.stderr)

    # Output JSON
    json_output = json.dumps(result, indent=2)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(json_output)
            f.write("\n")
        print(f"\nSaved scrape result: {args.output}", file=sys.stderr)
    else:
        print(json_output)

    # Optionally update the shared metrics file
    if args.update_metrics:
        updated = update_metrics_file(result)
        if not updated:
            print("WARNING: Metrics file was NOT updated.", file=sys.stderr)

    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
