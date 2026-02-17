#!/usr/bin/env python3
"""
COMEX Silver Data Report
========================
Fetches official silver data from CME Group / COMEX:
  1) Downloads the Metals Issues & Stops YTD delivery report (.xls)
  2) Extracts essential silver delivery data
  3) Downloads silver futures contract data (settlements, volume, open interest)
  4) Evaluates contracts, calculates silver delivered and on order (3 months ahead)
  5) Produces a text summary of findings

Silver futures contract spec:
  - Symbol: SI
  - Contract size: 5,000 troy ounces
  - Exchange: COMEX (CME Group)
"""

import os
import sys
import json
import time
import io
from datetime import datetime, timedelta
from collections import OrderedDict

import re

import requests
import pandas as pd

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")
SILVER_CONTRACT_SIZE_OZ = 5000  # 5,000 troy oz per full-size silver futures contract
TROY_OZ_PER_KG = 32.1507

# CME Group URLs
DELIVERY_REPORT_URL = "https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsYTDReport.pdf"
DAILY_DELIVERY_URL = "https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsReport.pdf"
WAREHOUSE_STOCKS_URL = "https://www.cmegroup.com/delivery_reports/Silver_stocks.xls"

# CME Group API endpoints for silver futures (product ID 458 = Silver Futures)
# The Settlements API provides settle price, volume, OI, change, high, low, last
# per contract month.  The old Volume API (/CmeWS/mvc/Volume/...) was retired by
# CME and now returns HTTP 403, but its data is redundant — the settlements
# endpoint already includes volume and open-interest fields.
SETTLEMENTS_URL = "https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/458/FUT"

# Reference HTML pages (not scraped — only used as Referer header)
SETTLEMENTS_PAGE = "https://www.cmegroup.com/markets/metals/precious/silver.settlements.html"

# Request headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "application/json,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cmegroup.com/markets/metals/precious/silver.html",
}

# JSON API headers
API_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.cmegroup.com/markets/metals/precious/silver.settlements.html",
    "X-Requested-With": "XMLHttpRequest",
}

# ---------------------------------------------------------------------------
# Contract month helpers
# ---------------------------------------------------------------------------
MONTH_CODES = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}
MONTH_NUM_TO_CODE = {v: k for k, v in MONTH_CODES.items()}
MONTH_NAMES = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}
MONTH_NAME_TO_NUM = {v: k for k, v in MONTH_NAMES.items()}


def contract_month_label(month_num, year):
    """Return e.g. 'MAR 2026'."""
    return f"{MONTH_NAMES[month_num]} {year}"


def months_in_range(start_date, num_months=3):
    """Return list of (month_num, year) for num_months starting from start_date."""
    result = []
    m, y = start_date.month, start_date.year
    for _ in range(num_months + 1):  # include current month + 3 ahead
        result.append((m, y))
        m += 1
        if m > 12:
            m = 1
            y += 1
    return result


# ---------------------------------------------------------------------------
# 1) Download delivery report PDF
# ---------------------------------------------------------------------------
def download_delivery_report(force=False):
    """Download the Metals Issues & Stops YTD delivery report (.pdf)."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    filepath = os.path.join(CACHE_DIR, "MetalsIssuesAndStopsYTDReport.pdf")

    if not force and os.path.exists(filepath):
        age_hours = (time.time() - os.path.getmtime(filepath)) / 3600
        if age_hours < 12:
            print(f"  Using cached delivery report ({age_hours:.1f}h old)")
            return filepath

    # Try PDF first (XLS is blocked by CME since ~2026), then fall back to XLS
    delivery_urls = [
        ("https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsYTDReport.pdf", "pdf"),
        ("https://www.cmegroup.com/delivery_reports/MetalsIssuesAndStopsYTDReport.xls", "xls"),
    ]

    session = requests.Session()
    session.headers.update(HEADERS)

    for url, fmt in delivery_urls:
        print(f"  Trying: {url.split('/')[-1]}...")
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200 and len(resp.content) > 500:
                save_path = os.path.join(CACHE_DIR, f"MetalsIssuesAndStopsYTDReport.{fmt}")
                with open(save_path, "wb") as f:
                    f.write(resp.content)
                print(f"  Downloaded ({len(resp.content):,} bytes)")
                return save_path
            else:
                print(f"  HTTP {resp.status_code} ({len(resp.content)} bytes)")
        except Exception as e:
            print(f"  Failed: {e}")

    print(f"  WARNING: Could not download delivery report")
    print(f"  Delivery data will be estimated from contract open interest instead.")
    return None


# ---------------------------------------------------------------------------
# 1b) Download COMEX Silver Warehouse Stocks XLS
# ---------------------------------------------------------------------------
def download_warehouse_stocks(force=False):
    """Download the Silver warehouse stocks report (.xls) from CME."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    filepath = os.path.join(CACHE_DIR, "Silver_stocks.xls")

    if not force and os.path.exists(filepath):
        age_hours = (time.time() - os.path.getmtime(filepath)) / 3600
        if age_hours < 12:
            print(f"  Using cached warehouse stocks ({age_hours:.1f}h old)")
            return filepath

    print(f"  Downloading Silver warehouse stocks from CME Group...")
    try:
        resp = requests.get(WAREHOUSE_STOCKS_URL, headers=HEADERS, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 500:
            with open(filepath, "wb") as f:
                f.write(resp.content)
            print(f"  Downloaded ({len(resp.content):,} bytes)")
            return filepath
        else:
            print(f"  HTTP {resp.status_code} ({len(resp.content)} bytes)")
    except Exception as e:
        print(f"  Failed: {e}")

    print(f"  WARNING: Could not download warehouse stocks report.")
    return None


# ---------------------------------------------------------------------------
# 1c) Parse Silver Warehouse Stocks (Registered & Eligible)
# ---------------------------------------------------------------------------
def parse_warehouse_stocks(xls_path):
    """
    Parse the Silver_stocks.xls and extract registered/eligible data
    per vault and totals.
    """
    if xls_path is None:
        return None

    print(f"  Parsing warehouse stocks report...")
    try:
        df = pd.read_excel(xls_path, header=None)

        # Extract report date and activity date
        report_date = None
        activity_date = None
        for idx, row in df.iterrows():
            for val in row.values:
                s = str(val).strip()
                if s.startswith("Report Date:"):
                    report_date = s.replace("Report Date:", "").strip()
                elif s.startswith("Activity Date:"):
                    activity_date = s.replace("Activity Date:", "").strip()

        # Parse vault-by-vault data
        vaults = []
        current_vault = None
        total_registered = 0
        total_eligible = 0
        total_combined = 0

        for idx, row in df.iterrows():
            col0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            col7 = row.iloc[7] if len(row) > 7 and pd.notna(row.iloc[7]) else None  # TOTAL TODAY

            # Grand totals
            if col0 == "TOTAL REGISTERED":
                total_registered = float(col7) if col7 else 0
                continue
            elif col0 == "TOTAL ELIGIBLE":
                total_eligible = float(col7) if col7 else 0
                continue
            elif col0 == "COMBINED TOTAL":
                total_combined = float(col7) if col7 else 0
                continue

            # Vault header (all-caps, not a category label)
            if (col0 and col0 not in ("", "nan", "NaN", "DEPOSITORY", "Troy Ounce",
                "SILVER", "COMMODITY EXCHANGE, INC.", "METAL DEPOSITORY STATISTICS")
                and col0 not in ("Registered", "Eligible", "Total")
                and not col0.startswith("TOTAL") and not col0.startswith("COMBINED")
                and not col0.startswith("The information") and not col0.startswith("the Commodity")
                and not col0.startswith("This report") and not col0.startswith("For questions")
                and col7 is None):
                current_vault = col0
                continue

            # Registered / Eligible / Total rows under a vault
            if col0 in ("Registered", "Eligible", "Total") and current_vault and col7 is not None:
                today = float(col7) if col7 else 0
                prev = float(row.iloc[2]) if pd.notna(row.iloc[2]) else 0
                received = float(row.iloc[3]) if pd.notna(row.iloc[3]) else 0
                withdrawn = float(row.iloc[4]) if pd.notna(row.iloc[4]) else 0
                net_change = float(row.iloc[5]) if pd.notna(row.iloc[5]) else 0
                adjustment = float(row.iloc[6]) if pd.notna(row.iloc[6]) else 0

                if col0 == "Registered":
                    vault_entry = {
                        "vault": current_vault,
                        "registered_prev": prev,
                        "registered_today": today,
                    }
                    vaults.append(vault_entry)
                elif col0 == "Eligible":
                    if vaults and vaults[-1]["vault"] == current_vault:
                        vaults[-1]["eligible_prev"] = prev
                        vaults[-1]["eligible_today"] = today
                    else:
                        vault_entry = {
                            "vault": current_vault,
                            "registered_prev": 0,
                            "registered_today": 0,
                            "eligible_prev": prev,
                            "eligible_today": today,
                        }
                        vaults.append(vault_entry)
                elif col0 == "Total":
                    if vaults and vaults[-1]["vault"] == current_vault:
                        vaults[-1]["total_prev"] = prev
                        vaults[-1]["total_today"] = today
                        vaults[-1]["received"] = received
                        vaults[-1]["withdrawn"] = withdrawn
                        vaults[-1]["net_change"] = net_change

        result = {
            "report_date": report_date,
            "activity_date": activity_date,
            "total_registered_oz": total_registered,
            "total_eligible_oz": total_eligible,
            "total_combined_oz": total_combined,
            "vaults": vaults,
        }

        # Convert to tonnes for convenience
        result["total_registered_tonnes"] = total_registered / TROY_OZ_PER_KG / 1000
        result["total_eligible_tonnes"] = total_eligible / TROY_OZ_PER_KG / 1000
        result["total_combined_tonnes"] = total_combined / TROY_OZ_PER_KG / 1000

        print(f"  Report Date: {report_date}")
        print(f"  Activity Date: {activity_date}")
        print(f"  Found {len(vaults)} depositories")
        print(f"  Total Registered: {total_registered:,.0f} oz ({result['total_registered_tonnes']:,.1f} t)")
        print(f"  Total Eligible:   {total_eligible:,.0f} oz ({result['total_eligible_tonnes']:,.1f} t)")
        print(f"  Combined Total:   {total_combined:,.0f} oz ({result['total_combined_tonnes']:,.1f} t)")

        return result

    except Exception as e:
        print(f"  Error parsing warehouse stocks: {e}")
        import traceback
        traceback.print_exc()
        return None


# ---------------------------------------------------------------------------
# 1d) Download and parse daily delivery report (today's deliveries + MTD)
# ---------------------------------------------------------------------------
def fetch_daily_deliveries():
    """Download the daily Issues & Stops report and extract silver entries.

    Returns a dict with keys:
      - business_date: str
      - today_deliveries: int  (contracts delivered today)
      - month_to_date: int     (official CME MTD figure)
      - delivery_month: str    (e.g. 'FEBRUARY 2026')
    or None if silver is not present in today's daily report.
    """
    print(f"  Downloading daily delivery report...")
    try:
        resp = requests.get(DAILY_DELIVERY_URL, headers=HEADERS, timeout=15)
        if resp.status_code != 200 or len(resp.content) < 500:
            print(f"  HTTP {resp.status_code} — daily delivery report unavailable")
            return None
    except Exception as e:
        print(f"  Failed to download daily delivery report: {e}")
        return None

    if pdfplumber is None:
        print("  pdfplumber not installed — cannot parse daily delivery PDF")
        return None

    try:
        pdf = pdfplumber.open(io.BytesIO(resp.content))
        full_text = ""
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"
        pdf.close()
    except Exception as e:
        print(f"  Error parsing daily delivery PDF: {e}")
        return None

    # Look for silver section
    if "SILVER" not in full_text.upper():
        print("  No silver deliveries in today's daily report")
        return None

    result = {"business_date": None, "today_deliveries": 0,
              "month_to_date": 0, "delivery_month": None}

    lines = full_text.split("\n")
    in_silver_section = False
    for line in lines:
        line_s = line.strip()

        # Capture business date
        m = re.search(r"BUSINESS DATE:\s*(\S+)", line_s)
        if m:
            result["business_date"] = m.group(1)

        # Detect silver contract header
        if "SILVER" in line_s.upper() and "CONTRACT:" in line_s.upper():
            in_silver_section = True
            # e.g. "CONTRACT: FEBRUARY 2026 COMEX SILVER 5000 OZ FUTURES"
            cm = re.search(r"CONTRACT:\s*(\w+\s+\d{4})", line_s)
            if cm:
                result["delivery_month"] = cm.group(1)
            continue

        if in_silver_section:
            # TOTAL line: "TOTAL: 414 414" or "TOTAL: 0 0"
            if line_s.startswith("TOTAL:"):
                nums = re.findall(r"[\d,]+", line_s.replace("TOTAL:", ""))
                if nums:
                    result["today_deliveries"] = int(nums[0].replace(",", ""))
            # MONTH TO DATE line
            elif "MONTH TO DATE" in line_s:
                nums = re.findall(r"[\d,]+", line_s)
                if nums:
                    result["month_to_date"] = int(nums[0].replace(",", ""))
                in_silver_section = False  # done with silver section
            # New CONTRACT: header means silver section ended
            elif "CONTRACT:" in line_s:
                in_silver_section = False

    if result["today_deliveries"] > 0 or result["month_to_date"] > 0:
        print(f"  Silver daily deliveries: {result['today_deliveries']:,} today, "
              f"{result['month_to_date']:,} MTD ({result['delivery_month']})")
        return result

    print("  No silver deliveries in today's daily report")
    return None


# ---------------------------------------------------------------------------
# 2) Extract silver delivery data from delivery report
# ---------------------------------------------------------------------------
def extract_silver_deliveries(report_path):
    """Parse the delivery report (PDF or XLS) and extract silver-specific data."""
    if report_path is None:
        print("  No delivery report available, using CME web data instead.")
        return None

    if report_path.lower().endswith(".pdf"):
        return _extract_silver_from_pdf(report_path)
    else:
        return _extract_silver_from_xls(report_path)


def _extract_silver_from_pdf(pdf_path):
    """Parse the CME Issues & Stops YTD PDF for silver delivery data."""
    if pdfplumber is None:
        print("  pdfplumber not installed — run: pip install pdfplumber")
        return None

    print(f"  Parsing PDF delivery report...")
    try:
        pdf = pdfplumber.open(pdf_path)
    except Exception as e:
        print(f"  Error opening PDF: {e}")
        return None

    try:
        silver_pages = []
        month_headers = []  # column headers like DEC | JAN | FEB ...
        firms = []          # per-firm delivery records
        totals = {}         # month -> total contracts
        business_date = None

        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Capture business date from any page
            if not business_date:
                m = re.search(r"BUSINESS DATE:\s*(\S+)", text)
                if m:
                    business_date = m.group(1)

            # Only process COMEX 5000 SILVER FUTURES pages
            if "COMEX 5000 SILVER FUTURES" not in text.upper():
                continue

            silver_pages.append(page)

            for line in text.split("\n"):
                line_s = line.strip()

                # Parse month header row.
                # The header looks like:
                #   "FIRM NAME O I/S PREV DEC | JAN | FEB | MAR | ..."
                # "PREV DEC" is ONE column (= previous December), then
                # the pipe-delimited months follow: JAN, FEB, MAR, ...
                # The TOTALS line has one value per pipe segment:
                #   "TOTALS: | 12946 | 9889 | 4595 | ..."
                # So segment 1 = PREV DEC, segment 2 = JAN, etc.
                if not month_headers and "PREV" in line_s and "|" in line_s:
                    parts = re.split(r"[|]", line_s)
                    # Segment 0 ends with "PREV DEC" — that's the first
                    # data column.  Pipe segments 1..12 are JAN..DEC.
                    seg0 = parts[0].strip()
                    if "PREV" in seg0:
                        # Extract the month after PREV in seg0 (e.g. "DEC")
                        tokens = seg0.split()
                        prev_idx = next(i for i, t in enumerate(tokens)
                                        if t.upper() == "PREV")
                        prev_month = tokens[prev_idx + 1].upper() if prev_idx + 1 < len(tokens) else ""
                        if prev_month in MONTH_NAME_TO_NUM:
                            month_headers.append(f"PREV {prev_month}")
                        else:
                            month_headers.append("PREV")
                    # Then the pipe-delimited months
                    for col in parts[1:]:
                        tok = col.strip().upper()
                        if tok in MONTH_NAME_TO_NUM:
                            month_headers.append(tok)

                # Totals line: "TOTALS: | 12946 | 9889 | 4595 | ..."
                if line_s.startswith("TOTALS:"):
                    parts = re.split(r"[|]", line_s)
                    vals = [p.strip() for p in parts[1:]]  # skip "TOTALS:"
                    # month_headers is already clean: [PREV, DEC, JAN, FEB, ...]
                    for i, val in enumerate(vals):
                        v = val.replace(",", "").strip()
                        if v and i < len(month_headers):
                            try:
                                totals[month_headers[i]] = int(v)
                            except ValueError:
                                pass

                # Firm line: "072 | | I | 271 | 29 | 2| ..."
                # and name line: "GOLDMAN |C| S | 0 | 0 | 0| ..."
                if "|" in line_s and not line_s.startswith("_") and not line_s.startswith("TOTALS"):
                    parts = [p.strip() for p in re.split(r"[|]", line_s)]
                    # Firm name lines contain letters
                    if parts and parts[0] and parts[0][0].isalpha():
                        firm_name = parts[0]
                        i_s = None
                        for p in parts[1:4]:
                            if p in ("I", "S"):
                                i_s = p
                                break
                        firms.append({
                            "firm": firm_name,
                            "type": i_s,
                            "values": parts,
                        })

        if not silver_pages:
            print("  No COMEX 5000 Silver Futures data found in PDF")
            return None

        print(f"  Business Date: {business_date}")
        print(f"  Found {len(silver_pages)} silver pages, {len(firms)} firm entries")

        # Build structured result
        result = {
            "source": "pdf",
            "business_date": business_date,
            "product": "COMEX 5000 SILVER FUTURES",
            "month_headers": month_headers,
            "totals": totals,  # e.g. {"PREV DEC": 12946, "JAN": 9889, "FEB": 4595}
            "firms": firms,
        }

        if totals:
            # Show summary of totals
            total_parts = [f"{m}: {v:,}" for m, v in totals.items()]
            print(f"  Delivery Totals (contracts): {', '.join(total_parts)}")

        return result

    except Exception as e:
        print(f"  Error parsing PDF: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        pdf.close()


def _extract_silver_from_xls(xls_path):
    """Legacy: Parse the delivery report XLS for silver-specific data."""
    print(f"  Parsing XLS delivery report...")
    try:
        xls = pd.ExcelFile(xls_path)
        silver_data = []

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, header=None)

            for idx, row in df.iterrows():
                row_str = " ".join(str(v) for v in row.values if pd.notna(v)).upper()
                if "SILVER" in row_str or "SI " in row_str:
                    silver_data.append({
                        "sheet": sheet_name,
                        "row": idx,
                        "data": [v for v in row.values if pd.notna(v)],
                    })

        if silver_data:
            print(f"  Found {len(silver_data)} silver-related entries")
            return {"source": "xls", "raw_entries": silver_data}
        else:
            print("  No silver entries found in XLS")
            return None

    except Exception as e:
        print(f"  Error parsing XLS: {e}")
        return None


# ---------------------------------------------------------------------------
# 3) Download contract data (settlements + volume/OI) from CME
# ---------------------------------------------------------------------------
def get_last_trade_date():
    """Get the most recent trade date in MM/DD/YYYY format for CME API calls."""
    now = datetime.now()
    # If before 6 PM CT (roughly 00:00 UTC next day), use yesterday
    # CME settlements are published after market close ~5:00 PM CT
    candidate = now - timedelta(days=1)
    # Skip weekends
    while candidate.weekday() >= 5:  # 5=Saturday, 6=Sunday
        candidate -= timedelta(days=1)
    return candidate.strftime("%m/%d/%Y")


def fetch_settlements_data():
    """Fetch silver futures settlement data from CME Group JSON API."""
    print(f"  Fetching settlement data...")

    # Try dates going back up to 5 business days
    for day_offset in range(6):
        dt = datetime.now() - timedelta(days=1 + day_offset)
        while dt.weekday() >= 5:
            dt -= timedelta(days=1)
        td = dt.strftime("%m/%d/%Y")

        url = (f"https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/"
               f"Settlements/458/FUT?strategy=DEFAULT&tradeDate={td}&pageSize=50")
        try:
            resp = requests.get(url, headers=API_HEADERS, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                settlements = data.get("settlements", [])
                # Filter out the "Total" row and empty entries
                real = [s for s in settlements
                        if s.get("month", "").upper() != "TOTAL"
                        and parse_number(s.get("openInterest", 0)) > 0]
                if real:
                    print(f"  Retrieved {len(real)} active contract months "
                          f"(trade date: {data.get('tradeDate', td)})")
                    print(f"  Report type: {data.get('reportType', 'N/A')}")
                    # Save raw API response
                    _save_raw_json(data, "settlements_raw.json")
                    return data
        except Exception as e:
            print(f"  API attempt for {td} failed: {e}")
            continue

    print("  WARNING: Could not fetch settlement data from CME API.")

    # Fall back to yfinance
    yf_data = fetch_settlements_via_yfinance()
    if yf_data:
        return yf_data

    # Fall back to cached data if available
    cache_path = os.path.join(CACHE_DIR, "settlements_raw.json")
    if os.path.exists(cache_path):
        try:
            age_hours = (time.time() - os.path.getmtime(cache_path)) / 3600
            with open(cache_path) as f:
                data = json.load(f)
            settlements = data.get("settlements", [])
            real = [s for s in settlements
                    if s.get("month", "").upper() != "TOTAL"
                    and parse_number(s.get("openInterest", 0)) > 0]
            if real:
                print(f"  Using cached settlement data ({age_hours:.1f}h old, "
                      f"trade date: {data.get('tradeDate', '?')})")
                return data
        except Exception:
            pass

    return None


def fetch_settlements_via_yfinance():
    """Fetch silver futures settlement data from Yahoo Finance (yfinance).
    
    This serves as a fallback when the CME Group API is unavailable.
    Queries individual contract month tickers (e.g. SIH26.CMX for MAR 26).
    """
    try:
        import yfinance as yf
    except ImportError:
        print("  yfinance not available for fallback.")
        return None

    print("  Trying yfinance as alternative data source...")
    now = datetime.now()
    settlements = []
    years = [now.year % 100, (now.year + 1) % 100]  # e.g. [26, 27]
    if now.month >= 11:
        years.append((now.year + 2) % 100)

    # Suppress yfinance HTTP 404 noise for expired/invalid contracts
    import logging
    yf_logger = logging.getLogger("yfinance")
    prev_level = yf_logger.level
    yf_logger.setLevel(logging.CRITICAL)

    count = 0
    for yr in years:
        for code, month_num in MONTH_CODES.items():
            symbol = f"SI{code}{yr:02d}.CMX"
            try:
                t = yf.Ticker(symbol)
                info = t.info
                price = info.get("regularMarketPrice") or info.get("previousClose")
                oi = info.get("openInterest", 0) or 0
                vol = info.get("regularMarketVolume") or info.get("volume", 0) or 0
                if not price or price <= 0:
                    continue

                prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose", 0) or 0
                change = round(price - prev_close, 3) if prev_close else 0
                month_label = f"{MONTH_NAMES[month_num]} {yr:02d}"

                settlements.append({
                    "month": month_label,
                    "open": str(info.get("regularMarketOpen", "-")),
                    "high": str(info.get("dayHigh", "-")),
                    "low": str(info.get("dayLow", "-")),
                    "last": str(price),
                    "change": str(change),
                    "settle": str(prev_close if prev_close else price),
                    "volume": f"{vol:,}",
                    "openInterest": f"{oi:,}",
                })
                count += 1
            except Exception:
                continue

    # Restore yfinance logging
    yf_logger.setLevel(prev_level)

    if not settlements:
        print("  yfinance: No contract data found.")
        return None

    # Sort by year then month
    def sort_key(s):
        parts = s["month"].split()
        mn = MONTH_NAME_TO_NUM.get(parts[0], 0)
        yr = int(parts[1]) if len(parts) > 1 else 0
        return (yr, mn)
    settlements.sort(key=sort_key)

    trade_date = now.strftime("%m/%d/%Y")
    data = {
        "tradeDate": trade_date,
        "reportType": "yfinance",
        "settlements": settlements,
    }
    print(f"  yfinance: Retrieved {count} active contract months")
    _save_raw_json(data, "settlements_raw.json")
    return data



# ---------------------------------------------------------------------------
# Helpers — save raw data
# ---------------------------------------------------------------------------
def _save_raw_json(data, filename):
    """Save raw JSON data to the cache directory."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, filename)
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception:
        pass


def _extract_trade_date(settlements, delivery_data, warehouse_data):
    """Derive the canonical CME trade date from available sources.

    Priority:
      1. settlements tradeDate  (e.g. '02/13/2026')
      2. delivery_summary business_date
      3. warehouse_stocks report_date

    Returns an ISO-style ``YYYYMMDD`` string, or ``None`` if no date found.
    """
    raw = None

    # 1) Settlements API — most reliable
    if settlements and isinstance(settlements, dict):
        raw = settlements.get("tradeDate")

    # 2) Delivery report business_date
    if not raw and delivery_data and isinstance(delivery_data, dict):
        raw = delivery_data.get("business_date")

    # 3) Warehouse stocks report_date
    if not raw and warehouse_data and isinstance(warehouse_data, dict):
        raw = warehouse_data.get("report_date")

    if not raw:
        return None

    # Normalise to YYYYMMDD.  Common CME formats:
    #   '02/13/2026'  (MM/DD/YYYY)
    #   '2/13/2026'   (M/DD/YYYY)
    raw = raw.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y%m%d")
        except ValueError:
            continue
    return None


TREND_FILE = os.path.join(CACHE_DIR, "silver_trend_history.json")


def _load_trend_history():
    """Load historical trend data from disk."""
    if os.path.exists(TREND_FILE):
        try:
            with open(TREND_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_trend_snapshot(silver_price, contracts, delivery_summary,
                         warehouse_data, daily_deliveries):
    """Save today's key metrics as a trend data point.

    The history file is keyed by date (YYYY-MM-DD).  Only one snapshot
    per calendar day is kept (the latest run overwrites earlier ones).
    """
    now = datetime.now()
    date_key = now.strftime("%Y-%m-%d")

    history = _load_trend_history()

    # Compute key metrics
    all_oi = sum(c.get("open_interest", 0) for c in contracts)

    # Target months OI (current + 3 ahead)
    target_months = months_in_range(now, num_months=3)
    target_labels = set()
    for m, y in target_months:
        target_labels.add(f"{MONTH_NAMES[m]} {y}")
        target_labels.add(f"{MONTH_NAMES[m]} {str(y)[2:]}")
    target_oi = 0
    for c in contracts:
        label = c.get("month_label", "")
        is_target = any(label.upper().startswith(tl.split()[0]) and
                       label.upper().endswith(tl.split()[-1]) if len(tl.split()) > 1
                       else label.upper().startswith(tl)
                       for tl in target_labels)
        if is_target:
            target_oi += c.get("open_interest", 0)

    # YTD delivered (excluding PREV months)
    current_month_name = MONTH_NAMES[now.month]
    ytd_delivered = 0
    current_month_delivered = 0
    if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
        for mon, num in delivery_summary.get("totals", {}).items():
            if not mon.startswith("PREV"):
                ytd_delivered += num
            if mon.upper() == current_month_name:
                current_month_delivered = num

    # Warehouse
    wh_registered = 0
    wh_eligible = 0
    wh_combined = 0
    if warehouse_data:
        wh_registered = warehouse_data.get("total_registered_oz", 0)
        wh_eligible = warehouse_data.get("total_eligible_oz", 0)
        wh_combined = warehouse_data.get("total_combined_oz", 0)

    # Per-month delivery breakdown (excluding PREV months)
    monthly_deliveries = {}
    if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
        for mon, num in delivery_summary.get("totals", {}).items():
            if not mon.startswith("PREV") and num > 0:
                monthly_deliveries[mon] = num

    snapshot = {
        "timestamp": now.isoformat(),
        "silver_price": silver_price,
        "all_oi": all_oi,
        "target_oi": target_oi,
        "ytd_delivered_contracts": ytd_delivered,
        "current_month_delivered": current_month_delivered,
        "current_month": current_month_name,
        "monthly_deliveries": monthly_deliveries,
        "warehouse_registered_oz": wh_registered,
        "warehouse_eligible_oz": wh_eligible,
        "warehouse_combined_oz": wh_combined,
    }

    # Per-contract OI for the next 6 months
    six_months = months_in_range(now, num_months=5)
    six_labels = set()
    for m, y in six_months:
        six_labels.add(f"{MONTH_NAMES[m]} {y}")
        six_labels.add(f"{MONTH_NAMES[m]} {str(y)[2:]}")
    contract_oi = {}
    for c in contracts:
        label = c.get("month_label", "")
        oi = c.get("open_interest", 0)
        in_six = any(label.upper().startswith(sl.split()[0]) and
                     label.upper().endswith(sl.split()[-1]) if len(sl.split()) > 1
                     else label.upper().startswith(sl)
                     for sl in six_labels)
        if in_six and oi > 0:
            contract_oi[label] = oi
    snapshot["contract_oi"] = contract_oi

    history[date_key] = snapshot

    # Keep last 365 days of history
    if len(history) > 365:
        sorted_keys = sorted(history.keys())
        for k in sorted_keys[:-365]:
            del history[k]

    os.makedirs(CACHE_DIR, exist_ok=True)
    try:
        with open(TREND_FILE, "w") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"  Warning: Could not save trend history: {e}")


def compute_trend(history, today_key):
    """Compute trend deltas from historical snapshots.

    Returns a dict with keys like:
      {
        "1d": {"silver_price": +0.5, "all_oi": -200, ...},
        "7d": {...},
        "30d": {...},
        "prev_date": "2026-02-12",
      }
    """
    if today_key not in history:
        return None

    today = history[today_key]
    sorted_dates = sorted(history.keys())
    today_idx = sorted_dates.index(today_key)

    def find_prior(days_back):
        """Find the closest snapshot to N days ago."""
        target = datetime.strptime(today_key, "%Y-%m-%d") - timedelta(days=days_back)
        target_str = target.strftime("%Y-%m-%d")
        # Find closest date <= target
        best = None
        for d in sorted_dates:
            if d <= target_str and d != today_key:
                best = d
        return best

    def calc_delta(prior_key):
        if prior_key is None:
            return None
        prior = history[prior_key]
        delta = {"date": prior_key}
        numeric_fields = [
            "silver_price", "all_oi", "target_oi",
            "ytd_delivered_contracts", "current_month_delivered",
            "warehouse_registered_oz", "warehouse_eligible_oz",
            "warehouse_combined_oz",
        ]
        for field in numeric_fields:
            t_val = today.get(field)
            p_val = prior.get(field)
            if t_val is not None and p_val is not None:
                delta[field] = t_val - p_val
                if p_val != 0:
                    delta[f"{field}_pct"] = (t_val - p_val) / abs(p_val) * 100
            else:
                delta[field] = None

        # Per-contract OI changes
        t_oi = today.get("contract_oi", {})
        p_oi = prior.get("contract_oi", {})
        oi_changes = {}
        for label in set(list(t_oi.keys()) + list(p_oi.keys())):
            t = t_oi.get(label, 0)
            p = p_oi.get(label, 0)
            if t != p:
                oi_changes[label] = t - p
        delta["contract_oi_changes"] = oi_changes

        # Per-month delivery changes
        t_del = today.get("monthly_deliveries", {})
        p_del = prior.get("monthly_deliveries", {})
        del_changes = {}
        for mon in set(list(t_del.keys()) + list(p_del.keys())):
            t = t_del.get(mon, 0)
            p = p_del.get(mon, 0)
            if t != p:
                del_changes[mon] = t - p
        delta["delivery_changes"] = del_changes
        return delta

    result = {}

    # Previous day (most recent prior entry)
    if today_idx > 0:
        prev_key = sorted_dates[today_idx - 1]
        result["prev"] = calc_delta(prev_key)
    else:
        result["prev"] = None

    result["7d"] = calc_delta(find_prior(7))
    result["30d"] = calc_delta(find_prior(30))

    return result


# ---------------------------------------------------------------------------
# 4) Evaluate contract data, calculate deliveries and standing-for-delivery
# ---------------------------------------------------------------------------
def parse_number(s):
    """Parse a number string that may have commas, +/- signs, or letters."""
    if s is None:
        return 0
    s = str(s).strip().replace(",", "").replace("+", "")
    # Remove trailing letters like 'A', 'B' (ask/bid indicators)
    while s and s[-1].isalpha():
        s = s[:-1]
    try:
        return float(s) if "." in s else int(s)
    except (ValueError, TypeError):
        return 0


def evaluate_contracts(settlements_data, delivery_data):
    """
    Evaluate silver futures contracts for the next 3 months.
    Calculate delivered ounces and ounces standing for delivery.
    """
    now = datetime.now()
    target_months = months_in_range(now, num_months=3)
    print(f"\n  Analyzing contracts for: {', '.join(contract_month_label(m, y) for m, y in target_months)}")

    contracts = OrderedDict()

    # --- Parse settlement data from JSON API ---
    if settlements_data and "settlements" in settlements_data:
        trade_date = settlements_data.get("tradeDate", "")
        for entry in settlements_data["settlements"]:
            month_str = entry.get("month", "").upper()
            if month_str == "TOTAL":
                continue  # skip summary row

            settle = parse_number(entry.get("settle", 0))
            volume = parse_number(entry.get("volume", 0))
            oi = parse_number(entry.get("openInterest", 0))
            change = parse_number(entry.get("change", 0))
            high = parse_number(entry.get("high", 0))
            low = parse_number(entry.get("low", 0))
            last = parse_number(entry.get("last", 0))

            contracts[month_str] = {
                "month_label": month_str,
                "settle_price": settle,
                "high": high,
                "low": low,
                "last": last,
                "volume": volume,
                "open_interest": oi,
                "change": change,
                "trade_date": trade_date,
                "source": "cme_api",
            }

    # --- Calculate silver ounces ---
    results = []
    for label, c in contracts.items():
        oi = c.get("open_interest", 0) or c.get("oi_at_close", 0)
        deliveries = c.get("deliveries_today", 0)
        settle = c.get("settle_price", 0)

        oz_standing = oi * SILVER_CONTRACT_SIZE_OZ
        oz_delivered_today = deliveries * SILVER_CONTRACT_SIZE_OZ
        kg_standing = oz_standing / TROY_OZ_PER_KG
        tonnes_standing = kg_standing / 1000

        c["oz_standing_for_delivery"] = oz_standing
        c["oz_delivered_today"] = oz_delivered_today
        c["kg_standing"] = kg_standing
        c["tonnes_standing"] = tonnes_standing

        results.append(c)

    # --- Pass through delivery data as-is ---
    delivery_summary = {}
    if delivery_data:
        if isinstance(delivery_data, dict) and delivery_data.get("source") == "pdf":
            delivery_summary = delivery_data
        elif isinstance(delivery_data, dict) and delivery_data.get("source") == "xls":
            for entry in delivery_data.get("raw_entries", []):
                data = entry.get("data", [])
                if len(data) >= 3:
                    delivery_summary[str(data[0])] = data
        elif isinstance(delivery_data, list):
            for entry in delivery_data:
                data = entry.get("data", [])
                if len(data) >= 3:
                    delivery_summary[str(data[0])] = data

    return results, delivery_summary


# ---------------------------------------------------------------------------
# 5) Generate text summary
# ---------------------------------------------------------------------------
def generate_summary(contracts, delivery_summary, silver_price=None, warehouse_data=None,
                     daily_deliveries=None, trend_data=None):
    """Generate a comprehensive text summary of COMEX silver data."""
    now = datetime.now()
    target_months = months_in_range(now, num_months=3)
    target_labels = set()
    for m, y in target_months:
        target_labels.add(f"{MONTH_NAMES[m]} {y}")
        target_labels.add(f"{MONTH_NAMES[m]} {str(y)[2:]}")

    lines = []
    lines.append("=" * 78)
    lines.append("  COMEX SILVER FUTURES — DATA REPORT")
    lines.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 78)
    lines.append("")

    # --- Current price ---
    if silver_price:
        lines.append(f"  Current Silver Price: ${silver_price:.3f} / troy oz")
        lines.append("")

    # --- Contract overview ---
    lines.append("-" * 78)
    lines.append("  SILVER FUTURES CONTRACTS — NEXT 3 MONTHS")
    lines.append("-" * 78)
    lines.append("")
    lines.append(f"  {'Contract':<10} {'Settle':>9} {'OI':>8} {'Vol':>8} "
                 f"{'Chg':>7} {'Standing oz':>14} {'Tonnes':>9}")
    lines.append(f"  {'─' * 8:<10} {'─' * 7:>9} {'─' * 6:>8} {'─' * 6:>8} "
                 f"{'─' * 5:>7} {'─' * 12:>14} {'─' * 7:>9}")

    total_oi = 0
    total_oz_standing = 0
    total_tonnes = 0
    active_contracts = []

    for c in contracts:
        label = c.get("month_label", "???")
        # Check if this is one of our target months
        is_target = any(label.upper().startswith(tl.split()[0]) and
                       label.upper().endswith(tl.split()[-1]) if len(tl.split()) > 1
                       else label.upper().startswith(tl)
                       for tl in target_labels)

        oi = c.get("open_interest", 0)
        settle = c.get("settle_price", 0)
        vol = c.get("volume", 0)
        px_chg = c.get("change", 0)
        oz = c.get("oz_standing_for_delivery", 0)
        tonnes = c.get("tonnes_standing", 0)

        if oi > 0 or is_target:
            marker = " *" if is_target else "  "
            lines.append(f"{marker}{label:<10} {settle:>9,.2f} {oi:>8,} {vol:>8,} "
                        f"{px_chg:>+7.2f} {oz:>14,} {tonnes:>9,.1f}")
            active_contracts.append(c)

            if is_target:
                total_oi += oi
                total_oz_standing += oz
                total_tonnes += tonnes

    lines.append("")
    lines.append(f"  * = Target months (current + 3 months ahead)")
    lines.append("")

    # --- Delivery summary ---
    lines.append("-" * 78)
    lines.append("  DELIVERY & STANDING SUMMARY (Target Period)")
    lines.append("-" * 78)
    lines.append("")
    lines.append(f"  Total Open Interest (target months):   {total_oi:>12,} contracts")
    lines.append(f"  Silver Standing for Delivery:          {total_oz_standing:>12,} troy oz")
    lines.append(f"                                         {total_tonnes:>12,.1f} metric tonnes")
    lines.append(f"                                         {total_tonnes * 1000:>12,.0f} kg")
    lines.append("")

    if silver_price and silver_price > 0:
        total_value = total_oz_standing * silver_price
        lines.append(f"  Notional Value of Standing Silver:     ${total_value:>14,.0f}")
        lines.append("")

    # Determine the current calendar month name (e.g. "FEB") for MTD labelling
    current_month_name = MONTH_NAMES[now.month]

    # --- Delivery report data ---
    if delivery_summary:
        lines.append("-" * 78)
        lines.append("  YTD DELIVERY REPORT (from CME Issues & Stops)")
        lines.append("-" * 78)
        lines.append("")

        if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
            biz_date = delivery_summary.get("business_date", "N/A")
            totals = delivery_summary.get("totals", {})
            lines.append(f"  Business Date: {biz_date}")
            lines.append(f"  Product: {delivery_summary.get('product', 'Silver Futures')}")
            lines.append("")
            if totals:
                lines.append(f"  {'Month':<16} {'Contracts':>12} {'Troy Ounces':>16}")
                lines.append(f"  {'─' * 14:<16} {'─' * 10:>12} {'─' * 14:>16}")
                ytd_total = 0
                for mon, num_contracts in totals.items():
                    oz = num_contracts * SILVER_CONTRACT_SIZE_OZ
                    # Label the current month as (MTD)
                    display_mon = mon
                    if mon.upper() == current_month_name:
                        display_mon = f"{mon} (MTD)"
                    # PREV months are prior-year carryover, not current YTD
                    if mon.startswith("PREV"):
                        display_mon = f"{mon} (prior yr)"
                    else:
                        ytd_total += num_contracts
                    lines.append(f"  {display_mon:<16} {num_contracts:>12,} {oz:>16,}")
                lines.append(f"  {'─' * 14:<16} {'─' * 10:>12} {'─' * 14:>16}")
                ytd_oz = ytd_total * SILVER_CONTRACT_SIZE_OZ
                lines.append(f"  {'YTD Total':<16} {ytd_total:>12,} {ytd_oz:>16,}")
                lines.append("")
                if silver_price and silver_price > 0:
                    lines.append(f"  YTD Delivered Value:   ${ytd_oz * silver_price:>18,.0f}")
                    lines.append("")
        else:
            for key, data in delivery_summary.items():
                lines.append(f"  {' | '.join(str(d) for d in data)}")
            lines.append("")

    # --- Current month deliveries to date ---
    # Show deliveries for the active delivery month with daily detail
    current_month_contracts = 0
    if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
        totals = delivery_summary.get("totals", {})
        current_month_contracts = totals.get(current_month_name, 0)

    if current_month_contracts > 0 or daily_deliveries:
        lines.append("-" * 78)
        lines.append(f"  CURRENT MONTH DELIVERIES — {MONTH_NAMES[now.month]} {now.year}")
        lines.append("-" * 78)
        lines.append("")
        if current_month_contracts > 0:
            cm_oz = current_month_contracts * SILVER_CONTRACT_SIZE_OZ
            cm_tonnes = cm_oz / TROY_OZ_PER_KG / 1000
            lines.append(f"  Month-to-Date Delivered:  {current_month_contracts:>10,} contracts")
            lines.append(f"                           {cm_oz:>10,} troy oz")
            lines.append(f"                           {cm_tonnes:>10,.1f} metric tonnes")
            if silver_price and silver_price > 0:
                lines.append(f"  MTD Delivered Value:     ${cm_oz * silver_price:>14,.0f}")
        if daily_deliveries:
            today_del = daily_deliveries.get("today_deliveries", 0)
            daily_mtd = daily_deliveries.get("month_to_date", 0)
            daily_date = daily_deliveries.get("business_date", "N/A")
            lines.append("")
            lines.append(f"  Daily Report ({daily_date}):")
            lines.append(f"    Today's Deliveries:    {today_del:>10,} contracts")
            if daily_mtd > 0:
                lines.append(f"    CME Official MTD:      {daily_mtd:>10,} contracts")
        elif current_month_contracts > 0:
            lines.append("")
            lines.append(f"  (No silver entries in today's daily delivery report)")
        lines.append("")

    # --- All contracts overview ---
    lines.append("-" * 78)
    lines.append("  ALL ACTIVE CONTRACTS OVERVIEW")
    lines.append("-" * 78)
    lines.append("")

    all_oi = sum(c.get("open_interest", 0) for c in contracts)
    all_oz = sum(c.get("oz_standing_for_delivery", 0) for c in contracts)
    all_tonnes = all_oz / TROY_OZ_PER_KG / 1000

    lines.append(f"  Total Open Interest (all months):      {all_oi:>12,} contracts")
    lines.append(f"  Total Silver Represented:              {all_oz:>12,} troy oz")
    lines.append(f"                                         {all_tonnes:>12,.1f} metric tonnes")
    lines.append("")

    if silver_price and silver_price > 0:
        all_value = all_oz * silver_price
        lines.append(f"  Total Notional Value:                  ${all_value:>14,.0f}")
        lines.append("")

    # --- COMEX Warehouse Stocks: Registered & Eligible ---
    if warehouse_data:
        lines.append("-" * 78)
        lines.append("  COMEX WAREHOUSE SILVER STOCKS (Registered & Eligible)")
        if warehouse_data.get("report_date"):
            lines.append(f"  Report Date: {warehouse_data['report_date']}  |  "
                        f"Activity Date: {warehouse_data.get('activity_date', 'N/A')}")
        lines.append("-" * 78)
        lines.append("")

        reg_oz = warehouse_data.get("total_registered_oz", 0)
        elig_oz = warehouse_data.get("total_eligible_oz", 0)
        comb_oz = warehouse_data.get("total_combined_oz", 0)
        reg_t = warehouse_data.get("total_registered_tonnes", 0)
        elig_t = warehouse_data.get("total_eligible_tonnes", 0)
        comb_t = warehouse_data.get("total_combined_tonnes", 0)

        lines.append(f"  {'Category':<22} {'Troy Ounces':>18} {'Metric Tonnes':>16}")
        lines.append(f"  {'─' * 20:<22} {'─' * 16:>18} {'─' * 14:>16}")
        lines.append(f"  {'Registered':<22} {reg_oz:>18,.0f} {reg_t:>16,.1f}")
        lines.append(f"  {'Eligible':<22} {elig_oz:>18,.0f} {elig_t:>16,.1f}")
        lines.append(f"  {'Combined Total':<22} {comb_oz:>18,.0f} {comb_t:>16,.1f}")
        lines.append("")

        if silver_price and silver_price > 0:
            reg_value = reg_oz * silver_price
            elig_value = elig_oz * silver_price
            comb_value = comb_oz * silver_price
            lines.append(f"  Registered Value:    ${reg_value:>18,.0f}")
            lines.append(f"  Eligible Value:      ${elig_value:>18,.0f}")
            lines.append(f"  Combined Value:      ${comb_value:>18,.0f}")
            lines.append("")

        # Coverage ratio: OI vs warehouse stocks
        if comb_oz > 0 and total_oz_standing > 0:
            coverage = comb_oz / total_oz_standing * 100
            lines.append(f"  Warehouse Coverage Ratio:  {coverage:>8.1f}%")
            lines.append(f"    (warehouse silver / silver standing for delivery in target period)")
            if coverage < 100:
                lines.append(f"    ⚠  Warehouse stocks BELOW contracts standing for delivery!")
            lines.append("")

        # Per-vault breakdown
        vaults = warehouse_data.get("vaults", [])
        if vaults:
            lines.append(f"  {'Depository':<42} {'Registered':>14} {'Eligible':>14}")
            lines.append(f"  {'─' * 40:<42} {'─' * 12:>14} {'─' * 12:>14}")
            for v in vaults:
                name = v.get('vault', '?')[:40]
                reg = v.get('registered_today', 0)
                elig = v.get('eligible_today', 0)
                if reg > 0 or elig > 0:
                    lines.append(f"  {name:<42} {reg:>14,.0f} {elig:>14,.0f}")
            lines.append("")

    lines.append("-" * 78)
    lines.append("  KEY OBSERVATIONS & ANALYSIS")
    lines.append("-" * 78)
    lines.append("")

    # Find the front month (highest OI among target months)
    if active_contracts:
        front = max(active_contracts,
                    key=lambda x: x.get("open_interest", 0))
        front_oi = front.get("open_interest", 0)
        front_oz = front.get("oz_standing_for_delivery", 0)
        lines.append(f"  • Front month: {front.get('month_label', '?')} with "
                    f"{front_oi:,} contracts ({front_oz:,} oz)")

    # Delivery month check — match current month AND current year
    current_label = f"{MONTH_NAMES[now.month]} {str(now.year)[2:]}"
    for c in contracts:
        label = c.get("month_label", "").upper()
        if label == current_label:
            oi = c.get("open_interest", 0)
            if oi > 0:
                lines.append(f"  • Current delivery month ({label}): "
                            f"{oi:,} contracts still open = {oi * SILVER_CONTRACT_SIZE_OZ:,} oz")

    # Highlight contracts with large OI (potential delivery pressure)
    for c in contracts:
        oi = c.get("open_interest", 0)
        label = c.get("month_label", "")
        if oi > 5000 and label != current_label:
            lines.append(f"  • {label}: {oi:,} contracts open interest "
                        f"({oi * SILVER_CONTRACT_SIZE_OZ:,} oz standing)")

    lines.append("")

    # --- Trend analysis ---
    if trend_data:
        lines.append("-" * 78)
        lines.append("  TREND ANALYSIS")
        lines.append("-" * 78)
        lines.append("")

        def fmt_delta(val, is_pct=False, is_price=False, is_oz=False):
            """Format a delta value with +/- sign."""
            if val is None:
                return "    n/a"
            if is_pct:
                return f"{val:>+7.1f}%"
            if is_price:
                return f"${val:>+8.3f}"
            if is_oz:
                if abs(val) >= 1_000_000:
                    return f"{val / 1_000_000:>+8.1f}M"
                elif abs(val) >= 1_000:
                    return f"{val / 1_000:>+8.0f}K"
                return f"{val:>+8.0f}"
            if abs(val) >= 1_000_000:
                return f"{val / 1_000:>+8.0f}K"
            return f"{val:>+8,}"

        # Header
        periods = []
        period_labels = []
        for key, label in [("prev", "Prev Day"), ("7d", "7 Days"), ("30d", "30 Days")]:
            if trend_data.get(key):
                periods.append(key)
                d = trend_data[key].get("date", "")
                period_labels.append(f"{label} ({d})")

        if periods:
            # Compact header
            hdr = f"  {'Metric':<30}"
            for lbl in period_labels:
                hdr += f"  {lbl:>22}"
            lines.append(hdr)
            lines.append(f"  {'─' * 28:<30}" + "".join(f"  {'─' * 20:>22}" for _ in periods))

            # Silver Price
            row = f"  {'Silver Price':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('silver_price'), is_price=True):>22}"
            lines.append(row)

            # Total OI
            row = f"  {'Total Open Interest':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('all_oi')):>22}"
            lines.append(row)

            # Target OI
            row = f"  {'Target Months OI':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('target_oi')):>22}"
            lines.append(row)

            # Current month deliveries
            row = f"  {current_month_name + ' Deliveries (MTD)':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('current_month_delivered')):>22}"
            lines.append(row)

            # YTD deliveries
            row = f"  {'YTD Deliveries':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('ytd_delivered_contracts')):>22}"
            lines.append(row)

            # Warehouse combined
            row = f"  {'Warehouse Combined':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('warehouse_combined_oz'), is_oz=True):>22}"
            lines.append(row)

            # Warehouse registered
            row = f"  {'Warehouse Registered':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('warehouse_registered_oz'), is_oz=True):>22}"
            lines.append(row)

            # Warehouse eligible
            row = f"  {'Warehouse Eligible':<30}"
            for key in periods:
                d = trend_data[key]
                row += f"  {fmt_delta(d.get('warehouse_eligible_oz'), is_oz=True):>22}"
            lines.append(row)

            lines.append("")

            # Per-contract OI changes for each period
            for key, plabel in [("prev", "Previous Day"), ("7d", "7 Days"), ("30d", "30 Days")]:
                if key not in periods or not trend_data.get(key):
                    continue
                d = trend_data[key]
                changes = d.get("contract_oi_changes", {})
                if not changes:
                    continue
                lines.append(f"  OI Changes vs {plabel} ({d.get('date', '')})")
                for label, chg in sorted(changes.items(),
                                        key=lambda x: abs(x[1]), reverse=True):
                    oz_chg = chg * SILVER_CONTRACT_SIZE_OZ
                    lines.append(f"    {label:<12} {chg:>+8,} contracts  "
                                f"({oz_chg:>+12,} oz)")
                lines.append("")

            # Per-month delivery changes for each period
            for key, plabel in [("prev", "Previous Day"), ("7d", "7 Days"), ("30d", "30 Days")]:
                if key not in periods or not trend_data.get(key):
                    continue
                d = trend_data[key]
                del_changes = d.get("delivery_changes", {})
                if not del_changes:
                    continue
                lines.append(f"  Delivery Changes vs {plabel} ({d.get('date', '')})")
                for mon, chg in sorted(del_changes.items(),
                                      key=lambda x: MONTH_NAME_TO_NUM.get(x[0], 99)):
                    oz_chg = chg * SILVER_CONTRACT_SIZE_OZ
                    lines.append(f"    {mon:<12} {chg:>+8,} contracts  "
                                f"({oz_chg:>+12,} oz)")
                lines.append("")
        else:
            lines.append("  No prior data available yet — trend will appear on next run.")
            lines.append("")

    # --- Condensed summary table ---
    lines.append("=" * 78)
    lines.append("  CONDENSED SUMMARY")
    lines.append("=" * 78)
    lines.append("")
    lines.append(f"  {'Category':<38} {'Contracts':>12} {'Troy Oz':>14} {'Tonnes':>10}")
    lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")

    # 1) Delivered silver — per month breakdown
    ytd_contracts = 0
    if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
        totals = delivery_summary.get("totals", {})
        for mon, num in totals.items():
            if num > 0:
                oz = num * SILVER_CONTRACT_SIZE_OZ
                t = oz / TROY_OZ_PER_KG / 1000
                label = mon
                if mon.upper() == current_month_name:
                    label = f"{mon} (MTD)"
                # PREV months are prior-year carryover — show but exclude from YTD
                if mon.startswith("PREV"):
                    label = f"{mon} (prior yr)"
                else:
                    ytd_contracts += num
                lines.append(f"  {'  Delivered ' + label:<38} {num:>12,} {oz:>14,} {t:>10,.1f}")
        ytd_oz = ytd_contracts * SILVER_CONTRACT_SIZE_OZ
        ytd_tonnes = ytd_oz / TROY_OZ_PER_KG / 1000
        lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
        lines.append(f"  {'YTD Delivered':<38} {ytd_contracts:>12,} {ytd_oz:>14,} {ytd_tonnes:>10,.1f}")

    # 2) Open interest — next 6 months only
    six_months = months_in_range(now, num_months=5)  # current + 5 ahead = 6
    six_month_labels = set()
    for m, y in six_months:
        six_month_labels.add(f"{MONTH_NAMES[m]} {y}")
        six_month_labels.add(f"{MONTH_NAMES[m]} {str(y)[2:]}")

    lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
    six_oi_total = 0
    six_oz_total = 0
    six_t_total = 0
    for c in contracts:
        oi = c.get("open_interest", 0)
        label = c.get("month_label", "")
        if oi <= 0:
            continue
        in_six = any(label.upper().startswith(sl.split()[0]) and
                     label.upper().endswith(sl.split()[-1]) if len(sl.split()) > 1
                     else label.upper().startswith(sl)
                     for sl in six_month_labels)
        if not in_six:
            continue
        oz = oi * SILVER_CONTRACT_SIZE_OZ
        t = oz / TROY_OZ_PER_KG / 1000
        is_target = any(label.upper().startswith(tl.split()[0]) and
                       label.upper().endswith(tl.split()[-1]) if len(tl.split()) > 1
                       else label.upper().startswith(tl)
                       for tl in target_labels)
        marker = "*" if is_target else " "
        lines.append(f" {marker}{'  OI ' + label:<38} {oi:>12,} {oz:>14,} {t:>10,.1f}")
        six_oi_total += oi
        six_oz_total += oz
        six_t_total += t
    lines.append(f"  {'6-Month OI Total':<38} {six_oi_total:>12,} {six_oz_total:>14,} {six_t_total:>10,.1f}")
    lines.append(f" *{'Target Months OI':<38} {total_oi:>12,} {total_oz_standing:>14,} {total_tonnes:>10,.1f}")

    # 3) Warehouse stocks
    if warehouse_data:
        reg_oz = warehouse_data.get("total_registered_oz", 0)
        elig_oz = warehouse_data.get("total_eligible_oz", 0)
        comb_oz = warehouse_data.get("total_combined_oz", 0)
        reg_t = warehouse_data.get("total_registered_tonnes", 0)
        elig_t = warehouse_data.get("total_eligible_tonnes", 0)
        comb_t = warehouse_data.get("total_combined_tonnes", 0)
        lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
        lines.append(f"  {'Warehouse Registered':<38} {'':>12} {reg_oz:>14,.0f} {reg_t:>10,.1f}")
        lines.append(f"  {'Warehouse Eligible':<38} {'':>12} {elig_oz:>14,.0f} {elig_t:>10,.1f}")
        lines.append(f"  {'Warehouse Combined':<38} {'':>12} {comb_oz:>14,.0f} {comb_t:>10,.1f}")

        # Coverage ratio
        if comb_oz > 0 and total_oz_standing > 0:
            coverage = comb_oz / total_oz_standing * 100
            lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
            lines.append(f"  {'Warehouse / Target OI Coverage':<38} {'':>12} {coverage:>13.1f}% {'':>10}")

    if silver_price and silver_price > 0:
        lines.append("")
        lines.append(f"  Silver Price: ${silver_price:.2f}/oz")
        if warehouse_data:
            comb_val = warehouse_data.get("total_combined_oz", 0) * silver_price
            lines.append(f"  Warehouse Value:       ${comb_val:>18,.0f}")
        lines.append(f"  Target OI Value:       ${total_oz_standing * silver_price:>18,.0f}")
        if ytd_contracts:
            lines.append(f"  YTD Delivered Value:   ${ytd_contracts * SILVER_CONTRACT_SIZE_OZ * silver_price:>18,.0f}")
            # Also show total including prior year for reference
            if isinstance(delivery_summary, dict) and delivery_summary.get("source") == "pdf":
                all_del = sum(delivery_summary.get("totals", {}).values())
                if all_del > ytd_contracts:
                    lines.append(f"  Incl. Prior Year:      ${all_del * SILVER_CONTRACT_SIZE_OZ * silver_price:>18,.0f}")

    lines.append("")
    lines.append("=" * 78)
    lines.append("  Note: 1 COMEX silver contract = 5,000 troy oz")
    lines.append("  Data source: CME Group (www.cmegroup.com)")
    lines.append("=" * 78)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Get current silver spot price
# ---------------------------------------------------------------------------
def get_silver_price():
    """Fetch current silver spot price using yfinance."""
    try:
        import yfinance as yf
        si = yf.Ticker("SI=F")
        hist = si.history(period="5d")
        if not hist.empty:
            price = hist["Close"].iloc[-1]
            print(f"  Current silver futures price: ${price:.3f}/oz")
            return price
    except Exception as e:
        print(f"  Could not fetch silver price via yfinance: {e}")

    # Try from our own portfolio data
    try:
        si_file = os.path.join(SCRIPT_DIR, "portfolio", "SI=F.json")
        if os.path.exists(si_file):
            with open(si_file) as f:
                data = json.load(f)
            if "last_price" in data:
                return data["last_price"]
    except Exception:
        pass

    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║          COMEX Silver Data Report Generator                     ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    # Step 0: Get current silver price
    print("[0/5] Fetching current silver price...")
    silver_price = get_silver_price()
    if silver_price is not None:
        _save_raw_json({"silver_price_usd": silver_price,
                        "timestamp": datetime.now().isoformat()},
                       "silver_price_raw.json")
    print()

    # Step 1: Download delivery report (YTD)
    print("[1/6] Downloading COMEX YTD delivery report...")
    xls_path = download_delivery_report()
    print()

    # Step 1b: Download warehouse stocks
    print("[1b/6] Downloading COMEX silver warehouse stocks...")
    stocks_path = download_warehouse_stocks()
    warehouse_data = parse_warehouse_stocks(stocks_path)
    print()

    # Step 2: Extract silver delivery data
    print("[2/6] Extracting silver delivery data...")
    delivery_data = extract_silver_deliveries(xls_path)
    print()

    # Step 3: Fetch daily delivery report (today's deliveries + MTD)
    print("[3/6] Fetching daily delivery report...")
    daily_deliveries = fetch_daily_deliveries()
    print()

    # Step 4: Download contract data (settlements include volume & OI)
    print("[4/6] Downloading silver futures contract data...")
    settlements = fetch_settlements_data()
    print()

    # Step 5: Evaluate contract data
    print("[5/6] Evaluating contracts and calculating deliveries...")
    contracts, delivery_summary = evaluate_contracts(settlements, delivery_data)
    print()

    # Step 6: Save trend snapshot & compute trend
    print("[6/7] Saving trend snapshot...")
    _save_trend_snapshot(silver_price, contracts, delivery_summary,
                         warehouse_data, daily_deliveries)
    history = _load_trend_history()
    today_key = datetime.now().strftime("%Y-%m-%d")
    trend_data = compute_trend(history, today_key)
    if trend_data and trend_data.get("prev"):
        print(f"  Trend data: {len(history)} days of history, "
              f"prev day = {trend_data['prev'].get('date', '?')}")
    else:
        print(f"  First run — trend data will appear on next run ({len(history)} snapshot(s) saved)")
    print()

    # ── Derive canonical CME trade date (synchronisation date) ──
    trade_date_tag = _extract_trade_date(settlements, delivery_data, warehouse_data)
    if trade_date_tag:
        print(f"  CME trade date (sync date): {trade_date_tag}")
    else:
        # Fallback to scrape date if no CME date available
        trade_date_tag = datetime.now().strftime("%Y%m%d")
        print(f"  WARNING: Could not determine CME trade date, "
              f"using scrape date: {trade_date_tag}")

    # Step 7: Generate summary
    print("[7/7] Generating summary report...")
    summary = generate_summary(contracts, delivery_summary, silver_price, warehouse_data,
                               daily_deliveries, trend_data)
    print()
    print(summary)

    # Save report to file — use trade date + scrape timestamp
    reports_dir = os.path.join(CACHE_DIR, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    report_path = os.path.join(
        reports_dir,
        f"silver_report_{trade_date_tag}_{datetime.now().strftime('%H%M%S')}.txt",
    )
    with open(report_path, "w") as f:
        f.write(summary)
    print(f"\n  Report saved to: {report_path}")

    # Also save as JSON for programmatic use
    json_data = {
        "trade_date": trade_date_tag,
        "generated": datetime.now().isoformat(),
        "silver_price_usd": silver_price,
        "contracts": contracts,
        "delivery_summary": delivery_summary,
        "warehouse_stocks": warehouse_data,
    }
    json_path = os.path.join(CACHE_DIR, "silver_contracts_latest.json")
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2, default=str)
    print(f"  JSON data saved to: {json_path}")

    # Dated archive — keyed to CME trade date (one per business day)
    dated_json = os.path.join(CACHE_DIR,
                              f"silver_contracts_{trade_date_tag}.json")
    with open(dated_json, "w") as f:
        json.dump(json_data, f, indent=2, default=str)
    print(f"  Dated JSON saved to: {dated_json}")

    # Save all raw input data together for archival
    raw_inputs_path = os.path.join(CACHE_DIR, "raw_inputs_latest.json")
    raw_inputs = {
        "trade_date": trade_date_tag,
        "generated": datetime.now().isoformat(),
        "input_files": {
            "delivery_report": xls_path,
            "warehouse_stocks": stocks_path,
        },
        "raw_api_data": {
            "silver_price_usd": silver_price,
            "settlements": settlements,
        },
        "parsed_data": {
            "delivery_data": delivery_data,
            "warehouse_data": warehouse_data,
        },
    }
    with open(raw_inputs_path, "w") as f:
        json.dump(raw_inputs, f, indent=2, default=str)
    print(f"  Raw input data saved to: {raw_inputs_path}")


if __name__ == "__main__":
    main()
