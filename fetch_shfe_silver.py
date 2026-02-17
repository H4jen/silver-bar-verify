#!/usr/bin/env python3
"""
SHFE Silver Data Fetcher
=========================
Fetches silver (AG) inventory and futures data from the Shanghai Futures Exchange
(SHFE / 上海期货交易所), analogous to what comex_silver_report2.py does for COMEX.

Data sources (all official SHFE JSON APIs):
  1) Daily trading data — silver futures quotes, volume, open interest
     /data/tradedata/future/dailydata/kx{YYYYMMDD}.dat
  2) Warehouse position data — per-slot cargo & warrant weights per depository
     /data/tradedata/future/dailydata/stock_ag_{YYYYMMDD}.dat
  3) Available date list for warehouse data
     /data/tradedata/future/dailydata/stock_data_list.dat

Key SHFE silver contract specs:
  - Symbol: AG
  - Contract size: 15 kg
  - Quotation: RMB yuan per kg
  - Exchange: SHFE (Shanghai Futures Exchange)

Terminology mapping (SHFE → COMEX):
  - CRGWEIGHT (cargo weight) ≈ total silver in vault (like COMEX combined total)
  - WRTWEIGHT (warrant weight) ≈ registered / deliverable silver (like COMEX registered)
  - Difference (cargo - warrant) ≈ non-warrant silver (like COMEX eligible)

Usage:
    python fetch_shfe_silver.py                    # fetch latest, print report
    python fetch_shfe_silver.py --date 20260213    # fetch specific date
    python fetch_shfe_silver.py --history 5        # fetch last N days
    python fetch_shfe_silver.py --compare          # compare with COMEX data
    python fetch_shfe_silver.py --json             # output JSON only (no report)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")  # shared cache dir

SHFE_BASE_URL = "https://www.shfe.com.cn"

# SHFE API endpoints (discovered from /eng/images/api.js)
DAILY_TRADING_URL = SHFE_BASE_URL + "/data/tradedata/future/dailydata/kx{date}.dat"
WAREHOUSE_STOCK_URL = SHFE_BASE_URL + "/data/tradedata/future/dailydata/stock_ag_{date}.dat"
STOCK_DATE_LIST_URL = SHFE_BASE_URL + "/data/tradedata/future/dailydata/stock_data_list.dat"

# Contract specs
SHFE_AG_CONTRACT_SIZE_KG = 15       # 15 kg per contract
TROY_OZ_PER_KG = 32.1507           # 1 kg = 32.1507 troy oz
GRAMS_PER_TROY_OZ = 31.1035

MONTH_NAMES = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}

CHINA_VAT_RATE = 0.13  # 13% VAT on precious metals


def fetch_cny_usd() -> Optional[float]:
    """Fetch live CNY/USD rate via yfinance (USDCNY=X ticker).

    Returns the number of CNY per 1 USD (e.g. 6.90), or None on failure.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker("USDCNY=X")
        hist = ticker.history(period="5d")
        if hist.empty:
            return None
        rate = float(hist["Close"].iloc[-1])
        if rate > 0:
            return round(rate, 4)
    except Exception:
        pass
    return None

# Request headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.shfe.com.cn/eng/reports/StatisticalData/DailyData/",
}


# ---------------------------------------------------------------------------
# API fetchers
# ---------------------------------------------------------------------------
def fetch_available_dates() -> list[int]:
    """Fetch the list of dates with warehouse position data available."""
    url = STOCK_DATE_LIST_URL
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        dates = json.loads(data["dataList"])
        return sorted(dates)
    except Exception as e:
        print(f"  ERROR fetching date list: {e}", file=sys.stderr)
        return []


def fetch_daily_trading(date_str: str) -> Optional[dict]:
    """Fetch daily trading data (kx report) for a given date.

    Returns the full JSON response or None on failure.
    """
    url = DAILY_TRADING_URL.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ERROR fetching trading data for {date_str}: {e}", file=sys.stderr)
        return None


def fetch_warehouse_stock(date_str: str) -> Optional[dict]:
    """Fetch per-slot silver warehouse position data for a given date.

    Returns the full JSON response or None on failure.
    """
    url = WAREHOUSE_STOCK_URL.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  ERROR fetching warehouse data for {date_str}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Data extraction & aggregation
# ---------------------------------------------------------------------------
def _rmb_kg_to_usd_oz(rmb_kg: float, cny_usd: Optional[float]) -> Optional[float]:
    """Convert RMB/kg → USD/oz (ex-VAT).

    Formula: (rmb_kg / TROY_OZ_PER_KG / cny_usd) / (1 + CHINA_VAT_RATE)
    Returns None if either input is missing.
    """
    if not rmb_kg or not cny_usd:
        return None
    rmb_per_oz = rmb_kg / TROY_OZ_PER_KG
    return round(rmb_per_oz / cny_usd / (1 + CHINA_VAT_RATE), 4)


def extract_silver_contracts(
    kx_data: dict,
    cny_usd: Optional[float] = None,
) -> list[dict]:
    """Extract silver (AG) futures contracts from daily trading data.

    Adds USD/oz converted prices (ex-VAT) alongside native RMB/kg.
    OI is also expressed in kg and troy oz.
    """
    contracts = []
    for c in kx_data.get("o_curinstrument", []):
        pid = c.get("PRODUCTID", "").strip()
        if pid != "ag_f":
            continue
        month = c.get("DELIVERYMONTH", "")
        # Skip the subtotal row (ag小计)
        if not month or not month.isdigit():
            continue

        settle = c.get("SETTLEMENTPRICE")
        close = c.get("CLOSEPRICE")
        high = c.get("HIGHESTPRICE")
        low = c.get("LOWESTPRICE")
        oi = c.get("OPENINTEREST", 0) or 0
        oi_kg = oi * SHFE_AG_CONTRACT_SIZE_KG
        oi_oz = round(oi_kg * TROY_OZ_PER_KG)

        # Readable month label: "2604" -> "APR 26"
        month_label = ""
        if len(month) == 4 and month.isdigit():
            mm = int(month[2:])
            yy = month[:2]
            month_label = f"{MONTH_NAMES.get(mm, '???')} {yy}"

        contracts.append({
            "delivery_month": month,
            "month_label": month_label,
            # Native RMB/kg
            "settlement_price_rmb_kg": settle,
            "close_price_rmb_kg": close,
            "open_price_rmb_kg": c.get("OPENPRICE"),
            "high_rmb_kg": high,
            "low_rmb_kg": low,
            "prev_settlement_rmb_kg": c.get("PRESETTLEMENTPRICE"),
            "change_vs_prev_settle": c.get("ZD2_CHG"),
            # USD/oz (ex-VAT, indicative)
            "settlement_price_usd_oz": _rmb_kg_to_usd_oz(settle, cny_usd),
            "close_price_usd_oz": _rmb_kg_to_usd_oz(close, cny_usd),
            "high_usd_oz": _rmb_kg_to_usd_oz(high, cny_usd),
            "low_usd_oz": _rmb_kg_to_usd_oz(low, cny_usd),
            # OI & volume
            "open_interest": oi,
            "open_interest_kg": oi_kg,
            "open_interest_oz": oi_oz,
            "volume": c.get("VOLUME", 0),
            "turnover_10k_rmb": c.get("TURNOVER", 0),
        })
    return sorted(contracts, key=lambda x: x["delivery_month"])


def extract_trade_date(kx_data: dict) -> Optional[str]:
    """Extract the official trade date from the daily data.

    Three date sources exist in the kx JSON (all should agree):
      1. report_date      — e.g. '20260213' (cleanest, YYYYMMDD)
      2. o_year/o_month/o_day — '2026'/'02'/'13'
      3. SETTDATE per warehouse entry (checked elsewhere)

    We prefer ``report_date`` and cross-check against the components.
    """
    # Primary: report_date (already YYYYMMDD)
    rd = kx_data.get("report_date", "").strip()
    if rd and len(rd) == 8 and rd.isdigit():
        # Cross-check against component fields if available
        y = kx_data.get("o_year", "")
        m = kx_data.get("o_month", "")
        d = kx_data.get("o_day", "")
        if y and m and d:
            component_date = f"{y}{m.zfill(2)}{d.zfill(2)}"
            if component_date != rd:
                print(f"  WARNING: report_date={rd} != o_year/month/day={component_date}",
                      file=sys.stderr)
        return rd

    # Fallback: component fields
    y = kx_data.get("o_year", "")
    m = kx_data.get("o_month", "")
    d = kx_data.get("o_day", "")
    if y and m and d:
        return f"{y}{m.zfill(2)}{d.zfill(2)}"
    return None


def aggregate_warehouse(stock_data: dict) -> dict:
    """Aggregate per-slot warehouse data into per-depository and totals.

    SHFE fields:
      WAREHOUSENAME — depository company name
      STGPOSNAME    — specific warehouse/location name
      CRGWEIGHT     — cargo weight in kg (total silver at position)
      WRTWEIGHT     — warrant weight in kg (registered for delivery)
      EXPIREDWEIGHT — expired warrant weight
      EXTENDWEIGHT  — extended warrant weight

    Returns dict with:
      depositories: list of per-depository aggregates
      totals: aggregate totals
      raw_positions: number of individual cargo slots
    """
    entries = stock_data.get("StockOutData", [])
    if not entries:
        return {"depositories": [], "totals": {}, "raw_positions": 0}

    # Aggregate by depository (WAREHOUSENAME) and sub-warehouse (STGPOSNAME)
    dep_data = defaultdict(lambda: {
        "cargo_kg": 0, "warrant_kg": 0, "expired_kg": 0, "extended_kg": 0,
        "positions": 0, "sub_warehouses": set(),
    })

    for e in entries:
        name = e.get("WAREHOUSENAME", "").strip()
        sub = e.get("STGPOSNAME", "").strip()
        crg = int(e.get("CRGWEIGHT", 0))
        wrt = int(e.get("WRTWEIGHT", 0))
        exp = int(e.get("EXPIREDWEIGHT", 0))
        ext = int(e.get("EXTENDWEIGHT", 0))

        d = dep_data[name]
        d["cargo_kg"] += crg
        d["warrant_kg"] += wrt
        d["expired_kg"] += exp
        d["extended_kg"] += ext
        d["positions"] += 1
        d["sub_warehouses"].add(sub)

    depositories = []
    for name in sorted(dep_data.keys()):
        d = dep_data[name]
        non_warrant = d["cargo_kg"] - d["warrant_kg"]
        depositories.append({
            "name": name,
            "cargo_kg": d["cargo_kg"],
            "warrant_kg": d["warrant_kg"],
            "non_warrant_kg": non_warrant,
            "expired_kg": d["expired_kg"],
            "extended_kg": d["extended_kg"],
            "positions": d["positions"],
            "sub_warehouses": len(d["sub_warehouses"]),
            # Convert to troy oz for comparison with COMEX
            "cargo_oz": round(d["cargo_kg"] * TROY_OZ_PER_KG),
            "warrant_oz": round(d["warrant_kg"] * TROY_OZ_PER_KG),
        })

    tot_cargo = sum(d["cargo_kg"] for d in depositories)
    tot_warrant = sum(d["warrant_kg"] for d in depositories)
    tot_expired = sum(d["expired_kg"] for d in depositories)
    tot_extended = sum(d["extended_kg"] for d in depositories)

    totals = {
        "cargo_kg": tot_cargo,
        "cargo_tonnes": tot_cargo / 1000,
        "cargo_oz": round(tot_cargo * TROY_OZ_PER_KG),
        "warrant_kg": tot_warrant,
        "warrant_tonnes": tot_warrant / 1000,
        "warrant_oz": round(tot_warrant * TROY_OZ_PER_KG),
        "non_warrant_kg": tot_cargo - tot_warrant,
        "non_warrant_tonnes": (tot_cargo - tot_warrant) / 1000,
        "non_warrant_oz": round((tot_cargo - tot_warrant) * TROY_OZ_PER_KG),
        "expired_kg": tot_expired,
        "extended_kg": tot_extended,
    }

    return {
        "depositories": depositories,
        "totals": totals,
        "raw_positions": len(entries),
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def generate_report(
    trade_date: str,
    contracts: list[dict],
    warehouse: dict,
    comex_data: Optional[dict] = None,
    cny_usd: Optional[float] = None,
) -> str:
    """Generate a human-readable text report (COMEX-aligned format)."""
    lines = []
    now = datetime.now()
    td_fmt = f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}"

    # Derive front-month silver price (highest OI, ex-VAT USD/oz)
    silver_price_usd = None
    front_month_settle_rmb = None
    front_contract = None
    if contracts:
        front_contract = max(contracts, key=lambda c: c.get("open_interest", 0) or 0)
        silver_price_usd = front_contract.get("settlement_price_usd_oz")
        front_month_settle_rmb = front_contract.get("settlement_price_rmb_kg")

    # ══════════════════════════════════════════════════════════════════════
    #  HEADER
    # ══════════════════════════════════════════════════════════════════════
    lines.append("=" * 78)
    lines.append("  SHFE SILVER FUTURES (AG) — DATA REPORT")
    lines.append(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 78)
    lines.append("")

    # --- Current price ---
    if silver_price_usd:
        lines.append(f"  Current Silver Price: ${silver_price_usd:.3f} / troy oz  (ex-VAT)")
        if front_month_settle_rmb:
            lines.append(f"  Front Month Settle:  {front_month_settle_rmb:,.0f} RMB/kg"
                         f"  (CNY/USD: {cny_usd:.4f})" if cny_usd else "")
    elif front_month_settle_rmb:
        lines.append(f"  Front Month Settle:  {front_month_settle_rmb:,.0f} RMB/kg")
    lines.append("")

    # ──────────────────────────────────────────────────────────────────────
    #  SILVER FUTURES CONTRACTS
    # ──────────────────────────────────────────────────────────────────────
    lines.append("-" * 78)
    lines.append("  SILVER FUTURES CONTRACTS (AG)")
    lines.append("-" * 78)
    lines.append("")
    lines.append(f"  {'Contract':<10} {'Settle':>9} {'USD/oz':>9} {'OI':>8} "
                 f"{'Vol':>8} {'Chg':>7} {'Standing oz':>14} {'Tonnes':>9}")
    lines.append(f"  {'─' * 8:<10} {'─' * 7:>9} {'─' * 7:>9} {'─' * 6:>8} "
                 f"{'─' * 6:>8} {'─' * 5:>7} {'─' * 12:>14} {'─' * 7:>9}")

    total_oi = 0
    total_vol = 0
    total_oz = 0
    total_tonnes = 0

    for c in contracts:
        settle_rmb = c.get("settlement_price_rmb_kg")
        settle_usd = c.get("settlement_price_usd_oz")
        oi = c.get("open_interest", 0)
        vol = c.get("volume", 0)
        chg = c.get("change_vs_prev_settle")
        label = c.get("month_label") or f"ag{c['delivery_month']}"
        oi_oz = c.get("open_interest_oz", 0)
        oi_kg = c.get("open_interest_kg", 0)
        oi_tonnes = oi_kg / 1000

        total_oi += oi
        total_vol += vol
        total_oz += oi_oz
        total_tonnes += oi_tonnes

        settle_s = f"{settle_rmb:,.0f}" if settle_rmb else "-"
        usd_s = f"{settle_usd:.2f}" if settle_usd else "-"
        chg_s = f"{chg:+,.0f}" if chg else "-"

        lines.append(f"  {label:<10} {settle_s:>9} {usd_s:>9} {oi:>8,} "
                     f"{vol:>8,} {chg_s:>7} {oi_oz:>14,} {oi_tonnes:>9,.1f}")

    lines.append("")

    # ──────────────────────────────────────────────────────────────────────
    #  ALL ACTIVE CONTRACTS OVERVIEW
    # ──────────────────────────────────────────────────────────────────────
    lines.append("-" * 78)
    lines.append("  ALL ACTIVE CONTRACTS OVERVIEW")
    lines.append("-" * 78)
    lines.append("")

    lines.append(f"  Total Open Interest (all months):      {total_oi:>12,} contracts")
    lines.append(f"  Total Silver Represented:              {total_oz:>12,} troy oz")
    lines.append(f"                                         {total_tonnes:>12,.1f} metric tonnes")
    lines.append("")

    if silver_price_usd and silver_price_usd > 0:
        total_value = total_oz * silver_price_usd
        lines.append(f"  Total Notional Value:                  ${total_value:>14,.0f}")
        lines.append("")

    # ──────────────────────────────────────────────────────────────────────
    #  WAREHOUSE STOCKS (Registered & Eligible)
    # ──────────────────────────────────────────────────────────────────────
    totals = warehouse.get("totals", {})
    deps = warehouse.get("depositories", [])

    lines.append("-" * 78)
    lines.append("  SHFE WAREHOUSE SILVER STOCKS (Registered & Eligible)")
    lines.append(f"  Trade Date: {td_fmt}")
    lines.append("-" * 78)
    lines.append("")

    reg_oz = totals.get("warrant_oz", 0)
    elig_oz = totals.get("non_warrant_oz", 0)
    comb_oz = totals.get("cargo_oz", 0)
    reg_t = totals.get("warrant_tonnes", 0)
    elig_t = totals.get("non_warrant_tonnes", 0)
    comb_t = totals.get("cargo_tonnes", 0)

    lines.append(f"  {'Category':<22} {'Troy Ounces':>18} {'Metric Tonnes':>16}")
    lines.append(f"  {'─' * 20:<22} {'─' * 16:>18} {'─' * 14:>16}")
    lines.append(f"  {'Registered':<22} {reg_oz:>18,} {reg_t:>16,.1f}")
    lines.append(f"  {'Eligible':<22} {elig_oz:>18,} {elig_t:>16,.1f}")
    lines.append(f"  {'Combined Total':<22} {comb_oz:>18,} {comb_t:>16,.1f}")
    lines.append("")

    if silver_price_usd and silver_price_usd > 0:
        reg_value = reg_oz * silver_price_usd
        elig_value = elig_oz * silver_price_usd
        comb_value = comb_oz * silver_price_usd
        lines.append(f"  Registered Value:    ${reg_value:>18,.0f}")
        lines.append(f"  Eligible Value:      ${elig_value:>18,.0f}")
        lines.append(f"  Combined Value:      ${comb_value:>18,.0f}")
        lines.append("")

    # Coverage ratio
    if comb_oz > 0 and total_oz > 0:
        coverage = comb_oz / total_oz * 100
        lines.append(f"  Warehouse Coverage Ratio:  {coverage:>8.1f}%")
        lines.append(f"    (warehouse silver / total open interest)")
        if coverage < 100:
            lines.append(f"    ⚠  Warehouse stocks BELOW total open interest!")
        lines.append("")

    # Per-vault breakdown
    if deps:
        lines.append(f"  {'Depository':<42} {'Registered':>14} {'Eligible':>14}")
        lines.append(f"  {'─' * 40:<42} {'─' * 12:>14} {'─' * 12:>14}")

        for d in deps:
            name = d["name"][:40]
            reg = d.get("warrant_oz", 0)
            elig = d.get("cargo_oz", 0) - d.get("warrant_oz", 0)
            if reg > 0 or elig > 0:
                lines.append(f"  {name:<42} {reg:>14,} {elig:>14,}")
        lines.append("")

    # ──────────────────────────────────────────────────────────────────────
    #  KEY OBSERVATIONS & ANALYSIS
    # ──────────────────────────────────────────────────────────────────────
    lines.append("-" * 78)
    lines.append("  KEY OBSERVATIONS & ANALYSIS")
    lines.append("-" * 78)
    lines.append("")

    if front_contract:
        fl = front_contract.get("month_label") or f"ag{front_contract['delivery_month']}"
        foi = front_contract.get("open_interest", 0)
        foz = front_contract.get("open_interest_oz", 0)
        lines.append(f"  • Front month: {fl} with "
                     f"{foi:,} contracts ({foz:,} oz)")

    # Highlight contracts with large OI
    for c in sorted(contracts, key=lambda x: x.get("open_interest", 0), reverse=True):
        oi = c.get("open_interest", 0)
        label = c.get("month_label") or f"ag{c['delivery_month']}"
        oi_oz_c = c.get("open_interest_oz", 0)
        if oi > 5000 and c is not front_contract:
            lines.append(f"  • {label}: {oi:,} contracts open interest "
                         f"({oi_oz_c:,} oz)")

    # Price note
    if silver_price_usd and front_month_settle_rmb and cny_usd:
        rmb_per_oz = front_month_settle_rmb / TROY_OZ_PER_KG
        usd_incl_vat = rmb_per_oz / cny_usd
        lines.append(f"  • SHFE price incl. 13% VAT: ${usd_incl_vat:.2f}/oz"
                     f"  →  ex-VAT: ${silver_price_usd:.2f}/oz")

    lines.append("")

    # ──────────────────────────────────────────────────────────────────────
    #  SHFE vs COMEX COMPARISON
    # ──────────────────────────────────────────────────────────────────────
    if comex_data:
        lines.append("-" * 78)
        lines.append("  SHFE vs COMEX COMPARISON")
        lines.append("-" * 78)
        lines.append("")

        ws = comex_data.get("warehouse_stocks", {})
        comex_reg_t = ws.get("total_registered_tonnes", 0)
        comex_elig_t = ws.get("total_eligible_tonnes", 0)
        comex_comb_t = ws.get("total_combined_tonnes", 0)

        lines.append(f"  {'Category':<30s} {'SHFE':>12s} {'COMEX':>12s} {'Ratio':>8s}")
        lines.append(f"  {'─'*30} {'─'*12} {'─'*12} {'─'*8}")

        ratio_total = comb_t / comex_comb_t if comex_comb_t else 0
        lines.append(f"  {'Total Vault (tonnes)':<30s} {comb_t:>12,.1f}"
                     f" {comex_comb_t:>12,.1f} {ratio_total:>7.1%}")

        ratio_reg = reg_t / comex_reg_t if comex_reg_t else 0
        lines.append(f"  {'Registered (tonnes)':<30s} {reg_t:>12,.1f}"
                     f" {comex_reg_t:>12,.1f} {ratio_reg:>7.1%}")

        ratio_elig = elig_t / comex_elig_t if comex_elig_t else 0
        lines.append(f"  {'Eligible (tonnes)':<30s} {elig_t:>12,.1f}"
                     f" {comex_elig_t:>12,.1f} {ratio_elig:>7.1%}")

        # Price comparison
        comex_price = comex_data.get("silver_price_usd")
        if comex_price and silver_price_usd:
            premium = ((silver_price_usd / comex_price) - 1) * 100
            lines.append(f"\n  Price (USD/oz): SHFE ex-VAT ${silver_price_usd:.2f}  vs  "
                         f"COMEX ${comex_price:.2f}  "
                         f"(Shanghai premium: {premium:+.1f}%)")

        # OI comparison
        comex_contracts_list = comex_data.get("contracts", [])
        comex_total_oi = sum(c.get("open_interest", 0) for c in comex_contracts_list
                            if isinstance(c, dict))
        comex_oi_oz = comex_total_oi * 5000

        if comex_oi_oz > 0:
            oi_ratio = total_oz / comex_oi_oz
            lines.append(f"  OI (troy oz):  SHFE {total_oz:>14,}  vs  "
                         f"COMEX {comex_oi_oz:>14,}  "
                         f"(ratio: {oi_ratio:.1%})")

        lines.append("")

    # ══════════════════════════════════════════════════════════════════════
    #  CONDENSED SUMMARY
    # ══════════════════════════════════════════════════════════════════════
    lines.append("=" * 78)
    lines.append("  CONDENSED SUMMARY")
    lines.append("=" * 78)
    lines.append("")
    lines.append(f"  {'Category':<38} {'Contracts':>12} {'Troy Oz':>14} {'Tonnes':>10}")
    lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")

    # Per-month OI
    for c in contracts:
        oi = c.get("open_interest", 0)
        if oi <= 0:
            continue
        label = c.get("month_label") or f"ag{c['delivery_month']}"
        c_oz = c.get("open_interest_oz", 0)
        c_kg = c.get("open_interest_kg", 0)
        c_t = c_kg / 1000
        lines.append(f"  {'  OI ' + label:<38} {oi:>12,} {c_oz:>14,} {c_t:>10,.1f}")

    lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
    lines.append(f"  {'Total OI':<38} {total_oi:>12,} {total_oz:>14,} {total_tonnes:>10,.1f}")

    # Warehouse stocks
    lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
    lines.append(f"  {'Warehouse Registered':<38} {'':>12} {reg_oz:>14,} {reg_t:>10,.1f}")
    lines.append(f"  {'Warehouse Eligible':<38} {'':>12} {elig_oz:>14,} {elig_t:>10,.1f}")
    lines.append(f"  {'Warehouse Combined':<38} {'':>12} {comb_oz:>14,} {comb_t:>10,.1f}")

    if comb_oz > 0 and total_oz > 0:
        coverage = comb_oz / total_oz * 100
        lines.append(f"  {'─' * 36:<38} {'─' * 10:>12} {'─' * 12:>14} {'─' * 8:>10}")
        lines.append(f"  {'Warehouse / Total OI Coverage':<38} {'':>12} {coverage:>13.1f}% {'':>10}")

    if silver_price_usd and silver_price_usd > 0:
        lines.append("")
        lines.append(f"  Silver Price: ${silver_price_usd:.2f}/oz (ex-VAT)")
        lines.append(f"  Warehouse Value:       ${comb_oz * silver_price_usd:>18,.0f}")
        lines.append(f"  Total OI Value:        ${total_oz * silver_price_usd:>18,.0f}")

    lines.append("")
    lines.append("=" * 78)
    lines.append("  Note: 1 SHFE silver (AG) contract = 15 kg")
    lines.append(f"  Registered = warrant silver | Eligible = non-warrant silver")
    lines.append(f"  Prices: RMB/kg incl. 13% VAT, USD/oz ex-VAT  |  CNY/USD: "
                 f"{cny_usd:.4f}" if cny_usd else "  Prices: RMB/kg incl. 13% VAT")
    lines.append("  Data source: SHFE (www.shfe.com.cn)")
    lines.append("=" * 78)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------
def save_shfe_data(
    trade_date: str,
    contracts: list[dict],
    warehouse: dict,
    kx_meta: dict,
    cny_usd: Optional[float] = None,
) -> str:
    """Save SHFE silver data as dated JSON file. Returns the file path."""
    os.makedirs(CACHE_DIR, exist_ok=True)

    # Derive front-month (highest OI) settlement in USD/oz for top-level field
    silver_price_usd = None
    if contracts:
        front = max(contracts, key=lambda c: c.get("open_interest", 0) or 0)
        silver_price_usd = front.get("settlement_price_usd_oz")

    # Aggregate OI in oz
    total_oi_contracts = sum(c.get("open_interest", 0) or 0 for c in contracts)
    total_oi_kg = total_oi_contracts * SHFE_AG_CONTRACT_SIZE_KG
    total_oi_oz = round(total_oi_kg * TROY_OZ_PER_KG)

    json_data = {
        "exchange": "SHFE",
        "product": "silver_ag",
        "trade_date": trade_date,
        "generated": datetime.now().isoformat(),
        "cny_usd": cny_usd,
        "silver_price_usd": silver_price_usd,
        "total_oi_contracts": total_oi_contracts,
        "total_oi_kg": total_oi_kg,
        "total_oi_oz": total_oi_oz,
        "contracts": contracts,
        "warehouse": warehouse,
        "meta": kx_meta,
    }
    path = os.path.join(CACHE_DIR, f"shfe_silver_{trade_date}.json")
    with open(path, "w") as f:
        json.dump(json_data, f, indent=2, default=str, ensure_ascii=False)
    return path


def load_comex_data(trade_date: str = None) -> Optional[dict]:
    """Load the most recent COMEX silver data for comparison."""
    import glob
    pattern = os.path.join(CACHE_DIR, "silver_contracts_*.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return None

    if trade_date:
        # Try exact match first
        exact = os.path.join(CACHE_DIR, f"silver_contracts_{trade_date}.json")
        if os.path.exists(exact):
            with open(exact) as f:
                return json.load(f)

    # Fall back to latest
    with open(files[-1]) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def find_latest_trade_date() -> Optional[str]:
    """Find the most recent trading date from SHFE."""
    dates = fetch_available_dates()
    if dates:
        return str(dates[-1])
    return None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch SHFE silver (AG) inventory and futures data",
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Specific trade date (YYYYMMDD). Default: latest available.",
    )
    parser.add_argument(
        "--history", type=int, default=0, metavar="N",
        help="Fetch last N days of data (saves JSON for each).",
    )
    parser.add_argument(
        "--compare", action="store_true",
        help="Compare SHFE data with COMEX data.",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output JSON instead of text report.",
    )
    parser.add_argument(
        "--cny-usd", type=float, default=None,
        help="CNY/USD exchange rate override. Default: live rate via yfinance.",
    )
    args = parser.parse_args()

    # In --json mode, send progress to stderr so stdout is clean JSON
    _out = sys.stderr if args.json else sys.stdout
    def log(*a, **kw):
        print(*a, **kw, file=_out)

    log()
    log("╔══════════════════════════════════════════════════════════════════╗")
    log("║          SHFE Silver (AG) Data Fetcher                          ║")
    log("╚══════════════════════════════════════════════════════════════════╝")
    log()

    # Resolve CNY/USD rate: CLI override or live fetch
    if args.cny_usd is None:
        log("  Fetching live CNY/USD rate...")
        args.cny_usd = fetch_cny_usd()
        if args.cny_usd:
            log(f"  CNY/USD = {args.cny_usd:.4f} (via yfinance)")
        else:
            log("  WARNING: Could not fetch CNY/USD rate. USD conversions will be skipped.")
        log()

    # Step 1: Determine date(s) to fetch
    log("[1/4] Checking available SHFE trading dates...")
    available_dates = fetch_available_dates()
    if not available_dates:
        log("  ERROR: Could not fetch available dates from SHFE.")
        return 1
    log(f"  {len(available_dates)} dates available "
        f"({available_dates[0]} → {available_dates[-1]})")

    if args.date:
        target_dates = [args.date]
        if int(args.date) not in available_dates:
            log(f"  WARNING: {args.date} may not have warehouse data available.")
    elif args.history > 0:
        target_dates = [str(d) for d in available_dates[-args.history:]]
    else:
        target_dates = [str(available_dates[-1])]

    log(f"  Will fetch: {', '.join(target_dates)}")
    log()

    # Process each date
    all_results = []

    for date_str in target_dates:
        log(f"[2/4] Fetching trading data for {date_str}...")
        kx_data = fetch_daily_trading(date_str)
        if not kx_data:
            log(f"  No trading data for {date_str}, skipping.")
            continue

        trade_date = extract_trade_date(kx_data) or date_str
        contracts = extract_silver_contracts(kx_data, cny_usd=args.cny_usd)
        log(f"  Found {len(contracts)} silver contracts")

        kx_meta = {
            "year_num": kx_data.get("o_year_num"),
            "total_num": kx_data.get("o_total_num"),
            "trade_day": kx_data.get("o_trade_day"),
            "weekday": kx_data.get("o_weekday"),
        }

        log(f"[3/4] Fetching warehouse inventory for {date_str}...")
        stock_data = fetch_warehouse_stock(date_str)
        if stock_data:
            warehouse = aggregate_warehouse(stock_data)
            log(f"  {warehouse['raw_positions']} cargo positions across "
                f"{len(warehouse['depositories'])} depositories")
            totals = warehouse["totals"]
            log(f"  Total cargo:   {totals['cargo_kg']:>10,} kg "
                f"({totals['cargo_tonnes']:,.1f} tonnes)")
            log(f"  Total warrant: {totals['warrant_kg']:>10,} kg "
                f"({totals['warrant_tonnes']:,.1f} tonnes)")
        else:
            log(f"  No warehouse data for {date_str}")
            warehouse = {"depositories": [], "totals": {}, "raw_positions": 0}

        # Save JSON
        log(f"[4/4] Saving data...")
        json_path = save_shfe_data(trade_date, contracts, warehouse, kx_meta,
                                   cny_usd=args.cny_usd)
        log(f"  Saved: {json_path}")

        all_results.append({
            "trade_date": trade_date,
            "contracts": contracts,
            "warehouse": warehouse,
            "kx_meta": kx_meta,
        })
        log()

    if not all_results:
        log("  No data fetched.")
        return 1

    # Generate report for the last (most recent) date
    latest = all_results[-1]
    comex_data = None
    if args.compare:
        log("Loading COMEX data for comparison...")
        comex_data = load_comex_data(latest["trade_date"])
        if comex_data:
            log(f"  COMEX data loaded: trade date {comex_data.get('trade_date', '?')}")
        else:
            log("  No COMEX data found for comparison.")
        log()

    if args.json:
        # JSON output mode — only JSON goes to stdout
        output = {
            "exchange": "SHFE",
            "product": "silver_ag",
            "trade_date": latest["trade_date"],
            "contracts": latest["contracts"],
            "warehouse": latest["warehouse"],
        }
        if comex_data:
            output["comex_comparison"] = {
                "comex_trade_date": comex_data.get("trade_date"),
                "comex_registered_tonnes": comex_data.get("warehouse_stocks", {}).get("total_registered_tonnes"),
                "comex_eligible_tonnes": comex_data.get("warehouse_stocks", {}).get("total_eligible_tonnes"),
                "comex_combined_tonnes": comex_data.get("warehouse_stocks", {}).get("total_combined_tonnes"),
            }
        print(json.dumps(output, indent=2, default=str, ensure_ascii=False))
    else:
        report = generate_report(
            latest["trade_date"],
            latest["contracts"],
            latest["warehouse"],
            comex_data=comex_data,
            cny_usd=args.cny_usd,
        )
        log(report)

        # Save report
        reports_dir = os.path.join(CACHE_DIR, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        report_path = os.path.join(
            reports_dir,
            f"shfe_silver_{latest['trade_date']}_{datetime.now().strftime('%H%M%S')}.txt",
        )
        with open(report_path, "w") as f:
            f.write(report)
        log(f"\n  Report saved to: {report_path}")

    # History summary
    if len(all_results) > 1:
        log("\n" + "─" * 78)
        log("  HISTORY SUMMARY")
        log("─" * 78)
        log(f"  {'Date':<12s} {'Cargo (t)':>10s} {'Warrant (t)':>12s}"
            f" {'OI (lots)':>12s}")
        log(f"  {'─'*10:<12s} {'─'*10:>10s} {'─'*12:>12s} {'─'*12:>12s}")
        for r in all_results:
            td = r["trade_date"]
            tot = r["warehouse"].get("totals", {})
            oi = sum(c["open_interest"] for c in r["contracts"])
            lines_out = (
                f"  {td:<12s}"
                f" {tot.get('cargo_tonnes', 0):>10,.1f}"
                f" {tot.get('warrant_tonnes', 0):>12,.1f}"
                f" {oi:>12,}"
            )
            log(lines_out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
