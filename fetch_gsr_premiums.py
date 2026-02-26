#!/usr/bin/env python3
"""
GSR & Global Silver Premium Tracker
=====================================

Tracks two key metrics daily:

1. **Spot vs Physical (Global)** — jämför "pappers"-spotpriset på
   COMEX/LBMA med settlementpriser i Shanghai (SHFE), Mumbai (MCX) och
   ett London/LBMA spot-referenspris. Premiums visar var det fysiska
   suget är störst — ofta indikerar hög premium en begynnande "squeeze".

2. **Gold/Silver Ratio (GSR)** — guld/silver. Historiskt ger ett
   sjunkande GSR signal om att silverkvinsten bör delroteras mot guld
   (t.ex. när ratiot passerar 60, 50, 40…).

Data-källor:
  • COMEX/LBMA spot  — yfinance (XAGUSD=X, XAUUSD=X, SI=F, GC=F)
  • Shanghai (SHFE)  — lokalt cachad SHFE-JSON (fetch_shfe_silver.py)
  • Mumbai (MCX)     — yfinance (SILVERM=F eller SILVERMIC=F)
  • GSR historik     — kumulativt JSON (gsr_premiums_history.json)

Utdata:
  • comex_data/gsr_premiums_YYYYMMDD.json   — daglig snapshot
  • comex_data/gsr_premiums_timeseries.csv  — rad-per-dag tidsserie

Usage:
    python fetch_gsr_premiums.py               # fetch + print rapport
    python fetch_gsr_premiums.py --history 30  # visa N dagars historik
    python fetch_gsr_premiums.py --json        # skriv JSON till stdout
    python fetch_gsr_premiums.py --csv-only    # uppdatera bara CSV
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(SCRIPT_DIR, "comex_data")
HISTORY_FILE   = os.path.join(CACHE_DIR, "gsr_premiums_history.json")
TIMESERIES_CSV = os.path.join(CACHE_DIR, "gsr_premiums_timeseries.csv")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TROY_OZ_PER_KG   = 32.1507
MCX_SILVER_GRAMS = 1000          # MCX Silver (M) contract: 30 kg; price quoted INR/kg
                                  # SILVERMIC = mini 5 kg; same price unit

GSR_ALERT_LOW   = 40.0   # historiskt lågt — extremt köpläge för silver
GSR_CAUTION_LOW = 50.0   # signalnivå för rotation silver → guld
GSR_NEUTRAL     = 75.0   # "fair value" historisk median
GSR_HIGH        = 90.0   # silver historiskt billigt, köpläge

CSV_COLUMNS = [
    "date",
    "silver_spot_usd_oz",      # COMEX SI=F / LBMA reference
    "gold_spot_usd_oz",        # COMEX GC=F reference
    "gsr",                     # gold_spot / silver_spot
    "gsr_signal",              # text signal level
    "comex_futures_usd_oz",    # COMEX front-month (SI=F)
    "comex_basis",             # futures - spot
    # --- per-region physical price in USD/oz (via goldprice.org + FX) ---
    "region_usd_usd_oz",       # USA / LBMA (goldprice.org USD)
    "region_gbp_usd_oz",       # London / UK
    "region_eur_usd_oz",       # Europe (DE/EU)
    "region_chf_usd_oz",       # Switzerland
    "region_sgd_usd_oz",       # Singapore
    "region_aud_usd_oz",       # Australia
    "region_cad_usd_oz",       # Canada
    "region_sek_usd_oz",       # Sweden
    "region_inr_usd_oz",       # India
    "region_cny_usd_oz",       # China (goldprice.org)
    "region_try_usd_oz",       # Turkey
    "shfe_front_usd_oz",       # SHFE physical front-month settlement
    "avg_physical_usd_oz",     # average of all available regional prices
    # --- estimated physical buy price (spot × dealer premium, excl. tax) ---
    "phys_buy_usd_oz",         # LBMA spot + 8% dealer premium (no VAT)
    # --- legacy premium columns ---
    "shfe_premium_pct",
    "shfe_cny_usd",
    "mcx_silver_usd_oz",
    "mcx_premium_pct",
    "mcx_inr_usd",
    "data_timestamp_utc",
]

# Ordered list of region keys and display labels for plots
REGION_COLS = [
    ("region_usd_usd_oz", "USA/LBMA",   "USD"),
    ("region_gbp_usd_oz", "London/UK",  "GBP"),
    ("region_eur_usd_oz", "Europa/DE",  "EUR"),
    ("region_chf_usd_oz", "Schweiz",    "CHF"),
    ("region_sgd_usd_oz", "Singapore",  "SGD"),
    ("region_aud_usd_oz", "Australien", "AUD"),
    ("region_cad_usd_oz", "Kanada",     "CAD"),
    ("region_sek_usd_oz", "Sverige",    "SEK"),
    ("region_inr_usd_oz", "Indien",     "INR"),
    ("region_cny_usd_oz", "Kina (GP)",  "CNY"),
    ("region_try_usd_oz", "Turkiet",    "TRY"),
    ("shfe_front_usd_oz", "Shanghai/SHFE", "CNY"),
]


# ---------------------------------------------------------------------------
# yfinance helpers
# ---------------------------------------------------------------------------

def _yfinance_price(ticker: str, period: str = "5d") -> Optional[float]:
    """Return latest Close price for *ticker* via yfinance, or None."""
    try:
        import yfinance as yf
        hist = yf.Ticker(ticker).history(period=period)
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"  [yfinance] {ticker} failed: {e}", file=sys.stderr)
        return None


def _yfinance_prices_multi(tickers: list[str]) -> dict[str, Optional[float]]:
    """Fetch multiple tickers in one yfinance call (faster)."""
    results: dict[str, Optional[float]] = {t: None for t in tickers}
    try:
        import yfinance as yf
        data = yf.download(
            tickers,
            period="5d",
            progress=False,
            auto_adjust=True,
        )
        close = data["Close"] if "Close" in data.columns else data
        for t in tickers:
            try:
                col = close[t] if t in close.columns else close
                val = col.dropna().iloc[-1]
                results[t] = float(val)
            except Exception:
                pass
    except Exception as e:
        print(f"  [yfinance multi] failed: {e}", file=sys.stderr)
        # Fallback: fetch individually
        for t in tickers:
            results[t] = _yfinance_price(t)
    return results


# ---------------------------------------------------------------------------
# SHFE cache reader
# ---------------------------------------------------------------------------

def _load_shfe_latest() -> Optional[dict[str, Any]]:
    """Load the most recent SHFE silver JSON from comex_data/."""
    pattern = os.path.join(CACHE_DIR, "shfe_silver_????????.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    path = files[-1]
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"  [shfe] Could not load {path}: {e}", file=sys.stderr)
        return None


def _shfe_front_month(data: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return the front-month AG contract from SHFE data."""
    contracts = data.get("contracts", [])
    if not contracts:
        return None
    # Sort by delivery_month ascending, take first with positive OI
    active = [c for c in contracts if c.get("open_interest", 0) > 0]
    if not active:
        active = contracts
    return sorted(active, key=lambda c: c.get("delivery_month", "9999"))[0]


# ---------------------------------------------------------------------------
# MCX India helper
# ---------------------------------------------------------------------------

_MCX_TICKERS = [
    "SILVERMIC=F",   # vad Yahoo Finance ibland har för Silver Mini MCX
    "SILVERM=F",     # Silver M (30 kg) på MCX India
]


def _fetch_mcx_silver_inr_kg() -> tuple[Optional[float], str]:
    """
    Return (price_inr_per_kg, ticker_used) for MCX India silver.

    Tries SILVERM=F and SILVERMIC=F via yfinance.
    MCX quotes are in INR per kg for standard Silver (SILVERM) and
    INR per kg for Silver Mini (SILVERMIC).
    Returns (None, '') on failure.
    """
    import yfinance as yf
    for ticker in _MCX_TICKERS:
        try:
            hist = yf.Ticker(ticker).history(period="5d")
            if not hist.empty:
                price = float(hist["Close"].iloc[-1])
                if price > 0:
                    return price, ticker
        except Exception:
            continue
    return None, ""


def _inr_usd_rate() -> Optional[float]:
    """Return INR per USD (e.g. ~83.5) or None."""
    return _yfinance_price("USDINR=X")


# ---------------------------------------------------------------------------
# goldprice.org — live silver price in 11 currencies simultaneously
# ---------------------------------------------------------------------------

_GOLDPRICE_URL = (
    "https://data-asg.goldprice.org/dbXRates/"
    "USD,EUR,GBP,AUD,CHF,SGD,INR,CNY,SEK,TRY,CAD"
)
_FRANKFURTER_URL = (
    "https://api.frankfurter.app/latest?from=USD"
    "&to=EUR,GBP,AUD,CHF,SGD,INR,CNY,SEK,TRY,CAD"
)

def _fetch_goldprice_regions() -> dict[str, float]:
    """
    Returns dict: currency → xagPrice (local currency / troy oz)
    from goldprice.org.
    """
    try:
        from curl_cffi import requests as cr
        r = cr.get(_GOLDPRICE_URL, impersonate="chrome120", timeout=15,
                   headers={"Referer": "https://goldprice.org/"})
        if not r.ok:
            return {}
        out: dict[str, float] = {}
        for item in r.json().get("items", []):
            curr = item.get("curr", "")
            price = item.get("xagPrice")
            if curr and price:
                out[curr] = float(price)
        return out
    except Exception as e:
        print(f"  [goldprice.org] {e}", file=sys.stderr)
        return {}


def _fetch_fx_rates() -> dict[str, float]:
    """Returns dict: currency → units per 1 USD (frankfurter.app)."""
    try:
        from curl_cffi import requests as cr
        r = cr.get(_FRANKFURTER_URL, impersonate="chrome120", timeout=10)
        if r.ok:
            rates = r.json().get("rates", {})
            rates["USD"] = 1.0
            return rates
    except Exception as e:
        print(f"  [frankfurter] {e}", file=sys.stderr)
    return {}


# ---------------------------------------------------------------------------
# GSR signal text
# ---------------------------------------------------------------------------

def _gsr_signal(gsr: float) -> str:
    if gsr <= GSR_ALERT_LOW:
        return "EXTREMT_LÅGT"       # sell silver, buy gold
    if gsr <= GSR_CAUTION_LOW:
        return "ROTERA_SIGNAL"      # consider rotating silver profit → gold
    if gsr <= GSR_NEUTRAL:
        return "NEUTRALT"
    return "HISTORISKT_HÖGT"        # silver cheap vs gold


# ---------------------------------------------------------------------------
# Main fetch logic
# ---------------------------------------------------------------------------

def fetch_all() -> dict[str, Any]:
    """Fetch all data and return a single snapshot dict."""
    ts_utc = datetime.now(timezone.utc).isoformat()
    today  = datetime.now().strftime("%Y%m%d")

    print("Hämtar priser…")

    # ------------------------------------------------------------------
    # 1) Gold & Silver: use SI=F / GC=F as primary (COMEX near-month),
    #    fall back to XAGUSD=X / XAUUSD=X if the futures are unavailable.
    # ------------------------------------------------------------------
    prices = _yfinance_prices_multi(["SI=F", "GC=F", "XAGUSD=X", "XAUUSD=X"])

    comex_fut   = prices.get("SI=F")
    gold_fut    = prices.get("GC=F")
    silver_spot = comex_fut or prices.get("XAGUSD=X")
    gold_spot   = gold_fut  or prices.get("XAUUSD=X")

    # ------------------------------------------------------------------
    # 2) GSR
    # ------------------------------------------------------------------
    gsr: Optional[float] = None
    gsr_signal_txt = "N/A"
    if silver_spot and gold_spot and silver_spot > 0:
        gsr = round(gold_spot / silver_spot, 4)
        gsr_signal_txt = _gsr_signal(gsr)

    # Basis (contango / backwardation)
    comex_basis: Optional[float] = None
    if comex_fut and silver_spot:
        comex_basis = round(comex_fut - silver_spot, 4)

    # ------------------------------------------------------------------
    # 3) SHFE (Shanghai) front-month settlement → USD/oz premium
    # ------------------------------------------------------------------
    shfe_data = _load_shfe_latest()
    shfe_front_usd     = None
    shfe_premium_pct   = None
    shfe_cny_usd       = None
    shfe_trade_date    = None

    if shfe_data:
        shfe_trade_date = shfe_data.get("trade_date")
        shfe_cny_usd    = shfe_data.get("cny_usd")
        front_contract  = _shfe_front_month(shfe_data)
        if front_contract:
            shfe_front_usd = front_contract.get("settlement_price_usd_oz")
            if shfe_front_usd and silver_spot and silver_spot > 0:
                shfe_premium_pct = round(
                    (shfe_front_usd - silver_spot) / silver_spot * 100, 3
                )

    # ------------------------------------------------------------------
    # 4) MCX India silver → USD/oz premium
    # ------------------------------------------------------------------
    mcx_usd_oz       = None
    mcx_premium_pct  = None
    mcx_inr_usd      = None
    mcx_ticker_used  = ""

    try:
        inr_per_usd = _inr_usd_rate()
        mcx_inr_kg, mcx_ticker_used = _fetch_mcx_silver_inr_kg()
        if mcx_inr_kg and inr_per_usd and inr_per_usd > 0:
            mcx_inr_usd = round(inr_per_usd, 4)
            mcx_usd_oz = round(mcx_inr_kg / inr_per_usd / TROY_OZ_PER_KG, 4)
            if silver_spot and silver_spot > 0:
                mcx_premium_pct = round(
                    (mcx_usd_oz - silver_spot) / silver_spot * 100, 3
                )
    except Exception as e:
        print(f"  [MCX] Fel: {e}", file=sys.stderr)

    # ------------------------------------------------------------------
    # 5) goldprice.org — per-region silver price in local currency → USD/oz
    # ------------------------------------------------------------------
    print("  Hämtar regionala priser (goldprice.org + FX)…")
    gp_data  = _fetch_goldprice_regions()   # curr → local_price/oz
    fx_rates = _fetch_fx_rates()            # curr → units per USD

    # currency → column name mapping
    _CURR_TO_COL = {
        "USD": "region_usd_usd_oz",
        "GBP": "region_gbp_usd_oz",
        "EUR": "region_eur_usd_oz",
        "CHF": "region_chf_usd_oz",
        "SGD": "region_sgd_usd_oz",
        "AUD": "region_aud_usd_oz",
        "CAD": "region_cad_usd_oz",
        "SEK": "region_sek_usd_oz",
        "INR": "region_inr_usd_oz",
        "CNY": "region_cny_usd_oz",
        "TRY": "region_try_usd_oz",
    }

    region_prices: dict[str, Optional[float]] = {col: None for col in _CURR_TO_COL.values()}
    physical_prices_for_avg: list[float] = []

    for curr, col in _CURR_TO_COL.items():
        local_price = gp_data.get(curr)
        fx          = fx_rates.get(curr)
        if local_price and fx and fx > 0:
            usd_oz = round(local_price / fx, 4)
            region_prices[col] = usd_oz
            physical_prices_for_avg.append(usd_oz)

    # Also include SHFE in average if its date matches today
    if shfe_front_usd and shfe_trade_date and str(shfe_trade_date) == today:
        physical_prices_for_avg.append(shfe_front_usd)

    avg_physical = (
        round(sum(physical_prices_for_avg) / len(physical_prices_for_avg), 4)
        if physical_prices_for_avg else None
    )

    # ------------------------------------------------------------------
    # Assemble snapshot
    # ------------------------------------------------------------------
    snapshot: dict[str, Any] = {
        "date":                      today,
        "data_timestamp_utc":        ts_utc,
        # Spot / GSR
        "silver_spot_usd_oz":        round(silver_spot, 4) if silver_spot else None,
        "gold_spot_usd_oz":          round(gold_spot, 4)   if gold_spot   else None,
        "gsr":                       gsr,
        "gsr_signal":                gsr_signal_txt,
        # COMEX futures
        "comex_futures_usd_oz":      round(comex_fut, 4)   if comex_fut   else None,
        "comex_basis":               comex_basis,
        # Per-region physical prices (USD/oz)
        **region_prices,
        # SHFE
        "shfe_front_usd_oz":         round(shfe_front_usd, 4) if shfe_front_usd else None,
        "avg_physical_usd_oz":       avg_physical,
        # Estimated physical buy price: spot + 8% dealer premium (excl. VAT)
        "phys_buy_usd_oz":    round(silver_spot * 1.08, 4)          if silver_spot else None,
        # Legacy premium columns
        "shfe_premium_pct":          shfe_premium_pct,
        "shfe_cny_usd":              shfe_cny_usd,
        "shfe_trade_date":           shfe_trade_date,
        "mcx_silver_usd_oz":         mcx_usd_oz,
        "mcx_premium_pct":           mcx_premium_pct,
        "mcx_inr_usd":               mcx_inr_usd,
        "mcx_ticker":                mcx_ticker_used,
    }

    return snapshot


# ---------------------------------------------------------------------------
# History / persistence
# ---------------------------------------------------------------------------

def _load_history() -> list[dict[str, Any]]:
    if not os.path.exists(HISTORY_FILE):
        return []
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_history(history: list[dict[str, Any]]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def _upsert_history(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    """Insert or replace the entry for snapshot['date'] in history."""
    history = _load_history()
    date_key = snapshot["date"]
    history = [h for h in history if h.get("date") != date_key]
    history.append(snapshot)
    history.sort(key=lambda h: h.get("date", ""))
    _save_history(history)
    return history


def _save_daily_json(snapshot: dict[str, Any]) -> str:
    """Write snapshot to comex_data/gsr_premiums_YYYYMMDD.json."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    date_tag = snapshot["date"]
    path = os.path.join(CACHE_DIR, f"gsr_premiums_{date_tag}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_timeseries_csv(history: list[dict[str, Any]]) -> None:
    """Write/overwrite gsr_premiums_timeseries.csv from full history."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(TIMESERIES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in history:
            writer.writerow(row)
    print(f"  CSV uppdaterad → {os.path.relpath(TIMESERIES_CSV, SCRIPT_DIR)}")


# ---------------------------------------------------------------------------
# Report printer
# ---------------------------------------------------------------------------

def _pct_arrow(pct: Optional[float]) -> str:
    if pct is None:
        return "N/A"
    sign = "+" if pct >= 0 else ""
    arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "→")
    return f"{arrow} {sign}{pct:.2f}%"


def print_report(snap: dict[str, Any]) -> None:
    date_str = snap.get("date", "?")
    ts       = snap.get("data_timestamp_utc", "")

    # ── helpers ────────────────────────────────────────────────────
    W = 66   # inner content width (between ║ chars)

    def row(text: str) -> str:
        """Left-pad to W, wrap in ║ borders."""
        return f"║{text:<{W}}║"

    def sep() -> str:
        return "╠" + "═" * W + "╣"

    silver = snap.get("silver_spot_usd_oz")
    gold   = snap.get("gold_spot_usd_oz")
    gsr    = snap.get("gsr")
    signal = snap.get("gsr_signal", "N/A")

    comex_fut   = snap.get("comex_futures_usd_oz")
    comex_basis = snap.get("comex_basis")
    shfe_usd    = snap.get("shfe_front_usd_oz")
    shfe_pct    = snap.get("shfe_premium_pct")
    shfe_date   = snap.get("shfe_trade_date", "?")
    mcx_usd     = snap.get("mcx_silver_usd_oz")
    mcx_pct     = snap.get("mcx_premium_pct")

    s_sil   = f"${silver:.3f} /oz" if silver else "N/A"
    s_gld   = f"${gold:.2f} /oz"   if gold   else "N/A"
    s_gsr   = f"{gsr:.2f}"         if gsr    else "N/A"
    s_basis = f"(basis: {comex_basis:+.3f})" if comex_basis is not None else ""
    s_fut   = f"${comex_fut:.3f} /oz  {s_basis}" if comex_fut else "N/A"

    print()
    print("╔" + "═" * W + "╗")
    print(row(f"  GSR & Global Silver Premium Tracker"))
    print(row(f"  Datum: {date_str}    {ts[11:19]} UTC"))
    print(sep())
    print(row("  PRISER (COMEX)"))
    print(sep())
    print(row(f"  Silver  (SI=F)   {s_sil}"))
    print(row(f"  Guld    (GC=F)   {s_gld}"))
    print(row(f"  COMEX fut        {s_fut}"))
    print(sep())
    print(row("  GOLD/SILVER RATIO  (GSR)  ⚖️"))
    print(sep())
    print(row(f"  GSR    {s_gsr}     Signal: {signal}"))
    print(row(""))
    _gsr_bar(gsr, W)
    print(sep())
    print(row("  GLOBALA PREMIUMS vs COMEX SPOT  🌍"))
    print(sep())

    # COMEX/London = referens
    print(row(f"  COMEX / London (ref)   {s_sil:<20}  →  0.000%"))

    if shfe_usd:
        shfe_str = f"${shfe_usd:.3f} /oz"
        prem_str = _pct_arrow(shfe_pct)
        print(row(f"  Shanghai SHFE ({shfe_date})  {shfe_str:<16} {prem_str}"))
    else:
        print(row("  Shanghai SHFE            Ingen SHFE-data i cache"))

    if mcx_usd:
        mcx_str = f"${mcx_usd:.3f} /oz"
        mprem   = _pct_arrow(mcx_pct)
        ticker  = snap.get("mcx_ticker", "")
        print(row(f"  Mumbai MCX ({ticker:<11})  {mcx_str:<16} {mprem}"))
    else:
        print(row("  Mumbai MCX               Ej tillgänglig (ticker saknas)"))

    print(sep())
    print(row("  TOLKNING"))
    print(sep())
    _print_interpretation(gsr, shfe_pct, mcx_pct)
    print("╚" + "═" * W + "╝")
    print()


def _gsr_bar(gsr: Optional[float], W: int = 66) -> None:
    """Print an ASCII bar showing where GSR sits on the historical scale (30-120)."""
    low, high = 30.0, 120.0
    bar_width = W - 6   # account for leading '  [' and trailing ']'

    def _pos(val: float) -> int:
        return max(0, min(bar_width - 1,
                         int((val - low) / (high - low) * bar_width)))

    bar = ["-"] * bar_width
    for marker in (40, 50, 75, 90):
        bar[_pos(marker)] = "|"

    if gsr:
        bar[_pos(max(low, min(high, gsr)))] = "█"

    bar_str = "".join(bar)
    # Build label row below the bar; labels: 30, 50, 75, 90, 120
    label_row = [" "] * bar_width
    for lbl, val in (("30", 30), ("50", 50), ("75", 75), ("90", 90)):
        p = _pos(val)
        for i, ch in enumerate(lbl):
            if p + i < bar_width:
                label_row[p + i] = ch
    # "120" at the right edge — fit what we can
    lbl120 = "120"
    p120 = _pos(119)
    for i, ch in enumerate(lbl120):
        tgt = p120 - len(lbl120) + 1 + i   # right-align before edge
        if 0 <= tgt < bar_width:
            label_row[tgt] = ch
    line2 = "  " + "".join(label_row)
    print(f"║  [{bar_str}]║")
    print(f"║{line2:<{W}}║")


def _print_interpretation(
    gsr:      Optional[float],
    shfe_pct: Optional[float],
    mcx_pct:  Optional[float],
) -> None:
    lines = []

    if gsr is not None:
        if gsr <= GSR_ALERT_LOW:
            lines.append(f"  GSR {gsr:.1f} — EXTREMT LÅGT. Silver historiskt dyrt vs guld.")
            lines.append("  Överväg att rotera silvervinster → guld/platina.")
        elif gsr <= GSR_CAUTION_LOW:
            lines.append(f"  GSR {gsr:.1f} — Rotationssignal. Silver starkt vs guld.")
            lines.append("  Historiskt har GSR under 50 lett till utsträckning.")
        elif gsr <= GSR_NEUTRAL:
            lines.append(f"  GSR {gsr:.1f} — Neutralt läge. Silver marginellt attraktivt.")
        else:
            lines.append(f"  GSR {gsr:.1f} — Silver historiskt BILLIGT vs guld.")
            lines.append("  Klassiskt köpläge för silver relativt guld.")

    if shfe_pct is not None:
        if shfe_pct > 5:
            lines.append(f"  SHFE +{shfe_pct:.1f}% — Stark Shanghai-premium, kinesisk"
                         " fysisk efterfrågan hög.")
        elif shfe_pct > 1:
            lines.append(f"  SHFE +{shfe_pct:.1f}% — Liten Shanghai-premium (normalt).")
        elif shfe_pct < -1:
            lines.append(f"  SHFE {shfe_pct:.1f}% — DISCOUNT i Shanghai, ovanligt.")

    if mcx_pct is not None:
        if mcx_pct > 5:
            lines.append(f"  MCX +{mcx_pct:.1f}% — Hög Mumbai-premium. Indisk fysisk"
                         " efterfrågan driver priset.")
        elif mcx_pct > 1:
            lines.append(f"  MCX +{mcx_pct:.1f}% — Normal Mumbai-premium.")

    if not lines:
        lines.append("  Otillräcklig data för tolkning.")

    for line in lines:
        padded = f"║{line:<66}║"
        print(padded)


# ---------------------------------------------------------------------------
# History display
# ---------------------------------------------------------------------------

def print_history(n: int = 30) -> None:
    history = _load_history()
    if not history:
        print("Ingen historik hittad.")
        return

    recent = history[-n:]

    print()
    print(f"{'Datum':<12} {'Silver':>9} {'Guld':>9} {'GSR':>7}  {'SHFE%':>7}  {'MCX%':>7}  Signal")
    print("─" * 72)
    for h in recent:
        d    = h.get("date", "?")
        sil  = h.get("silver_spot_usd_oz")
        gld  = h.get("gold_spot_usd_oz")
        gsr  = h.get("gsr")
        shfe = h.get("shfe_premium_pct")
        mcx  = h.get("mcx_premium_pct")
        sig  = h.get("gsr_signal", "")[:14]

        s_s  = f"${sil:.3f}"  if sil  else "  N/A   "
        s_g  = f"${gld:.2f}"  if gld  else "  N/A   "
        s_gsr= f"{gsr:.2f}"   if gsr  else "  N/A "
        s_sh = f"{shfe:+.2f}%" if shfe is not None else "   N/A "
        s_mx = f"{mcx:+.2f}%"  if mcx  is not None else "   N/A "

        print(f"{d:<12} {s_s:>9} {s_g:>9} {s_gsr:>7}  {s_sh:>7}  {s_mx:>7}  {sig}")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="GSR & Global Silver Premium Tracker"
    )
    parser.add_argument(
        "--history", type=int, nargs="?", const=30, metavar="N",
        help="Visa N senaste dagars historik (default 30)",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Skriv dagens snapshot som JSON till stdout",
    )
    parser.add_argument(
        "--csv-only", action="store_true",
        help="Regenerera bara tidserie-CSV från befintlig historik",
    )
    args = parser.parse_args()

    os.makedirs(CACHE_DIR, exist_ok=True)

    # Just regenerate CSV from existing history
    if args.csv_only:
        history = _load_history()
        write_timeseries_csv(history)
        return 0

    # Just show history table
    if args.history:
        print_history(args.history)
        return 0

    # --- Full fetch ---
    snapshot = fetch_all()

    # Persist
    daily_path = _save_daily_json(snapshot)
    history    = _upsert_history(snapshot)
    write_timeseries_csv(history)
    print(f"  Snapshot sparad → {os.path.relpath(daily_path, SCRIPT_DIR)}")

    if args.json:
        print(json.dumps(snapshot, indent=2, ensure_ascii=False))
        return 0

    print_report(snapshot)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
