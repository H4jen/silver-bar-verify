#!/usr/bin/env python3
"""
test_physical_silver_prices.py
================================
Jämför fysiska silverpriset från hela världen — samma troy oz, olika marknader.

Datakällor:
  1. goldprice.org   — Live LBMA-baserat spotpris i 11 valutor samtidigt
  2. frankfurter.app — Oberoende FX-kurser för kross-kontroll
  3. SHFE cache      — Shanghai-börsen (fetch_shfe_silver.py lokalt cache)
  4. yfinance        — COMEX SI=F front-month referens, USDINR live

Regionala skatter & tullar (inbyggd i "effektivt köppris"):
  DE/EUR  : 0% moms på investeringssilvertackor >500g (7% på mynt)
  UK/GBP  : 0% VAT på silvertackor
  CH/CHF  : 0% moms på investeringssilver
  SG/SGD  : 0% GST på ädelmetaller
  SE/SEK  : 25% moms på silver (OBS: högt!)
  AU/AUD  : 10% GST tillkommer på detaljhandel
  IN/INR  : ~12.5% total (3% GST + ~9.8% importtull)
  CN/CNY  : 13% moms via SHFE-settlement
  TR/TRY  : 18% moms + volatil premie (ibland +20-40%)
  US/USD  : 0% federal moms på tackor (delstater varierar)
  CA/CAD  : 0% GST/HST på ädelmetalltackor

Usage:
    python test_physical_silver_prices.py
    python test_physical_silver_prices.py --verbose
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(SCRIPT_DIR, "comex_data")

TROY_OZ_PER_KG = 32.1507

# ---------------------------------------------------------------------------
# Region metadata
# ---------------------------------------------------------------------------
# currency → (flag, region name, tax_note, vat_pct, dealer_premium_low, dealer_premium_high)
REGIONS: dict[str, tuple[str, str, str, float, float, float]] = {
    "USD": ("🇺🇸", "USA (COMEX/LBMA ref)", "0% federal moms",              0.00, 1.5, 4.0),
    "GBP": ("🇬🇧", "London / UK",          "0% VAT on bars",               0.00, 2.0, 5.0),
    "EUR": ("🇪🇺", "Europa (DE/EU)",        "0% moms >500g tackor",         0.00, 2.0, 5.0),
    "CHF": ("🇨🇭", "Schweiz",              "0% moms invest.silver",        0.00, 1.0, 3.0),
    "SGD": ("🇸🇬", "Singapore",            "0% GST ädel metaller",         0.00, 1.5, 4.0),
    "AUD": ("🇦🇺", "Australien",           "10% GST detaljhandel",        10.00, 2.0, 5.0),
    "CAD": ("🇨🇦", "Kanada",              "0% GST/HST tackor",             0.00, 2.0, 5.0),
    "SEK": ("🇸🇪", "Sverige",              "25% moms (högt!)",            25.00, 2.0, 5.0),
    "INR": ("🇮🇳", "Indien (Mumbai/MCX)", "3% GST + ~9.8% tull = ~12.5%",12.50, 2.0, 5.0),
    "CNY": ("🇨🇳", "Kina (Shanghai/SHFE)", "13% moms",                   13.00, 1.5, 4.0),
    "TRY": ("🇹🇷", "Turkiet",              "18% moms + lokal premie",     18.00, 5.0,20.0),
}

# ---------------------------------------------------------------------------
# Fetch: goldprice.org multi-currency
# ---------------------------------------------------------------------------
GOLDPRICE_URL = (
    "https://data-asg.goldprice.org/dbXRates/"
    "USD,EUR,GBP,AUD,CHF,SGD,INR,CNY,SEK,TRY,CAD"
)

def fetch_goldprice() -> dict[str, dict[str, float]]:
    """
    Returns dict keyed by currency, each with keys:
      xagPrice  — silver price in that currency per troy oz
      xauPrice  — gold price in that currency per troy oz
    """
    try:
        from curl_cffi import requests as cr
        r = cr.get(GOLDPRICE_URL, impersonate="chrome120", timeout=15,
                   headers={"Referer": "https://goldprice.org/"})
        if not r.ok:
            print(f"  [goldprice.org] HTTP {r.status_code}", file=sys.stderr)
            return {}
        data = r.json()
        result: dict[str, dict[str, float]] = {}
        for item in data.get("items", []):
            curr = item.get("curr", "")
            if curr:
                result[curr] = {
                    "xagPrice": float(item.get("xagPrice", 0)),
                    "xauPrice": float(item.get("xauPrice", 0)),
                }
        return result
    except Exception as e:
        print(f"  [goldprice.org] error: {e}", file=sys.stderr)
        return {}

# ---------------------------------------------------------------------------
# Fetch: Frankfurter FX rates (cross-check)
# ---------------------------------------------------------------------------
FRANKFURTER_URL = (
    "https://api.frankfurter.app/latest?from=USD"
    "&to=EUR,GBP,AUD,CHF,SGD,INR,CNY,SEK,TRY,CAD"
)

def fetch_fx_rates() -> dict[str, float]:
    """Returns dict currency → units per 1 USD (e.g. EUR: 0.846)."""
    try:
        from curl_cffi import requests as cr
        r = cr.get(FRANKFURTER_URL, impersonate="chrome120", timeout=10)
        if r.ok:
            d = r.json()
            rates = d.get("rates", {})
            rates["USD"] = 1.0
            return rates
    except Exception as e:
        print(f"  [frankfurter] error: {e}", file=sys.stderr)
    return {}

# ---------------------------------------------------------------------------
# Fetch: COMEX SI=F front-month via yfinance
# ---------------------------------------------------------------------------
def fetch_comex_silver() -> Optional[float]:
    """Returns COMEX SI=F close price in USD/oz, or None."""
    try:
        import yfinance as yf
        hist = yf.Ticker("SI=F").history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"  [yfinance SI=F] error: {e}", file=sys.stderr)
    return None

# ---------------------------------------------------------------------------
# Fetch: SHFE front-month from local cache
# ---------------------------------------------------------------------------
def fetch_shfe() -> Optional[dict[str, Any]]:
    """Read latest shfe_silver_YYYYMMDD.json from cache dir."""
    pattern = os.path.join(CACHE_DIR, "shfe_silver_????????.json")
    files = sorted(glob.glob(pattern))
    if not files:
        return None
    try:
        with open(files[-1], encoding="utf-8") as f:
            d = json.load(f)
        contracts = d.get("contracts", [])
        active = [c for c in contracts if c.get("open_interest", 0) > 0]
        if not active:
            active = contracts
        front = sorted(active, key=lambda c: c.get("delivery_month", "9999"))[0]
        return {
            "trade_date":           d.get("trade_date"),
            "settlement_usd_oz":    front.get("settlement_price_usd_oz"),
            "settlement_rmb_kg":    front.get("settlement_price_rmb_kg"),
            "cny_usd":              d.get("cny_usd"),
            "delivery_month":       front.get("delivery_month"),
            "cache_file":           os.path.basename(files[-1]),
        }
    except Exception as e:
        print(f"  [SHFE cache] error: {e}", file=sys.stderr)
        return None

# ---------------------------------------------------------------------------
# Build comparison table
# ---------------------------------------------------------------------------

def build_table(
    gp_data:     dict[str, dict[str, float]],
    fx_rates:    dict[str, float],
    comex_usd:   Optional[float],
    shfe:        Optional[dict[str, Any]],
    verbose:     bool = False,
) -> None:

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ── reference: USD price from goldprice.org ──
    usd_ref: Optional[float] = gp_data.get("USD", {}).get("xagPrice")
    if not usd_ref and comex_usd:
        usd_ref = comex_usd

    W = 90

    def hline(ch="─") -> str:
        return ch * W

    print()
    print("╔" + "═"*W + "╗")
    print(f"║  {'Fysiskt Silverpris — Global Jämförelse':^{W-2}}║")
    print(f"║  {timestamp:^{W-2}}║")
    print("╠" + "═"*W + "╣")
    print(f"║  {'KÄLLA':25}  {'Lokal valuta/oz':17}  {'USD/oz':10}  "
          f"{'▲/▼ vs ref':11}  {'Moms/tull':11}  {'Eff. köppris*':12}║")
    print("╠" + "═"*W + "╣")

    rows = []

    # ── 1: COMEX SI=F ──────────────────────────────────────────────
    if comex_usd:
        rows.append({
            "flag":    "🇺🇸",
            "label":   "COMEX SI=F (futures)",
            "local":   f"${comex_usd:.3f}",
            "local_curr": "USD",
            "usd_oz":  comex_usd,
            "vat":     0.0,
            "prem_lo": 1.5,
            "prem_hi": 4.0,
            "note":    "Front-month futures, ref pris",
        })

    # ── 2: goldprice.org per region ────────────────────────────────
    for curr, (flag, region, tax_note, vat, prem_lo, prem_hi) in REGIONS.items():
        if curr not in gp_data:
            continue
        local_price = gp_data[curr]["xagPrice"]
        # derive USD/oz via goldprice.org's own USD price
        usd_price = gp_data.get("USD", {}).get("xagPrice", 0)
        # use FX as cross-check: local_price / fx_rate[curr] should ≈ usd_price
        fx = fx_rates.get(curr, 1.0)
        if fx > 0:
            usd_from_fx = local_price / fx
        else:
            usd_from_fx = None

        rows.append({
            "flag":       flag,
            "label":      region,
            "local":      f"{local_price:,.3f} {curr}",
            "local_curr": curr,
            "usd_oz":     usd_from_fx or usd_price,
            "vat":        vat,
            "prem_lo":    prem_lo,
            "prem_hi":    prem_hi,
            "note":       tax_note,
            "fx_rate":    fx,
            "local_price":local_price,
        })

    # ── 3: SHFE physical settlement ────────────────────────────────
    if shfe and shfe.get("settlement_usd_oz"):
        su = shfe["settlement_usd_oz"]
        sr = shfe["settlement_rmb_kg"]
        td = shfe.get("trade_date", "?")
        dm = shfe.get("delivery_month", "")
        cny = shfe.get("cny_usd")
        cny_str = f"{cny}" if cny else "?"
        rows.append({
            "flag":    "🇨🇳",
            "label":   f"SHFE (Shanghai físico) [{td}]",
            "local":   f"¥{sr:,} CNY/kg" if sr else "N/A",
            "local_curr": "CNY",
            "usd_oz":  su,
            "vat":     13.0,
            "prem_lo": 1.5,
            "prem_hi": 4.0,
            "note":    f"Front-month {dm}, CNY/USD={cny_str}",
        })

    # ── print rows ─────────────────────────────────────────────────
    for row in rows:
        usd    = row.get("usd_oz")
        vat    = row.get("vat", 0.0)
        plo    = row.get("prem_lo", 0.0)
        phi    = row.get("prem_hi", 0.0)

        usd_str    = f"${usd:.3f}" if usd else " N/A  "
        local_str  = row.get("local", "N/A")

        # premium vs COMEX reference
        if usd and usd_ref and usd_ref > 0:
            diff_pct = (usd - usd_ref) / usd_ref * 100
            arrow    = "▲" if diff_pct > 0.05 else ("▼" if diff_pct < -0.05 else "→")
            diff_str = f"{arrow}{diff_pct:+.1f}%"
        else:
            diff_str = "  N/A "

        # effective buy price = usd_oz × (1 + vat/100) × (1 + avg_dealer_premium/100)
        avg_prem = (plo + phi) / 2
        if usd:
            eff = usd * (1 + vat / 100) * (1 + avg_prem / 100)
            eff_str = f"~${eff:.2f}"
        else:
            eff_str = "N/A"

        vat_str = f"{vat:.0f}%" if vat > 0 else "0%"

        flag = row.get("flag", "  ")
        label = f"{flag} {row['label']}"

        print(f"║  {label:<27}{local_str:>17}  {usd_str:>10}  "
              f"{diff_str:>11}  {vat_str:>11}  {eff_str:>12}║")

    print("╠" + "═"*W + "╣")

    # ── Summary row ────────────────────────────────────────────────
    if usd_ref:
        print(f"║  {'Referens (LBMA spot USD)':40}  ${usd_ref:.3f}/oz"
              f"{'':>{W - 57}}║")
        kg_price = usd_ref * TROY_OZ_PER_KG
        print(f"║  {'= 1kg silver:':40}  ${kg_price:,.0f} USD"
              f"{'':>{W - 58}}║")

    print("╠" + "═"*W + "╣")
    print(f"║  * Eff. köppris = marknadspris × (1+moms) × (1+genomsnittlig handlarpremie)"
          f"{'':>{W - 75}}║")
    print(f"║  Handlarpremier är uppskattningar baserade på branschdata sep 2025-2026."
          f"{'':>{W - 72}}║")
    print("╚" + "═"*W + "╝")

    # ── verbose: FX cross-check ────────────────────────────────────
    if verbose and fx_rates and gp_data:
        print()
        print(hline())
        print("  FX KROSS-KONTROLL (goldprice.org vs frankfurter.app)")
        print(hline())
        print(f"  {'Valuta':<8} {'GP lokal/oz':>14} {'FX→USD/oz':>12} {'FF FX rate':>12}  diff")
        print("  " + "─"*58)
        usd_gp = gp_data.get("USD", {}).get("xagPrice", 0)
        for curr in ("EUR","GBP","AUD","CHF","SGD","INR","CNY","SEK","TRY","CAD"):
            gp  = gp_data.get(curr, {}).get("xagPrice")
            fx  = fx_rates.get(curr)
            if gp and fx and fx > 0:
                usd_fx = gp / fx
                diff   = usd_fx - usd_gp
                print(f"  {curr:<8} {gp:>14.3f} {usd_fx:>12.3f} {fx:>12.5f}  {diff:+.3f}")
        print()

    # ── SHFE details ───────────────────────────────────────────────
    if verbose and shfe:
        print()
        print(hline())
        print("  SHFE DETALJER (från cache)")
        print(hline())
        for k, v in shfe.items():
            if v is not None:
                print(f"  {k:<25} {v}")
        print()

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Global fysisk silverpris-jämförelse"
    )
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Visa FX-korscheck och SHFE-detaljer")
    args = parser.parse_args()

    print("Hämtar data…")

    gp_data   = fetch_goldprice()
    fx_rates  = fetch_fx_rates()
    comex_usd = fetch_comex_silver()
    shfe      = fetch_shfe()

    sources_ok = []
    if gp_data:  sources_ok.append(f"goldprice.org ({len(gp_data)} valutor)")
    if fx_rates: sources_ok.append(f"frankfurter.app ({len(fx_rates)-1} kurser)")
    if comex_usd:sources_ok.append(f"COMEX SI=F ${comex_usd:.3f}")
    if shfe:     sources_ok.append(f"SHFE cache ({shfe.get('trade_date','?')})")

    print(f"  Källor OK: {', '.join(sources_ok)}")

    if not gp_data and not comex_usd:
        print("\nFEL: Ingen prisdata tillgänglig.", file=sys.stderr)
        return 1

    build_table(gp_data, fx_rates, comex_usd, shfe, verbose=args.verbose)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
