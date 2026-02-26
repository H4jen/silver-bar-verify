#!/usr/bin/env python3
"""
test_physical_premiums.py
==========================
Verifierar hur mycket FYSISKT silver kostar jämfört med spotpriset.

Typiska premiums över spot (2025-2026):
  1oz Silver Eagle (mynttyp)   :  8-13%  (USA)
  1oz Maple Leaf / Philharmonic:  6-10%  (global)
  1oz silver bar (generisk)    :  3-5%   (USA/EU)
  100g silver bar              :  3-6%   (EU)
  1kg silver bar               :  2-4%   (EU)

Datakällor:
  1. goldprice.org — Live LBMA spot
  2. BullionVault  — Faktiska köp/sälj-priser för allokerat fysiskt silver
  3. Kitco         — Dealer ask/bid priser (live)
  4. COMEX SI=F    — yfinance referens

Kör:
    python test_physical_premiums.py
    python test_physical_premiums.py --verbose
"""

from __future__ import annotations

import argparse
import sys
import json
from datetime import datetime, timezone
from typing import Optional

# ============================================================================
# 1. SPOT PRICE — goldprice.org
# ============================================================================

GOLDPRICE_URL = (
    "https://data-asg.goldprice.org/dbXRates/"
    "USD,EUR,GBP,CHF,SEK,AUD,CAD,SGD,INR,CNY,TRY"
)

def fetch_spot() -> dict:
    """Hämtar spot via goldprice.org. Returnerar {currency: {xagPrice, xauPrice}}."""
    try:
        from curl_cffi import requests as cr
        r = cr.get(GOLDPRICE_URL, impersonate="chrome120", timeout=15,
                   headers={"Referer": "https://goldprice.org/"})
        r.raise_for_status()
        data = r.json()
        result = {}
        for item in data.get("items", []):
            curr = item.get("curr", "")
            if curr:
                result[curr] = {
                    "xagPrice": float(item.get("xagPrice", 0)),
                    "xauPrice": float(item.get("xauPrice", 0)),
                }
        return result
    except Exception as e:
        print(f"  [goldprice.org] fel: {e}", file=sys.stderr)
        return {}

# ============================================================================
# 2. BULLIONVAULT — faktiska köp/sälj-priser (allokerat fysiskt silver)
# ============================================================================
# BullionVault erbjuder 0-premie på stora platser (Toronto, Zürich, Singapore, NY, London)
# men har storage-avg. Priset är i USD/kg.

BULLIONVAULT_URL = "https://www.bullionvault.com/tracker_process.do?v=2"
TROY_OZ_PER_KG   = 32.1507

def fetch_bullionvault() -> Optional[dict]:
    """
    Hämtar BullionVault live silver köp/sälj.
    Returnerar dict med lokal info per vault, och bästa globala buy/sell.
    """
    try:
        from curl_cffi import requests as cr
        r = cr.get(
            BULLIONVAULT_URL, impersonate="chrome120", timeout=15,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120.0",
                "Accept": "application/json, */*",
            }
        )
        if not r.ok:
            print(f"  [BullionVault] HTTP {r.status_code}", file=sys.stderr)
            return None
        data = r.json()
        # BullionVault returns prices in USD/kg
        silver = data.get("silver", data.get("ag", {}))
        if not silver:
            # Try to find silver prices in the response
            for key in data:
                if isinstance(data[key], dict) and "buy" in str(data[key]):
                    silver = data[key]
                    break
        if not silver:
            if isinstance(data, list):
                silver = data
        vaults = []
        best_buy  = None
        best_sell = None
        raw = silver if isinstance(silver, list) else silver.get("prices", silver.get("data", []))
        if isinstance(raw, dict):
            raw = [raw]
        for item in (raw if isinstance(raw, list) else []):
            loc   = item.get("securityId", item.get("vault", item.get("marketId", "?")))
            qty   = item.get("quantity", item.get("qty", 0))
            buy   = item.get("limit", item.get("buy", item.get("purchasePrice")))
            sell  = item.get("bid", item.get("sell", item.get("salePrice")))
            currency = item.get("currency", item.get("curr", "USD"))
            if buy:
                buy_usd_oz  = float(buy) / TROY_OZ_PER_KG if currency == "USD" else None
                sell_usd_oz = float(sell) / TROY_OZ_PER_KG if sell and currency == "USD" else None
                vaults.append({
                    "vault": loc, "qty_kg": qty,
                    "buy_usd_kg": float(buy), "sell_usd_kg": float(sell) if sell else None,
                    "buy_usd_oz": buy_usd_oz, "sell_usd_oz": sell_usd_oz,
                })
                if buy_usd_oz and (best_buy is None or buy_usd_oz < best_buy):
                    best_buy = buy_usd_oz
                if sell_usd_oz and (best_sell is None or sell_usd_oz > best_sell):
                    best_sell = sell_usd_oz
        return {
            "vaults":    vaults,
            "best_buy":  best_buy,
            "best_sell": best_sell,
            "raw":       data,
        }
    except Exception as e:
        print(f"  [BullionVault] fel: {e}", file=sys.stderr)
        return None

# ============================================================================
# 3. COMEX SI=F via yfinance
# ============================================================================

def fetch_comex() -> Optional[float]:
    try:
        import yfinance as yf
        hist = yf.Ticker("SI=F").history(period="5d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"  [SI=F] fel: {e}", file=sys.stderr)
    return None

# ============================================================================
# 4. KITCO — scrape spot ask price
# ============================================================================

def fetch_kitco_spot() -> Optional[float]:
    """Försöker hämta Kitco live silver spot (USD/oz)."""
    try:
        from curl_cffi import requests as cr
        # Kitco metals API endpoint
        r = cr.get(
            "https://www.kitco.com/market/",
            impersonate="chrome120", timeout=10,
            headers={"Accept": "application/json"}
        )
        if r.ok:
            import re
            text = r.text
            # Look for silver spot in page
            m = re.search(r'"Silver"[^}]*?"ask"\s*:\s*"?([\d.]+)', text, re.IGNORECASE)
            if not m:
                m = re.search(r'SILVER_SPOT_ASK["\s:]+([0-9.]+)', text, re.IGNORECASE)
            if m:
                return float(m.group(1))
    except Exception as e:
        print(f"  [Kitco] fel: {e}", file=sys.stderr)
    return None

# ============================================================================
# 5. PRODUCT PREMIUM TABLE
# ============================================================================

# Typical dealer premiums over spot (2025-2026 market data)
# Based on: APMEX, JM Bullion, European dealers, SE dealers
PRODUCTS = [
    # (name,               prem_low, prem_high, note)
    ("1oz Silver Eagle",        8.0,  13.0, "ASE, US Mint — mynt med legal tender"),
    ("1oz Maple Leaf / Phil.",   6.0,  10.0, "Kanadensiskt/österrikiskt mynt"),
    ("1oz Britannia / Lunar",    6.0,  10.0, "Brittiska/australiska mynt"),
    ("1oz generisk silver bar",  3.0,   5.5, "Enkel tacka, ej myntformat"),
    ("10oz silver bar",          2.5,   4.5, "Folkligt format"),
    ("100g silver bar",          3.0,   5.0, "Vanligt i Europa"),
    ("1kg silver bar",           2.0,   3.5, "Lagerinvestering, lägst premie"),
    ("100oz silver bar",         1.5,   3.0, "Professionell/institutionell"),
]

# ============================================================================
# REGIONS — skatter & valuta
# ============================================================================

REGIONS = [
    # (flag, region, currency, vat_pct, import_duty_pct, notes)
    ("🇺🇸", "USA",         "USD",  0.0,  0.0, "Ingen federal moms på tackor"),
    ("🇬🇧", "UK",          "GBP",  0.0,  0.0, "0% VAT på investeringstackor"),
    ("🇪🇺", "EU / DE",     "EUR",  0.0,  0.0, "0% moms >500g tackor (DE, AT, LU)"),
    ("🇨🇭", "Schweiz",     "CHF",  0.0,  0.0, "0% moms invest.silver"),
    ("🇸🇬", "Singapore",   "SGD",  0.0,  0.0, "0% GST ädelmetaller"),
    ("🇦🇺", "Australien",  "AUD", 10.0,  0.0, "10% GST detaljhandel"),
    ("🇨🇦", "Kanada",      "CAD",  0.0,  0.0, "0% GST/HST tackor"),
    ("🇸🇪", "Sverige",     "SEK", 25.0,  0.0, "25% MOMS (hög!)"),
    ("🇮🇳", "Indien",      "INR",  3.0,  9.8, "3% GST + ~9.8% importtull"),
    ("🇨🇳", "Kina",        "CNY", 13.0,  0.0, "13% moms via SHFE"),
    ("🇹🇷", "Turkiet",     "TRY", 18.0,  0.0, "18% moms + volatil lokalpremie"),
]

# ============================================================================
# PRINT HELPERS
# ============================================================================

def W(n=88) -> str:
    return "═" * n

def section(title: str, n=88) -> str:
    return f"╠{W(n)}╣\n║  {title:<{n-2}}║"

def row(text: str, n=88) -> str:
    return f"║  {text:<{n-2}}║"

# ============================================================================
# MAIN REPORT
# ============================================================================

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--bv",             action="store_true",
                        help="Visa raw BullionVault JSON")
    args = parser.parse_args()

    N = 88
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print("Hämtar priser…")
    spot_data = fetch_spot()
    comex     = fetch_comex()
    bv        = fetch_bullionvault()

    lbma_spot: Optional[float] = None
    if spot_data.get("USD"):
        lbma_spot = spot_data["USD"]["xagPrice"]
    ref = lbma_spot or comex or 0.0

    sources = []
    if lbma_spot:  sources.append(f"goldprice.org/LBMA ${lbma_spot:.3f}")
    if comex:      sources.append(f"COMEX SI=F ${comex:.3f}")
    if bv and bv.get("best_buy"):
        sources.append(f"BullionVault buy ${bv['best_buy']:.3f}")

    print(f"  OK: {', '.join(sources)}")

    # -----------------------------------------------------------------
    # HEADER
    print()
    print(f"╔{W(N)}╗")
    print(f"║  {'FYSISKT SILVER — Premium-analys':^{N-2}}║")
    print(f"║  {ts:^{N-2}}║")
    print(f"╠{W(N)}╣")
    print(f"║  {'SPOT / REFERENSPRISER':^{N-2}}║")
    print(f"╠{W(N)}╣")

    if lbma_spot:
        print(row(f"  goldprice.org / LBMA spot  :  ${lbma_spot:.3f} /oz  ({lbma_spot * TROY_OZ_PER_KG:,.0f} USD/kg)", N))
    if comex:
        diff_c = (comex - ref) / ref * 100 if ref else 0
        print(row(f"  COMEX SI=F (futures)        :  ${comex:.3f} /oz  ({diff_c:+.2f}% vs spot)", N))
    if bv and bv.get("best_buy"):
        diff_bv = (bv["best_buy"] - ref) / ref * 100 if ref else 0
        print(row(f"  BullionVault (allokerat)    :  ${bv['best_buy']:.3f} /oz  ({diff_bv:+.2f}% vs LBMA)", N))
        if bv.get("best_sell"):
            sp = bv["best_sell"]
            print(row(f"    sälj-pris               :  ${sp:.3f} /oz  (spread ${bv['best_buy']-sp:.3f})", N))

    # -----------------------------------------------------------------
    # PRODUKTPREMIUMS
    print(f"╠{W(N)}╣")
    print(f"║  {'PRODUKTPREMIUM ÖVER SPOT (handlares ask vs LBMA)':^{N-2}}║")
    print(f"╠{W(N)}╣")
    hdr = f"  {'Produkt':<38} {'Prem-låg':>8} {'Prem-hög':>9} {'USD/oz (low)':>13} {'USD/oz (high)':>14}"
    print(row(hdr, N))
    print(f"║  {'─'*(N-4)}  ║")

    for name, plo, phi, note in PRODUCTS:
        usd_low  = ref * (1 + plo / 100)
        usd_high = ref * (1 + phi / 100)
        r = f"  {name:<38} {plo:>6.1f}%  {phi:>6.1f}%    ${usd_low:>8.2f}      ${usd_high:>8.2f}"
        print(row(r, N))

    # -----------------------------------------------------------------
    # REGIONAL EFFECTIVE PRICE (1oz coin, typ Maple/Phil., ~8% premie median)
    print(f"╠{W(N)}╣")
    print(f"║  {'EFFEKTIVT KÖPPRIS PER REGION — 1oz mynt (~8% handlarpremie)':^{N-2}}║")
    print(f"╠{W(N)}╣")
    hdr2 = f"  {'Region':<22} {'Lokal spot/oz':>14} {'Lokalt köppris*':>16} {'vs LBMA USD':>12} {'Skatt':>6}"
    print(row(hdr2, N))
    print(f"║  {'─'*(N-4)}  ║")

    DEALER_PREM = 8.0  # % för 1oz mynt (median)

    for flag, region, curr, vat, duty, notes in REGIONS:
        gp = spot_data.get(curr, {})
        local_spot = gp.get("xagPrice")
        if not local_spot:
            print(row(f"  {flag} {region:<20}  {'N/A':>14}  — ej tillgänglig", N))
            continue

        # Effective: spot * (1 + duty/100) * (1 + vat/100) * (1 + dealer/100)
        eff_local = local_spot * (1 + duty / 100) * (1 + vat / 100) * (1 + DEALER_PREM / 100)

        # Convert back to USD via goldprice.org's USD spot
        if curr == "USD":
            eff_usd = eff_local
            local_str = f"${local_spot:>8.3f}"
            eff_str   = f"${eff_local:>8.2f}"
        else:
            from curl_cffi import requests as cr  # already imported above
            # derive FX from ratio goldprice.org local/USD
            usd_ref_gp = spot_data.get("USD", {}).get("xagPrice", ref)
            fx = local_spot / usd_ref_gp if usd_ref_gp else 1.0
            eff_usd = eff_local / fx if fx else None
            local_str = f"{local_spot:>9.2f} {curr}"
            eff_str   = f"{eff_local:>9.2f} {curr}"

        total_tax = vat + duty
        total_prem_vs_lbma = (eff_usd - ref) / ref * 100 if eff_usd and ref else None

        prem_str = f"{total_prem_vs_lbma:+.1f}%" if total_prem_vs_lbma is not None else "N/A"
        tax_str  = f"{total_tax:.0f}%"

        line = f"  {flag} {region:<20}  {local_str:>14}  {eff_str:>16}  {prem_str:>12}  {tax_str:>6}  {notes}"
        print(row(line, N))

    # -----------------------------------------------------------------
    print(f"╠{W(N)}╣")
    print(row(f"  * Eff från: spot × (1+importtull) × (1+moms) × (1+{DEALER_PREM:.0f}% handlarpremie)", N))
    print(row( "    Handlarpremie 8% = typisk 1oz mynt (Maple Leaf, Philharmoniker, etc.)", N))
    print(row( "    Kör med --verbose för regiondetaljering", N))
    print(f"╚{W(N)}╝")

    # -----------------------------------------------------------------
    # VERBOSE: Jämförelse med olika premienivåer
    if args.verbose and ref:
        print()
        print(f"  {'─'*84}")
        print(f"  PREMIEKALKYL — vad du betalar för 1oz vid olika premienivåer (spotref ${ref:.3f})")
        print(f"  {'─'*84}")
        print(f"  {'Premie':>8}  {'USD/oz':>10}  {'1kg kostar':>12}  {'Månadskostnad 100oz':>20}")
        print(f"  {'─'*84}")
        for pct in [0, 2, 4, 6, 8, 10, 12, 15, 20]:
            p_usd = ref * (1 + pct / 100)
            kg    = p_usd * TROY_OZ_PER_KG
            mo100 = p_usd * 100
            print(f"  {pct:>6.0f}%  ${p_usd:>9.3f}  ${kg:>10.0f}  ${mo100:>18.0f}")
        print()

        print(f"  {'─'*84}")
        print(f"  SLUTSATS:")
        print(f"  Spot (LBMA)   : ${ref:.3f}/oz")
        for name, plo, phi, _ in PRODUCTS:
            lo = ref * (1 + plo/100)
            hi = ref * (1 + phi/100)
            print(f"  {name:<38}: ${lo:.2f} – ${hi:.2f}")
        print()

    # BullionVault raw
    if args.bv and bv:
        print("\n--- BullionVault RAW ---")
        print(json.dumps(bv.get("raw", bv), indent=2)[:5000])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
