#!/usr/bin/env python3
"""
fetch_dealer_prices.py
========================
Hämtar faktiska handlarpriser för 1oz silver bar från lokala marknader
med Playwright (full JS-rendering).

Datakällor:
  • Royal Mint (UK)      — £/oz via live-metal-price widget + bar listing
  • BullionStar (SG)     — SGD via services.bullionstar.com/product/v2/prices
  • SHFE (CN)            — cachad settlement (fetch_shfe_silver.py)
  Fallback (estimerat):
  • LBMA spot × dealer_premium baserat på goldprice.org

Utdata:
  Tabell med faktiskt handlarpris i lokal valuta och USD/oz,
  premium % över LBMA spot.

Usage:
    python fetch_dealer_prices.py
    python fetch_dealer_prices.py --json
    python fetch_dealer_prices.py --save   # sparar till JSON i comex_data/
"""

from __future__ import annotations

import argparse
import asyncio
import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR  = os.path.join(SCRIPT_DIR, "comex_data")
TROY_OZ_PER_KG = 32.1507

# ─── Royal Mint product IDs for 1oz silver bars ──────────────────
# class="text-white metal-price"  → live LBMA spot in GBP
# product listing: From:£XX.XX pattern for smallest (1oz) bar

# ─── BullionStar ─────────────────────────────────────────────────
# API: https://services.bullionstar.com/product/v2/prices?currency=SGD
# 1oz generic silver bar = productId 14
BULLIONSTAR_1OZ_PRODUCT_ID = 14

# ─────────────────────────────────────────────────────────────────

# Currencies needed by the scrapers (units per 1 USD)
_FX_CURRENCIES = ["GBP", "EUR", "AUD"]


def _fetch_fx_bulk() -> dict[str, float]:
    """Fetch all needed FX rates at once via yfinance (Yahoo Finance).
    Ticker format USD{CURR}=X gives units of CURR per 1 USD.
    Falls back to frankfurter.app if yfinance fails.
    """
    rates: dict[str, float] = {"USD": 1.0}
    tickers = [f"USD{c}=X" for c in _FX_CURRENCIES]
    try:
        import yfinance as yf
        data = yf.download(tickers, period="1d", auto_adjust=False, progress=False)
        close = data["Close"] if "Close" in data else data
        for c in _FX_CURRENCIES:
            ticker = f"USD{c}=X"
            try:
                val = float(close[ticker].dropna().iloc[-1])
                if val > 0:
                    rates[c] = val
            except Exception:
                pass
        missing = [c for c in _FX_CURRENCIES if c not in rates]
        if missing:
            raise ValueError(f"yfinance missing: {missing}")
        # Log the rates used
        rate_str = "  ".join(f"USD/{c}={rates[c]:.4f}" for c in _FX_CURRENCIES if c in rates)
        print(f"  FX (Yahoo Finance): {rate_str}")
        return rates
    except Exception as e:
        print(f"  [FX/yfinance] {e} — falling back to frankfurter.app", file=sys.stderr)

    # Fallback: frankfurter.app (ECB rates)
    try:
        from curl_cffi import requests as cr
        currencies = ",".join(_FX_CURRENCIES)
        r = cr.get(f"https://api.frankfurter.app/latest?from=USD&to={currencies}",
                   impersonate="chrome120", timeout=8)
        if r.ok:
            fb_rates = r.json().get("rates", {})
            rates.update({k: v for k, v in fb_rates.items() if v})
            rate_str = "  ".join(f"USD/{c}={rates[c]:.4f}" for c in _FX_CURRENCIES if c in rates)
            print(f"  FX (frankfurter fallback): {rate_str}")
    except Exception as e2:
        print(f"  [FX/frankfurter] {e2}", file=sys.stderr)

    return rates


def _fx_to_usd(currency: str, fx_cache: Optional[dict] = None) -> Optional[float]:
    """Return units of <currency> per 1 USD from cache, or single yfinance lookup."""
    if currency == "USD":
        return 1.0
    if fx_cache and currency in fx_cache:
        return fx_cache[currency]
    # Single-ticker fallback
    try:
        import yfinance as yf
        val = float(yf.Ticker(f"USD{currency}=X").fast_info["last_price"])
        if val > 0:
            return val
    except Exception:
        pass
    return None


def _parse_currency_amount(text: str) -> Optional[float]:
    """Extract numeric value from strings like '£70.79', 'SGD 124.50', '1,234.56 kr'."""
    text = text.strip()
    # remove currency symbols / prefixes
    cleaned = re.sub(r'[£€$₹A-Za-z\s]', '', text)
    cleaned = cleaned.replace(',', '')
    try:
        return float(cleaned)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────
# PLAYWRIGHT SCRAPERS
# ─────────────────────────────────────────────────────────────────

async def scrape_royal_mint(page) -> dict:
    """
    Royal Mint (UK) — 1oz silver bar price in GBP.
    Uses live-metal-price widget class for spot + product listing for bar price.
    """
    result = {"dealer": "Royal Mint", "region": "UK", "currency": "GBP",
              "url": "https://www.royalmint.com/invest/bullion/bullion-bars/silver-bars/",
              "product": "1oz Silver Bar", "source": "royalmint.com"}
    try:
        resp = await page.goto(result["url"], wait_until="networkidle", timeout=25000)
        if not resp or resp.status >= 400:
            result["error"] = f"HTTP {resp.status if resp else 'timeout'}"
            return result

        # Live silver spot price
        spot_el = await page.query_selector_all(".text-white.metal-price")
        spot_gbp = None
        for el in spot_el:
            txt = (await el.inner_text()).strip()
            if re.search(r'^\£[\d.,]+$', txt):
                val = _parse_currency_amount(txt)
                if val and 20 < val < 500:   # plausible silver spot in GBP
                    spot_gbp = val
                    break

        # Product listing — find smallest bar (1oz), starting from lowest From:£XX price
        text = await page.inner_text("body")
        # Product cards with "From:£XX.XX" — the lowest ones are 1oz bars
        from_prices = re.findall(r'From:£([\d,]+\.?\d*)', text)
        numeric = sorted([float(p.replace(',','')) for p in from_prices if p])
        # 1oz silver bar at ~£65-90, exclude gold (£700+)
        oz_bar_prices = [p for p in numeric if 50 < p < 200]

        result["lbma_spot_gbp"] = spot_gbp
        result["ask_price_local"] = oz_bar_prices[0] if oz_bar_prices else None
        result["ask_price_label"] = f"£{oz_bar_prices[0]:.2f}" if oz_bar_prices else None
        if spot_gbp and oz_bar_prices:
            result["premium_pct"] = round((oz_bar_prices[0] - spot_gbp) / spot_gbp * 100, 2)
        return result
    except Exception as e:
        result["error"] = str(e)[:100]
        return result


async def scrape_bgasc(page) -> dict:
    """
    BGASC (USA) — cheapest generic 1oz silver bar/round in USD.
    Scrapes product listings from homepage (best-sellers section).
    """
    result = {"dealer": "BGASC", "region": "USA", "currency": "USD",
              "url": "https://www.bgasc.com/",
              "product": "1oz Silver Bar/Round", "source": "bgasc.com"}
    try:
        resp = await page.goto(result["url"], wait_until="networkidle", timeout=30000)
        if not resp or resp.status >= 400:
            result["error"] = f"HTTP {resp.status if resp else 'timeout'}"
            return result
        await page.wait_for_timeout(3000)
        text = await page.inner_text("body")
        lines = text.split('\n')

        # Parse "name\n\nAs Low As:\xa0\n\n$price\n\nADD TO CART" blocks
        candidates = []  # (price, name)
        for i, line in enumerate(lines):
            if "As Low As" in line and i + 2 < len(lines):
                # Price is 2 lines ahead (blank line between)
                price_line = lines[i + 2].strip()
                m = re.match(r'\$\s*([\d,]+\.?\d*)', price_line)
                if not m:  # try i+1 as fallback
                    price_line = lines[i + 1].strip()
                    m = re.match(r'\$\s*([\d,]+\.?\d*)', price_line)
                if m:
                    val = float(m.group(1).replace(',', ''))
                    # Walk back to find product name (skip blank lines)
                    name = ""
                    for j in range(i - 1, max(0, i - 7), -1):
                        candidate = lines[j].strip()
                        if candidate and "As Low As" not in candidate and candidate != "ADD TO CART":
                            name = candidate
                            break
                    candidates.append((val, name))

        # Filter: must contain '1 oz' + 'silver' + not 'gold' + price in USD 1oz range
        silver_1oz = [
            (v, n) for v, n in candidates
            if '1 oz' in n.lower() and 'silver' in n.lower()
            and 'gold' not in n.lower()
            and 88 < v < 130
        ]
        # Prefer bars over rounds/coins, else take cheapest
        bars = [(v, n) for v, n in silver_1oz if 'bar' in n.lower()]
        target = sorted(bars or silver_1oz)[0] if (bars or silver_1oz) else None

        if target:
            val, name = target
            result["ask_price_local"] = val
            result["ask_price_label"] = f"${val:.2f}"
            result["product_name"] = name
        else:
            result["error"] = "No 1oz silver product found"
        return result
    except Exception as e:
        result["error"] = str(e)[:100]
        return result


async def scrape_abc_bullion(page) -> dict:
    """ABC Bullion (Australia) — 1oz silver pool allocated in AUD."""
    result = {"dealer": "ABC Bullion", "region": "Australia", "currency": "AUD",
              "url": "https://www.abcbullion.com.au/store/silver",
              "product": "1oz Silver Pool Allocated", "source": "abcbullion.com.au"}
    try:
        resp = await page.goto(result["url"], wait_until="networkidle", timeout=25000)
        if not resp or resp.status >= 400:
            result["error"] = f"HTTP {resp.status if resp else 'timeout'}"
            return result
        await page.wait_for_timeout(1500)

        text = await page.inner_text("body")
        # Collect AUD prices in 1oz silver range (AU$120-220)
        aud_vals = []
        for line in text.split('\n'):
            line = line.strip()
            m = re.search(r'\$\s*([\d,]+\.?\d*)', line)
            if m:
                val = float(m.group(1).replace(',', ''))
                if 120 < val < 230:
                    aud_vals.append(val)

        if aud_vals:
            val = min(aud_vals)  # cheapest = pool/bulk price
            result["ask_price_local"] = val
            result["ask_price_label"] = f"A${val:.2f}"
            result["note"] = "Pool/bulk allocated; physical bar prices slightly higher"
        else:
            result["error"] = "No AUD silver price found"
        return result
    except Exception as e:
        result["error"] = str(e)[:100]
        return result


async def scrape_proaurum(page) -> dict:
    """proaurum.de (EU/DE) — 1oz silver bar in EUR.

    Tries to get the 1oz Umicore bar.  If out-of-stock the 1kg Umicore bar
    price is used as a per-oz proxy (includes German 19 % VAT, so the premium
    vs LBMA reflects the real all-in retail cost in the euro zone).
    """
    result = {"dealer": "proaurum (DE)", "region": "EU", "currency": "EUR",
              "product": "1oz Silver Bar", "source": "proaurum.de"}
    url = "https://www.proaurum.de/shop/silber/silberbarren/"
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=25000)
        await page.wait_for_timeout(2500)
        if not resp or resp.status >= 400:
            result["error"] = f"HTTP {resp.status if resp else 'no response'}"
            return result
        result["url"] = page.url

        text = await page.inner_text("body")
        lines = text.split('\n')

        # Page format (proaurum.de):
        #   Silberbarren 31,1 Gramm Umicore  | Kaufen <price>€  | <sell_price>€ Verkaufen
        # OR
        #   Silberbarren 31,1 Gramm Umicore  | Benachrichtigung anlegen  | <sell>€ Verkaufen
        # Separator tokens: "Kaufen" = in-stock buy price; "Benachrichtigung anlegen" = out-of-stock

        def parse_eur_price(s: str) -> Optional[float]:
            for pat in [r'([\d]{1,5}[.,][\d]{2})\s*€', r'€\s*([\d]{1,5}[.,][\d]{2})']:
                m = re.search(pat, s)
                if m:
                    raw = m.group(1)
                    try:
                        return float(raw.replace('.', '').replace(',', '.'))
                    except Exception:
                        pass
            return None

        # Build flat token list: join adjacent non-empty lines per product block
        flat = [l.strip() for l in lines if l.strip()]

        # proaurum.de row structure (inner_text, per product):
        #   IN STOCK:   Silberbarren <name>  \n  <buy>€  \n  Kaufen  \n  <sell>€  \n  Verkaufen
        #   OUT STOCK:  Benachrichtigung anlegen  \n  <sell>€  \n  Verkaufen  \n  Silberbarren <name>
        # Strategy: find "Kaufen" tokens and walk backward/forward for price + name.
        products = []  # (per_oz_price, product_name, buy_price, is_in_stock, weight_g)

        def parse_eur_price(s: str) -> Optional[float]:
            """Parse German/EU number format: 3.158,26 € → 3158.26; 169,81 € → 169.81."""
            # Full German format: digits with optional dot-thousands + comma-decimal
            m = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2})\s*€', s)
            if m:
                return float(m.group(1).replace('.', '').replace(',', '.'))
            # Simple integer with €
            m = re.search(r'(\d+)\s*€', s)
            if m:
                return float(m.group(1))
            return None

        def weight_from_name(name: str) -> Optional[float]:
            m = re.search(r'(\d+(?:[.,]\d+)?)\s*gramm', name, re.I)
            if m:
                raw = m.group(1)
                # German thousands separator: "1.000" = 1000 g, "31,1" = 31.1 g
                if re.match(r'^\d{1,3}\.\d{3}$', raw):
                    return float(raw.replace('.', ''))   # "1.000" → 1000.0
                if re.match(r'^\d{1,3},\d{3}$', raw):
                    return float(raw.replace(',', ''))   # "1,000" → 1000.0 (alt form)
                return float(raw.replace(',', '.'))      # "31,1" → 31.1
            if re.search(r'31[,.]1\s*g|1\s*unze|1\s*feinunze', name, re.I):
                return 31.1035
            if re.search(r'1[.,]000\s*gramm|1000\s*gramm|\b1\s*kg\b', name, re.I):
                return 1000.0
            if re.search(r'5[.,]000\s*gramm|\b5\s*kg\b', name, re.I):
                return 5000.0
            return None

        for i, tok in enumerate(flat):
            if tok.lower() != "kaufen":
                continue
            # Walk backward to find the price (immediately before "Kaufen")
            buy_price = None
            for j in range(i - 1, max(-1, i - 4), -1):
                p = parse_eur_price(flat[j])
                if p and p > 10:
                    buy_price = p
                    break
            if not buy_price:
                continue
            # Walk backward past the price to find product name
            name = None
            for j in range(i - 2, max(-1, i - 8), -1):
                t = flat[j]
                if t.lower().startswith("silberbarren") or re.search(r'silver bar', t, re.I):
                    name = t
                    break
            if not name:
                continue
            wt_g = weight_from_name(name)
            if not wt_g or wt_g > 1001.0:   # skip institutional vault bars (>1kg)
                continue
            per_oz_price = (buy_price / (wt_g / 31.1035)) / 1.19   # strip DE 19% MwSt
            products.append((per_oz_price, name, buy_price, True, wt_g))

        if not products:
            result["error"] = "No silver bar products parsed from proaurum.de"
            return result

        # products = (per_oz_price, name, buy_price, in_stock, weight_g)
        # Prefer in-stock 1oz; fall back to cheapest per-oz from any weight
        one_oz = [t for t in products if abs(t[4] - 31.1035) < 2]
        in_stk_1oz = [t for t in one_oz if t[3]]
        pool = in_stk_1oz or one_oz or sorted(products)

        pool.sort()
        ppoz, name, buy_price, stk, wt_g = pool[0]

        result["ask_price_local"] = ppoz
        result["ask_price_label"] = f"€{ppoz:.2f}"
        result["product_name"] = name
        result["in_stock"] = stk
        if wt_g and abs(wt_g - 31.1035) > 2:
            result["product_note"] = f"{wt_g:.0f}g bar, price shown per-oz (ex. DE 19% MwSt)"
        return result

    except Exception as e:
        result["error"] = str(e)[:120]
        return result


def _load_shfe() -> Optional[dict]:
    """Fetch live SHFE silver front-month settlement price.

    Tries today + the previous 4 calendar days (SHFE is closed weekends/holidays).
    Falls back to the local JSON cache if all live attempts fail.
    """
    def _build_result(contracts, trade_date, cny_usd, source_label):
        active = [c for c in contracts if c.get("open_interest", 0) > 0] or contracts
        if not active:
            return None
        front = sorted(active, key=lambda c: c.get("delivery_month", "9999"))[0]
        rmb_kg = front.get("settlement_price_rmb_kg")
        usd_oz = front.get("settlement_price_usd_oz")
        # Recompute usd_oz if cny_usd available but field is missing
        if not usd_oz and rmb_kg and cny_usd:
            TROY_OZ_PER_KG = 32.1507
            CHINA_VAT = 0.13
            usd_oz = round((rmb_kg / TROY_OZ_PER_KG / cny_usd) / (1 + CHINA_VAT), 4)
        return {
            "dealer": "SHFE",
            "region": "China",
            "currency": "CNY",
            "product": f"Front-month {front.get('delivery_month', '')} settlement",
            "source": source_label,
            "trade_date": trade_date,
            "ask_price_local": rmb_kg,
            "ask_price_label": f"¥{rmb_kg:,}/kg" if rmb_kg else "N/A",
            "ask_price_usd_oz": usd_oz,
            "cny_usd": cny_usd,
        }

    # ── 1. Try live fetch ──────────────────────────────────────────
    try:
        import sys as _sys
        _sys.path.insert(0, SCRIPT_DIR)
        from fetch_shfe_silver import (
            fetch_daily_trading, extract_silver_contracts, fetch_cny_usd
        )

        cny_usd = fetch_cny_usd()
        today = datetime.now(timezone.utc)
        for days_back in range(5):          # try today and 4 previous days
            d = today - __import__('datetime').timedelta(days=days_back)
            date_str = d.strftime("%Y%m%d")
            kx = fetch_daily_trading(date_str)
            if not kx:
                continue
            contracts = extract_silver_contracts(kx, cny_usd)
            if contracts:
                result = _build_result(contracts, date_str, cny_usd, "shfe.com.cn (live)")
                if result:
                    print(f"  SHFE live: {date_str} – ¥{result['ask_price_local']:,}/kg")
                    return result
    except Exception as e:
        print(f"  [SHFE live] {e} — falling back to cache", file=sys.stderr)

    # ── 2. Fall back to local JSON cache ──────────────────────────
    files = sorted(glob.glob(os.path.join(CACHE_DIR, "shfe_silver_????????.json")))
    if not files:
        return None
    try:
        with open(files[-1]) as f:
            d = json.load(f)
        contracts = d.get("contracts", [])
        result = _build_result(contracts, d.get("trade_date"), d.get("cny_usd"), "shfe.com.cn (cache)")
        return result
    except Exception as e:
        print(f"  [SHFE cache] {e}", file=sys.stderr)
        return None


def _to_usd_oz(result: dict, fx_cache: dict) -> Optional[float]:
    """Convert dealer ask price to USD/oz."""
    if result.get("ask_price_usd_oz"):
        return result["ask_price_usd_oz"]
    price = result.get("ask_price_local")
    if not price:
        return None
    curr = result.get("currency", "USD")
    if curr == "USD":
        return price
    # For CNY/kg → USD/oz handled by SHFE already
    if curr == "CNY" and result.get("dealer") == "SHFE":
        return None  # already has ask_price_usd_oz
    fx = fx_cache.get(curr) or _fx_to_usd(curr, fx_cache)
    if fx:
        return round(price / fx, 4)
    return None


# ─────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────

def print_report(results: list[dict], lbma_spot_usd: Optional[float]) -> None:
    N = 94
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    def W(): return "═" * N
    def row(text): return f"║  {text:<{N-2}}║"

    print()
    print(f"╔{W()}╗")
    print(f"║  {'FYSISKA HANDLARPRISER — Lokal marknadsdata (1oz silver bar)':^{N-2}}║")
    print(f"║  {ts:^{N-2}}║")
    if lbma_spot_usd:
        print(f"║  {'LBMA spot ref: $' + f'{lbma_spot_usd:.3f}' + '/oz':^{N-2}}║")
    print(f"╠{W()}╣")
    hdr = f"  {'Handlare/Börs':<22} {'Region':<13} {'Lokal pris':<18} {'USD/oz':>9}  {'vs LBMA':>8}  {'Källa'}"
    print(row(hdr))
    print(f"║  {'─'*(N-4)}  ║")

    for r in results:
        dealer  = r.get("dealer", "?")
        region  = r.get("region", "?")
        label   = r.get("ask_price_label") or r.get("error", "N/A")
        usd_oz  = r.get("ask_price_usd_oz")
        usd_str = f"${usd_oz:.3f}" if usd_oz else " N/A  "
        source  = r.get("source", "?")
        prem_str = ""
        if usd_oz and lbma_spot_usd and lbma_spot_usd > 0:
            pct = (usd_oz - lbma_spot_usd) / lbma_spot_usd * 100
            arrow = "▲" if pct > 0.1 else ("▼" if pct < -0.1 else "→")
            prem_str = f"{arrow}{pct:+.1f}%"
        extra = r.get("premium_label","")
        if extra:
            label = f"{label} ({extra})"

        line = f"  {dealer:<22} {region:<13} {label:<18} {usd_str:>9}  {prem_str:>8}  {source}"
        print(row(line))

    print(f"╠{W()}╣")
    if lbma_spot_usd:
        print(row(f"  LBMA spot: ${lbma_spot_usd:.3f}/oz  ({lbma_spot_usd*TROY_OZ_PER_KG:,.0f} USD/kg)"))
    print(f"╚{W()}╝")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

async def _run(args) -> list[dict]:
    from playwright.async_api import async_playwright

    results: list[dict] = []

    # Pre-fetch all FX rates at once via Yahoo Finance
    fx_cache: dict = _fetch_fx_bulk()

    # LBMA spot via goldprice.org
    lbma_usd = None
    try:
        from curl_cffi import requests as cr
        r = cr.get("https://data-asg.goldprice.org/dbXRates/USD",
                   impersonate="chrome120", timeout=10,
                   headers={"Referer": "https://goldprice.org/"})
        if r.ok:
            items = r.json().get("items", [])
            if items:
                lbma_usd = float(items[0].get("xagPrice", 0))
                print(f"  LBMA spot: ${lbma_usd:.3f}/oz")
    except Exception as e:
        print(f"  [LBMA] {e}", file=sys.stderr)

    # SHFE (no Playwright needed)
    shfe = _load_shfe()
    if shfe:
        print(f"  SHFE: {shfe.get('trade_date')} {shfe.get('ask_price_label')} = ${shfe.get('ask_price_usd_oz'):.3f}/oz")
        results.append(shfe)

    # Playwright scrapers
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
            locale="en-GB",
        )
        page = await ctx.new_page()

        scrapers = [
            ("Royal Mint (GBP)",   scrape_royal_mint),
            ("BGASC (USD)",         scrape_bgasc),
            ("ABC Bullion (AUD)",  scrape_abc_bullion),
            ("proaurum (EUR)",      scrape_proaurum),
        ]

        for name, scraper_fn in scrapers:
            print(f"  Scraping {name}…")
            r = await scraper_fn(page)
            # Compute USD/oz
            r["ask_price_usd_oz"] = _to_usd_oz(r, fx_cache)
            if args.verbose:
                print(f"    → {r}")
            results.append(r)
            if "error" in r:
                print(f"    ⚠ {r['error']}", file=sys.stderr)

        await browser.close()

    return results, lbma_usd


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json",    action="store_true", help="Print JSON output")
    parser.add_argument("--save",    action="store_true", help="Save JSON to comex_data/")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    print("Hämtar faktiska handlarpriser med Playwright…")
    results, lbma_usd = asyncio.run(_run(args))

    if args.json:
        print(json.dumps({"lbma_spot_usd_oz": lbma_usd, "dealers": results}, indent=2))
        return 0

    print_report(results, lbma_usd)

    if args.save:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        out_path = os.path.join(CACHE_DIR, f"dealer_prices_{today}.json")
        with open(out_path, "w") as f:
            json.dump({"date": today, "lbma_spot_usd_oz": lbma_usd, "dealers": results}, f, indent=2)
        print(f"\n  Sparat → {out_path}")

    _append_timeseries_csv(results, lbma_usd)
    return 0


DEALER_PRICES_CSV   = os.path.join(CACHE_DIR, "dealer_prices_timeseries.csv")
DEALER_HISTORY_FILE = os.path.join(CACHE_DIR, "dealer_prices_history.json")

# Region → CSV column name
_REGION_COL = {
    "China":     "shfe_usd_oz",
    "UK":        "royal_mint_usd_oz",
    "USA":       "bgasc_usd_oz",
    "Australia": "abc_bullion_usd_oz",
    "EU":        "proaurum_usd_oz",
}

CSV_COLUMNS = ["date", "lbma_spot_usd_oz", "avg_physical_usd_oz"] + list(_REGION_COL.values())


# ---------------------------------------------------------------------------
# History helpers  (mirrors gsr_premiums_history.json pattern)
# ---------------------------------------------------------------------------

def _load_dealer_history() -> list[dict]:
    """Load dealer_prices_history.json; return [] if absent or corrupt."""
    if not os.path.exists(DEALER_HISTORY_FILE):
        return []
    try:
        with open(DEALER_HISTORY_FILE, encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_dealer_history(history: list[dict]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(DEALER_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, ensure_ascii=False)


def _upsert_dealer_history(row: dict) -> list[dict]:
    """Insert or replace the entry for row['date'] in dealer_prices_history.json."""
    history = _load_dealer_history()
    date_key = row["date"]
    history = [h for h in history if h.get("date") != date_key]
    history.append(row)
    history.sort(key=lambda h: h.get("date", ""))
    _save_dealer_history(history)
    return history


def rebuild_dealer_timeseries_csv() -> None:
    """Regenerate dealer_prices_timeseries.csv from dealer_prices_history.json.
    Call this if the CSV is ever lost or corrupted."""
    import csv
    history = _load_dealer_history()
    if not history:
        print("  ⚠  dealer_prices_history.json is empty — nothing to rebuild")
        return
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(DEALER_PRICES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(history)
    print(f"  CSV rebuilt from history ({len(history)} rows) → {DEALER_PRICES_CSV}")


def _append_timeseries_csv(results: list[dict], lbma_usd: Optional[float]) -> None:
    """Append today's dealer prices to dealer_prices_timeseries.csv.
    If a row for today already exists it is overwritten.
    Also upserts into dealer_prices_history.json for long-term reconstruction."""
    import csv

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row: dict = {"date": today, "lbma_spot_usd_oz": round(lbma_usd, 4) if lbma_usd else ""}
    for r in results:
        col = _REGION_COL.get(r.get("region", ""))
        if col:
            val = r.get("ask_price_usd_oz")
            row[col] = round(val, 4) if val else ""

    # Compute average across all physical dealer prices
    dealer_vals = [float(row[c]) for c in _REGION_COL.values() if row.get(c) not in ("", None)]
    row["avg_physical_usd_oz"] = round(sum(dealer_vals) / len(dealer_vals), 4) if dealer_vals else ""

    # Persist to history JSON (source of truth for reconstruction)
    _upsert_dealer_history(row)
    print(f"  History JSON uppdaterad → {DEALER_HISTORY_FILE}")

    # Read existing rows, replace today's if present
    existing: list[dict] = []
    if os.path.exists(DEALER_PRICES_CSV):
        with open(DEALER_PRICES_CSV, newline="") as f:
            existing = [r for r in csv.DictReader(f) if r.get("date") != today]

    existing.append(row)
    existing.sort(key=lambda r: r.get("date", ""))

    with open(DEALER_PRICES_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(existing)
    print(f"  Timeseries CSV uppdaterad → {DEALER_PRICES_CSV}")


if __name__ == "__main__":
    raise SystemExit(main())
