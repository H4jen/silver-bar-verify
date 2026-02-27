#!/usr/bin/env python3
"""
Generates a self-contained HTML gallery (index.html) inside comex_data/plots/
so the plots can be browsed in a browser via a local HTTP server.

Usage:
    python generate_gallery.py
"""

import os
import glob
from datetime import datetime

PLOT_DIR = os.path.join(os.path.dirname(__file__), "comex_data", "plots")

TITLES = {
    "00a_comex_oi_warehouse":          "COMEX – OI vs Warehouse",
    "00b_shfe_oi_warehouse":           "SHFE – OI vs Warehouse",
    "01a_comex_registered_vs_eligible": "COMEX – Registered vs Eligible",
    "01b_shfe_registered_vs_eligible":  "SHFE – Registered vs Eligible",
    "02a_comex_oi_by_month":           "COMEX – OI by Month",
    "02b_shfe_oi_by_month":            "SHFE – OI by Month",
    "03a_comex_oi_timeseries":         "COMEX – OI Time-series",
    "03b_shfe_oi_timeseries":          "SHFE – OI Time-series",
    "04_etc_holdings":                 "ETC Holdings",
    "05_etc_collateral_ratio":         "ETC Collateral Ratio",
    "06_etc_verification_gap":         "ETC Verification Gap",
    "07_oi_warehouse_ratio":           "OI / Warehouse Ratio",
    "08_registered_to_oi":             "Registered-to-OI",
    "09_total_oi_trend":               "Total OI Trend",
    "10_gsr":                          "Gold/Silver Ratio",
    "11_global_premiums":              "Global Premiums",
    "12_premium_pct":                  "Premium %",
}


def make_title(stem: str) -> str:
    return TITLES.get(stem, stem.replace("_", " ").title())


def generate(plot_dir: str = PLOT_DIR) -> str:
    pngs = sorted(glob.glob(os.path.join(plot_dir, "*.png")))
    if not pngs:
        raise FileNotFoundError(f"No PNG files found in {plot_dir}")

    cards = []
    for path in pngs:
        stem = os.path.splitext(os.path.basename(path))[0]
        title = make_title(stem)
        fname = os.path.basename(path)
        cards.append(f"""
      <div class="card">
        <a href="{fname}" target="_blank">
          <img src="{fname}" alt="{title}" loading="lazy">
        </a>
        <p>{title}</p>
      </div>""")

    cards_html = "\n".join(cards)
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Silver Bar Verify – Plots</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: system-ui, sans-serif;
      background: #111;
      color: #e0e0e0;
      padding: 1.5rem;
    }}
    h1 {{
      text-align: center;
      font-size: 1.6rem;
      margin-bottom: 0.3rem;
      color: #fff;
    }}
    .subtitle {{
      text-align: center;
      font-size: 0.8rem;
      color: #888;
      margin-bottom: 1.8rem;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(420px, 1fr));
      gap: 1.2rem;
    }}
    .card {{
      background: #1e1e1e;
      border-radius: 8px;
      overflow: hidden;
      border: 1px solid #333;
      transition: border-color 0.2s;
    }}
    .card:hover {{ border-color: #666; }}
    .card img {{
      width: 100%;
      display: block;
      cursor: zoom-in;
    }}
    .card p {{
      padding: 0.5rem 0.8rem;
      font-size: 0.85rem;
      color: #aaa;
      text-align: center;
    }}
  </style>
</head>
<body>
  <h1>Silver Bar Verify – Plots</h1>
  <p class="subtitle">Generated {generated} &nbsp;·&nbsp; {len(pngs)} charts &nbsp;·&nbsp; click to open full-size</p>
  <div class="grid">
{cards_html}
  </div>
</body>
</html>
"""

    out = os.path.join(plot_dir, "index.html")
    with open(out, "w") as f:
        f.write(html)
    print(f"Gallery written → {out}")
    return out


if __name__ == "__main__":
    generate()
