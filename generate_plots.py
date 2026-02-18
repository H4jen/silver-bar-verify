#!/usr/bin/env python3
"""
Silver Bar Verify – Plot Generator
====================================

Reads all time-series CSVs from ``comex_data/`` and produces plots
saved to ``comex_data/plots/``.

Usage:
    python generate_plots.py            # generate all plots
"""

import os
import sys
from datetime import datetime, date, timedelta

import matplotlib
matplotlib.use("Agg")                       # headless backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import pandas as pd

# ── paths ──────────────────────────────────────────────────────────
DATA_DIR   = os.path.join(os.path.dirname(__file__), "comex_data")
PLOT_DIR   = os.path.join(DATA_DIR, "plots")

COMEX_CSV  = os.path.join(DATA_DIR, "comex_silver_timeseries.csv")
SHFE_CSV   = os.path.join(DATA_DIR, "shfe_silver_timeseries.csv")
ETC_CSV    = os.path.join(DATA_DIR, "silver_etcs_timeseries.csv")

# ── styling ────────────────────────────────────────────────────────
plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.facecolor":   "#f8f8f8",
    "axes.grid":        True,
    "grid.alpha":       0.35,
    "grid.linewidth":   0.6,
    "font.size":        11,
})

COMEX_COLOR_REG  = "#1f77b4"   # blue
COMEX_COLOR_ELIG = "#aec7e8"   # light blue
COMEX_COLOR_COMB = "#2ca02c"   # green
SHFE_COLOR_REG   = "#d62728"   # red
SHFE_COLOR_ELIG  = "#ff9896"   # light red
SHFE_COLOR_COMB  = "#ff7f0e"   # orange


def load_csv(path: str) -> pd.DataFrame | None:
    """Load a CSV, parse the 'date' column, sort by date."""
    if not os.path.isfile(path):
        print(f"  ⚠  {os.path.basename(path)} not found – skipping")
        return None
    df = pd.read_csv(path, parse_dates=["date"])
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


# ── Plot 1: Registered vs Eligible vs Combined ────────────────────
def _plot_warehouse(df: pd.DataFrame, exchange: str,
                    color_reg: str, color_elig: str,
                    color_comb: str, filename: str) -> str:
    """
    Single-exchange warehouse chart.
    X-axis : calendar year starting Jan 1  (1-year window)
    Y-axis : million troy ounces
    Lines  : registered, eligible, combined total
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    year = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    dates = df["date"]
    reg   = df["warehouse_registered_oz"] / 1e6
    elig  = df["warehouse_eligible_oz"]   / 1e6
    comb  = df["warehouse_combined_oz"]   / 1e6

    ax.plot(dates, reg,  color=color_reg,  linewidth=2,
            marker="o", markersize=4, label="Registered")
    ax.plot(dates, elig, color=color_elig, linewidth=2,
            marker="s", markersize=4, label="Eligible")
    ax.plot(dates, comb, color=color_comb, linewidth=2.5,
            marker="D", markersize=4, label="Combined Total",
            linestyle="--")

    # annotate latest values
    for series, color, va in [(reg,  color_reg,  "bottom"),
                               (elig, color_elig, "top"),
                               (comb, color_comb, "bottom")]:
        last_val = series.iloc[-1]
        last_dt  = dates.iloc[-1]
        ax.annotate(f"{last_val:,.1f}M",
                    xy=(last_dt, last_val),
                    textcoords="offset points", xytext=(8, 0),
                    fontsize=9, color=color, fontweight="bold",
                    va=va)

    # ── axes formatting ──
    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    fig.autofmt_xdate(rotation=0, ha="center")

    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v:,.0f}"))
    ax.set_ylabel("Million Troy Ounces")
    ax.set_xlabel(str(year))

    ax.set_title(f"{exchange} Silver Warehouse Stocks – Registered vs Eligible (Plot 1)",
                 fontsize=14, fontweight="bold", pad=12)
    ax.legend(loc="lower right", framealpha=0.9)

    fig.tight_layout()
    out = os.path.join(PLOT_DIR, filename)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def plot_registered_vs_eligible(comex: pd.DataFrame | None,
                                 shfe: pd.DataFrame | None) -> list[str]:
    """Generate one chart per exchange: registered, eligible, combined."""
    saved = []

    if comex is not None and not comex.empty:
        out = _plot_warehouse(comex, "COMEX",
                              COMEX_COLOR_REG, COMEX_COLOR_ELIG,
                              COMEX_COLOR_COMB,
                              "01a_comex_registered_vs_eligible.png")
        saved.append(out)

    if shfe is not None and not shfe.empty:
        out = _plot_warehouse(shfe, "SHFE",
                              SHFE_COLOR_REG, SHFE_COLOR_ELIG,
                              SHFE_COLOR_COMB,
                              "01b_shfe_registered_vs_eligible.png")
        saved.append(out)

    if not saved:
        print("  ⚠  No warehouse data to plot")

    return saved


# ── Plot 2: OI by Contract Month ──────────────────────────────────

# palette for up to 12 contract months
_MONTH_COLORS = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b",
    "#e377c2", "#7f7f7f", "#bcbd22", "#17becf", "#aec7e8", "#ffbb78",
]


def _pivot_month_oi(df: pd.DataFrame, label_prefix: str,
                    contracts_prefix: str, n_slots: int,
                    oz_per_contract: float) -> pd.DataFrame:
    """
    Pivot the ranked/numbered month-OI columns into a tidy DataFrame:
        date | month_label | oi_oz
    then pivot wide so each unique month_label is a column of oi_oz.
    """
    rows = []
    for _, row in df.iterrows():
        dt = row["date"]
        for i in range(1, n_slots + 1):
            label = row.get(f"{label_prefix}{i}_label",
                            row.get(f"{label_prefix}{i}_month"))
            contracts = row.get(f"{contracts_prefix}{i}_contracts", 0)
            if pd.isna(label) or pd.isna(contracts):
                continue
            rows.append({"date": dt, "month": label,
                         "oi_oz": float(contracts) * oz_per_contract})
    tidy = pd.DataFrame(rows)
    if tidy.empty:
        return tidy
    wide = tidy.pivot_table(index="date", columns="month",
                            values="oi_oz", aggfunc="sum").fillna(0)
    # sort columns chronologically (e.g. "MAR 26", "APR 26", …)
    month_order = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
                   "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
                   "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
    def _sort_key(col):
        parts = col.split()
        m = month_order.get(parts[0], 0) if parts else 0
        y = int(parts[1]) if len(parts) > 1 else 0
        return (y, m)
    wide = wide[sorted(wide.columns, key=_sort_key)]
    wide.index = pd.to_datetime(wide.index)
    return wide


def _plot_oi_by_month(df: pd.DataFrame, exchange: str,
                      label_prefix: str, contracts_prefix: str,
                      n_slots: int, oz_per_contract: float,
                      wh_col: str, wh_reg_col: str, wh_elig_col: str,
                      filename: str, latest_only: bool = False) -> str:
    """
    Grouped bar chart – one bar per contract month, grouped by date.
    X-axis : contract month labels
    Y-axis : million troy ounces
    Horizontal dashed lines for warehouse registered, eligible, combined.
    """
    import numpy as np

    # Use the latest row to get the current snapshot of OI per month
    # (for time-series with many dates we show bars per date, grouped by month)
    wide = _pivot_month_oi(df, label_prefix, contracts_prefix,
                           n_slots, oz_per_contract)
    if wide.empty:
        return ""

    # restrict to latest date if requested
    if latest_only and len(wide) > 1:
        wide = wide.iloc[[-1]]

    fig, ax = plt.subplots(figsize=(14, 6))

    months = wide.columns.tolist()
    n_months = len(months)
    n_dates  = len(wide)
    colors = _MONTH_COLORS[:n_months]

    if n_dates == 1:
        # single date → simple bar chart, one bar per month
        vals = wide.iloc[0] / 1e6
        x = np.arange(n_months)
        bars = ax.bar(x, vals, color=colors, width=0.6, edgecolor="white")
        ax.set_xticks(x)
        ax.set_xticklabels(months, fontsize=10)

        # value labels on bars
        for bar, v in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    f"{v:,.0f}M", ha="center", va="bottom",
                    fontsize=9, fontweight="bold")

        date_label = wide.index[0].strftime("%Y-%m-%d")
        subtitle = f"as of {date_label}"
    else:
        # multiple dates → grouped bars (one group per month)
        x = np.arange(n_months)
        bar_width = 0.8 / n_dates
        date_labels = []

        for i, (dt, row) in enumerate(wide.iterrows()):
            vals = row / 1e6
            offset = (i - (n_dates - 1) / 2) * bar_width
            date_str = dt.strftime("%m/%d")
            date_labels.append(date_str)
            bars = ax.bar(x + offset, vals, width=bar_width,
                          label=date_str, alpha=0.85, edgecolor="white")
            # value labels on the last date's bars
            if i == n_dates - 1:
                for bar, v in zip(bars, vals):
                    ax.text(bar.get_x() + bar.get_width() / 2,
                            bar.get_height(),
                            f"{v:,.0f}M", ha="center", va="bottom",
                            fontsize=8, fontweight="bold")

        ax.set_xticks(x)
        ax.set_xticklabels(months, fontsize=10)
        subtitle = f"{date_labels[0]} – {date_labels[-1]}"

    # warehouse reference lines (latest values)
    wh_latest   = df[wh_col].iloc[-1]      / 1e6
    wh_reg      = df[wh_reg_col].iloc[-1]  / 1e6
    wh_elig     = df[wh_elig_col].iloc[-1] / 1e6

    ax.axhline(y=wh_latest, color="black",    linewidth=2,   linestyle="--",
               label=f"Warehouse Combined ({wh_latest:,.0f}M)")
    ax.axhline(y=wh_reg,    color="#1f77b4",  linewidth=1.8, linestyle=":",
               label=f"Warehouse Registered ({wh_reg:,.0f}M)")
    ax.axhline(y=wh_elig,   color="#ff7f0e",  linewidth=1.8, linestyle="-.",
               label=f"Warehouse Eligible ({wh_elig:,.0f}M)")

    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v:,.0f}"))
    ax.set_ylabel("Million Troy Ounces")
    ax.set_xlabel("Contract Month")

    ax.set_title(
        f"{exchange} Silver – Open Interest by Contract Month (Plot 2)\n"
        f"{subtitle}",
        fontsize=14, fontweight="bold", pad=12)
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)

    fig.tight_layout()
    out = os.path.join(PLOT_DIR, filename)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def plot_oi_vs_warehouse(comex: pd.DataFrame | None,
                          shfe: pd.DataFrame | None) -> list[str]:
    """Generate OI-by-month chart for each exchange."""
    saved = []

    if comex is not None and not comex.empty:
        out = _plot_oi_by_month(
            comex, "COMEX",
            label_prefix="oi_month_",
            contracts_prefix="oi_month_",
            n_slots=6,
            oz_per_contract=5000,        # COMEX silver = 5 000 oz / contract
            wh_col="warehouse_combined_oz",
            wh_reg_col="warehouse_registered_oz",
            wh_elig_col="warehouse_eligible_oz",
            filename="02a_comex_oi_by_month.png",
            latest_only=True,
        )
        if out:
            saved.append(out)

    if shfe is not None and not shfe.empty:
        out = _plot_oi_by_month(
            shfe, "SHFE",
            label_prefix="oi_rank_",
            contracts_prefix="oi_rank_",
            n_slots=6,
            oz_per_contract=482.26,      # SHFE silver = 15 kg ≈ 482.26 oz / contract
            wh_col="warehouse_combined_oz",
            wh_reg_col="warehouse_registered_oz",
            wh_elig_col="warehouse_eligible_oz",
            filename="02b_shfe_oi_by_month.png",
            latest_only=True,
        )
        if out:
            saved.append(out)

    if not saved:
        print("  ⚠  No OI data to plot")

    return saved


# ── Plot 3: OI per contract month over time ────────────────────────

# short-name → month number for delivery columns
_MON_NUM = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
            "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}


def _plot_oi_timeseries(df: pd.DataFrame, exchange: str,
                        label_prefix: str, contracts_prefix: str,
                        n_slots: int, oz_per_contract: float,
                        filename: str,
                        deliveries_oz_per_contract: float | None = None) -> str:
    """
    One line per contract month showing how its OI changes over time.
    X-axis : first day of current month → +6 months
    Y-axis (left)  : million troy ounces (OI lines)
    Y-axis (right) : million troy ounces (delivered, bar per month, COMEX only)
    """
    import numpy as np
    from dateutil.relativedelta import relativedelta

    wide = _pivot_month_oi(df, label_prefix, contracts_prefix,
                           n_slots, oz_per_contract)
    if wide.empty:
        return ""

    today   = date.today()
    x_start = datetime(today.year, today.month, 1)
    x_end   = x_start + relativedelta(months=6)

    fig, ax = plt.subplots(figsize=(14, 6))

    cols   = wide.columns.tolist()
    colors = _MONTH_COLORS[:len(cols)]

    for col, color in zip(cols, colors):
        vals = wide[col] / 1e6
        ax.plot(wide.index, vals, color=color, linewidth=2,
                marker="o", markersize=5, label=col)
        last_val = vals.iloc[-1]
        ax.annotate(f"{col}  {last_val:,.0f}M",
                    xy=(wide.index[-1], last_val),
                    textcoords="offset points", xytext=(8, 0),
                    fontsize=8, color=color, fontweight="bold",
                    va="center")

    # ── Delivered silver line (COMEX only) ──────────────────────────
    if deliveries_oz_per_contract is not None:
        delivery_cols = {
            "Current-month delivered": "current_month_delivered_oz",
        }
        delivery_colors = ["#9467bd"]
        ax2 = ax.twinx()
        plotted_any = False
        for (label, col), dcolor in zip(delivery_cols.items(), delivery_colors):
            if col not in df.columns:
                continue
            vals = df[col].dropna() / 1e6
            dates = df.loc[vals.index, "date"]
            if vals.empty:
                continue
            ax2.plot(dates, vals, color=dcolor, linewidth=2,
                     linestyle="--", marker="^", markersize=5, label=label)
            ax2.annotate(f"{vals.iloc[-1]:,.0f}M",
                         xy=(dates.iloc[-1], vals.iloc[-1]),
                         textcoords="offset points", xytext=(8, 0),
                         fontsize=8, color=dcolor, fontweight="bold",
                         va="center")
            plotted_any = True

        if plotted_any:
            ax2.set_ylabel("Delivered  (Million Troy Ounces)", color="#9467bd")
            ax2.tick_params(axis="y", labelcolor="#9467bd")
            ax2.set_ylim(bottom=0)
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
                lambda v, _: f"{v:,.0f}"))
            h2, l2 = ax2.get_legend_handles_labels()
            h1, l1 = ax.get_legend_handles_labels()
            ax.legend(h1 + h2, l1 + l2,
                      loc="lower right", framealpha=0.9, ncol=2, fontsize=9)
            ax2.set_xlim(x_start, x_end)
        else:
            ax.legend(loc="lower right", framealpha=0.9, ncol=2, fontsize=9)
    else:
        ax.legend(loc="lower right", framealpha=0.9, ncol=2, fontsize=9)

    ax.set_xlim(x_start, x_end)
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    fig.autofmt_xdate(rotation=0, ha="center")

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v:,.0f}"))
    ax.set_ylabel("Million Troy Ounces (OI)")
    ax.set_xlabel(f"{x_start.strftime('%b %Y')} – {x_end.strftime('%b %Y')}")

    ax.set_title(f"{exchange} Silver – Open Interest per Contract Month Over Time (Plot 3)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.tight_layout()
    out = os.path.join(PLOT_DIR, filename)
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


def plot_oi_timeseries(comex: pd.DataFrame | None,
                       shfe: pd.DataFrame | None) -> list[str]:
    """Generate OI-per-month time-series chart for each exchange."""
    saved = []

    if comex is not None and not comex.empty:
        out = _plot_oi_timeseries(
            comex, "COMEX",
            label_prefix="oi_month_",
            contracts_prefix="oi_month_",
            n_slots=6,
            oz_per_contract=5000,
            filename="03a_comex_oi_timeseries.png",
            deliveries_oz_per_contract=5000,   # COMEX: 5 000 oz / contract
        )
        if out:
            saved.append(out)

    if shfe is not None and not shfe.empty:
        out = _plot_oi_timeseries(
            shfe, "SHFE",
            label_prefix="oi_rank_",
            contracts_prefix="oi_rank_",
            n_slots=6,
            oz_per_contract=482.26,
            filename="03b_shfe_oi_timeseries.png",
            deliveries_oz_per_contract=None,   # no delivery data for SHFE
        )
        if out:
            saved.append(out)

    if not saved:
        print("  ⚠  No OI time-series data to plot")

    return saved


# ── ETC helpers ────────────────────────────────────────────────────
ETC_FUND_COLORS = {"invesco": "#1f77b4", "wisdomtree": "#d62728"}
ETC_FUND_MARKERS = {"invesco": "o", "wisdomtree": "s"}

def _etc_axis_setup(ax, year, x_start, x_end):
    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))

def _annotate_last(ax, dates, vals, color, fmt="{:.0f}"):
    ax.annotate(fmt.format(vals.iloc[-1]),
                xy=(dates.iloc[-1], vals.iloc[-1]),
                textcoords="offset points", xytext=(8, 0),
                fontsize=9, color=color, fontweight="bold", va="center")


# ── Plot 4: Physical oz & Bar Count ───────────────────────────────
def plot_etc_holdings(etc: pd.DataFrame) -> str:
    """WisdomTree bar count (left) and Invesco bar count (right) over time."""
    year    = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax2 = ax.twinx()

    # ── WisdomTree – left axis ──
    grp    = etc[etc["fund"] == "wisdomtree"]
    color  = ETC_FUND_COLORS["wisdomtree"]
    marker = ETC_FUND_MARKERS["wisdomtree"]
    dates  = grp["date"]
    bc     = grp["bar_count"]
    ax.plot(dates, bc, color=color, linewidth=2,
            marker=marker, markersize=5, label="WisdomTree Bar Count")
    _annotate_last(ax, dates, bc, color, "{:,.0f}")

    # ── Invesco – right axis ──
    grp_i   = etc[etc["fund"] == "invesco"]
    color_i = ETC_FUND_COLORS["invesco"]
    marker_i = ETC_FUND_MARKERS["invesco"]
    dates_i = grp_i["date"]
    bc_i    = grp_i["bar_count"]
    ax2.plot(dates_i, bc_i, color=color_i, linewidth=2,
             marker=marker_i, markersize=5, label="Invesco Bar Count")
    _annotate_last(ax2, dates_i, bc_i, color_i, "{:,.0f}")

    _etc_axis_setup(ax, year, x_start, x_end)
    ax.set_ylim(46000, 48000)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_ylabel("WisdomTree Bar Count", color=color)
    ax.tick_params(axis="y", labelcolor=color)

    ax2.set_ylim(14000, 16000)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax2.set_ylabel("Invesco Bar Count", color=color_i)
    ax2.tick_params(axis="y", labelcolor=color_i)

    ax.set_xlabel(str(year))

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="lower right", framealpha=0.9, fontsize=9)
    ax.set_title("Silver ETC Bar Count – WisdomTree & Invesco (Plot 4)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "04_etc_holdings.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 5: Daily Bar Delta ────────────────────────────────────────
def plot_etc_collateral(etc: pd.DataFrame) -> str:
    """Scatter plot of bars added (positive) and removed (negative) per day."""
    year    = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    fig, ax = plt.subplots(figsize=(14, 5))

    ax.axhline(0, color="black", linewidth=0.9, linestyle="-")

    for fund, grp in etc.groupby("fund"):
        color  = ETC_FUND_COLORS.get(fund, "#7f7f7f")
        marker = ETC_FUND_MARKERS.get(fund, "o")
        dates  = grp["date"]

        added   = grp["delta_bars_added"].fillna(0)
        removed = grp["delta_bars_removed"].fillna(0)

        # plot added as positive, removed as negative
        mask_add = added != 0
        mask_rem = removed != 0

        ax.scatter(dates[mask_add],  added[mask_add],
                   color=color, marker="^", s=80, zorder=3,
                   label=f"{fund.title()} Added")
        ax.scatter(dates[mask_rem], -removed[mask_rem],
                   color=color, marker="v", s=80, zorder=3, alpha=0.6,
                   label=f"{fund.title()} Removed")

        # annotate non-zero points
        for dt, v in zip(dates[mask_add], added[mask_add]):
            ax.annotate(f"+{v:.0f}", xy=(dt, v),
                        textcoords="offset points", xytext=(0, 6),
                        fontsize=8, color=color, ha="center")
        for dt, v in zip(dates[mask_rem], removed[mask_rem]):
            ax.annotate(f"-{v:.0f}", xy=(dt, -v),
                        textcoords="offset points", xytext=(0, -12),
                        fontsize=8, color=color, ha="center")

    ax.set_ylim(-500, 500)
    _etc_axis_setup(ax, year, x_start, x_end)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+,.0f}"))
    ax.set_ylabel("Bar Count Delta  (↑ added  /  ↓ removed)")
    ax.set_xlabel(str(year))
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title("Silver ETC – Daily Bar Count Changes (Plot 5)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "05_etc_collateral_ratio.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 6: Verification Integrity – difference_pct per fund ──────
def plot_verification_gap(etc: pd.DataFrame) -> str:
    """
    difference_pct (expected oz vs physical oz gap) over time.
    WisdomTree on left Y-axis (2% – 4%), Invesco on right Y-axis (-1% – +1%).
    """
    today   = date.today()
    year    = today.year
    # start of previous month
    if today.month > 1:
        x_start = datetime(year, today.month - 1, 1)
    else:
        x_start = datetime(year - 1, 12, 1)
    # end of current month (first day of next month minus one day)
    if today.month < 12:
        x_end = datetime(year, today.month + 1, 1) - timedelta(days=1)
    else:
        x_end = datetime(year + 1, 1, 1) - timedelta(days=1)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax2 = ax.twinx()

    # zero reference lines on both axes
    ax.axhline(0,  color=ETC_FUND_COLORS["wisdomtree"], linewidth=0.7,
               linestyle="--", zorder=2, alpha=0.5)
    ax2.axhline(0, color=ETC_FUND_COLORS["invesco"],    linewidth=0.7,
                linestyle="--", zorder=2, alpha=0.5)

    # ── WisdomTree – left axis ──
    wt = etc[etc["fund"] == "wisdomtree"]
    if not wt.empty:
        color  = ETC_FUND_COLORS["wisdomtree"]
        marker = ETC_FUND_MARKERS["wisdomtree"]
        dates  = wt["date"]
        gap    = wt["difference_pct"]
        ax.plot(dates, gap, color=color, linewidth=2,
                marker=marker, markersize=6, label="WisdomTree", zorder=3)
        for dt, v in zip(dates, gap):
            offset = (0, 9) if v >= 0 else (0, -12)
            ax.annotate(f"{v:+.3f}%", xy=(dt, v),
                        textcoords="offset points", xytext=offset,
                        fontsize=8, color=color, ha="center")

    # ── Invesco – right axis ──
    iv = etc[etc["fund"] == "invesco"]
    if not iv.empty:
        color_i  = ETC_FUND_COLORS["invesco"]
        marker_i = ETC_FUND_MARKERS["invesco"]
        dates_i  = iv["date"]
        gap_i    = iv["difference_pct"]
        ax2.plot(dates_i, gap_i, color=color_i, linewidth=2,
                 marker=marker_i, markersize=6, label="Invesco", zorder=3)
        for dt, v in zip(dates_i, gap_i):
            offset = (0, 9) if v >= 0 else (0, -12)
            ax2.annotate(f"{v:+.3f}%", xy=(dt, v),
                         textcoords="offset points", xytext=offset,
                         fontsize=8, color=color_i, ha="center")

    _etc_axis_setup(ax, year, x_start, x_end)
    # narrow window: weekly major ticks, daily minor
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    ax.xaxis.set_minor_locator(mdates.DayLocator())

    ax.set_ylim(2.0, 4.0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.2f}%"))
    ax.set_ylabel("WisdomTree  difference_pct", color=ETC_FUND_COLORS["wisdomtree"])
    ax.tick_params(axis="y", labelcolor=ETC_FUND_COLORS["wisdomtree"])

    ax2.set_ylim(-1.0, 1.0)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:+.2f}%"))
    ax2.set_ylabel("Invesco  difference_pct", color=ETC_FUND_COLORS["invesco"])
    ax2.tick_params(axis="y", labelcolor=ETC_FUND_COLORS["invesco"])

    ax.set_xlabel(str(year))

    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title("Silver ETC – Verification Integrity: Expected vs Physical Gap (Plot 6)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "06_etc_verification_gap.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 7: OI-to-Warehouse Ratio ─────────────────────────────────
def plot_oi_warehouse_ratio(comex: pd.DataFrame, shfe: pd.DataFrame) -> str:
    """
    all_oi_oz / warehouse_combined_oz for COMEX and SHFE.
    A ratio >1.0 means open interest exceeds warehouse stocks – stress signal.
    """
    year    = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    fig, ax = plt.subplots(figsize=(14, 6))

    # stress line at 1.0
    ax.axhline(1.0, color="#d62728", linewidth=1.2, linestyle="--",
               zorder=2, label="Stress threshold (1.0×)")

    if comex is not None and not comex.empty:
        dates = comex["date"]
        ratio = comex["all_oi_oz"] / comex["warehouse_combined_oz"]
        ax.plot(dates, ratio, color=COMEX_COLOR_REG, linewidth=2,
                marker="o", markersize=6, label="COMEX", zorder=3)

    if shfe is not None and not shfe.empty:
        dates = shfe["date"]
        ratio = shfe["total_oi_oz"] / shfe["warehouse_combined_oz"]
        ax.plot(dates, ratio, color=SHFE_COLOR_REG, linewidth=2,
                marker="s", markersize=6, label="SHFE", zorder=3)

    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}×"))
    ax.set_ylabel("OI / Warehouse Combined  (×)")
    ax.set_xlabel(str(year))
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title("COMEX & SHFE – OI-to-Warehouse Ratio (Plot 7)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "07_oi_warehouse_ratio.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 8: Registered Silver / OI ────────────────────────────────
def plot_registered_to_oi(comex: pd.DataFrame, shfe: pd.DataFrame) -> str:
    """
    warehouse_registered_oz / OI for COMEX and SHFE.
    A ratio of 1.0 means registered silver fully covers open interest.
    Y-axis: 0 → 1×
    """
    year    = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    fig, ax = plt.subplots(figsize=(14, 6))

    # full-coverage line at 1.0
    ax.axhline(1.0, color="#2ca02c", linewidth=1.2, linestyle="--",
               zorder=2, label="Full coverage (1.0×)")

    if comex is not None and not comex.empty:
        dates = comex["date"]
        next2_oi_oz = (comex["oi_month_1_contracts"] + comex["oi_month_2_contracts"]) * 5000
        ratio = comex["warehouse_registered_oz"] / next2_oi_oz
        ax.plot(dates, ratio, color=COMEX_COLOR_REG, linewidth=2,
                marker="o", markersize=6, label="COMEX", zorder=3)

    if shfe is not None and not shfe.empty:
        dates = shfe["date"]
        next2_oi_oz = (shfe["oi_rank_1_contracts"] + shfe["oi_rank_2_contracts"]) * 482.26
        ratio = shfe["warehouse_registered_oz"] / next2_oi_oz
        ax.plot(dates, ratio, color=SHFE_COLOR_REG, linewidth=2,
                marker="s", markersize=6, label="SHFE", zorder=3)

    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.set_ylim(0, 1.0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}×"))
    ax.set_ylabel("Registered Silver / Next-2-Month OI  (×)")
    ax.set_xlabel(str(year))
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title("COMEX & SHFE – Registered Silver Coverage of Next-2-Month OI (Plot 8)",
                 fontsize=14, fontweight="bold", pad=12)

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "08_registered_to_oi.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 9: Deliveries, OI & Warehouse – Current Month ──────────────
def _lookup_oi_by_month(df: pd.DataFrame, mon_abbr: str, yr2: str) -> pd.Series:
    """Return a Series of OI (oz) for the contract matching mon_abbr + yr2 (e.g. 'MAR', '26').
    Scans oi_month_1..6 label columns per row and sums matching contracts × 5000."""
    target = f"{mon_abbr.upper()} {yr2}"
    n_ranks = 6
    label_cols    = [f"oi_month_{i}_label"     for i in range(1, n_ranks + 1)]
    contract_cols = [f"oi_month_{i}_contracts" for i in range(1, n_ranks + 1)]
    result = pd.Series(0.0, index=df.index)
    for lc, cc in zip(label_cols, contract_cols):
        if lc in df.columns and cc in df.columns:
            matched = df[lc].str.upper().str.strip() == target
            result += matched * pd.to_numeric(df[cc], errors="coerce").fillna(0) * 5000
    return result


def plot_deliveries_oi_warehouse(comex: pd.DataFrame) -> str:
    """
    Combined intra-month view for COMEX (current calendar month):
      • Registered silver in warehouse  (blue)
      • YTD cumulative deliveries        (green)
      • OI for next calendar month       (orange, dashed)
      • OI for month after that          (red, dashed)
    X-axis : day 1 → last day of current month
    Y-axis : million troy ounces
    """
    import calendar
    today   = date.today()
    year    = today.year
    month   = today.month
    last_day = calendar.monthrange(year, month)[1]
    x_start = datetime(year, month, 1)
    x_end   = datetime(year, month, last_day)

    # Coming two months (next month, month after)
    m1_date = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1)
    m2_date = (m1_date + timedelta(days=32)).replace(day=1)
    m1_abbr, m1_yr2 = m1_date.strftime("%b").upper(), m1_date.strftime("%y")
    m2_abbr, m2_yr2 = m2_date.strftime("%b").upper(), m2_date.strftime("%y")
    lbl1 = f"{m1_abbr} {m1_yr2}"
    lbl2 = f"{m2_abbr} {m2_yr2}"

    fig, ax = plt.subplots(figsize=(14, 6))
    lines2, labels2 = [], []  # right-axis legend entries (populated below)

    if comex is not None and not comex.empty:
        # Restrict to current month
        mask = (comex["date"].dt.year == year) & (comex["date"].dt.month == month)
        df = comex[mask].copy()

        if not df.empty:
            dates = df["date"]

            # Warehouse registered (left axis)
            reg_moz = df["warehouse_registered_oz"] / 1e6
            ax.plot(dates, reg_moz, color=COMEX_COLOR_REG, linewidth=2.5,
                    marker="o", markersize=6, label="Registered", zorder=4)

            # Warehouse eligible (left axis)
            elig_moz = df["warehouse_eligible_oz"] / 1e6
            ax.plot(dates, elig_moz, color=COMEX_COLOR_ELIG, linewidth=2.5,
                    marker="D", markersize=5, label="Eligible", zorder=4)

            # OI summed for the coming two months (left axis)
            oi_combined_moz = (
                _lookup_oi_by_month(df, m1_abbr, m1_yr2) +
                _lookup_oi_by_month(df, m2_abbr, m2_yr2)
            ) / 1e6
            ax.plot(dates, oi_combined_moz, color="#ff7f0e", linewidth=2,
                    linestyle="--", marker="^", markersize=5,
                    label=f"OI {lbl1} + {lbl2}", zorder=3)

            # Cumulative deliveries (right axis)
            ax2 = ax.twinx()
            del_moz = df["current_month_delivered_oz"] / 1e6
            ax2.plot(dates, del_moz, color=COMEX_COLOR_COMB, linewidth=2.5,
                     marker="s", markersize=6, label="Delivered (MTD)", zorder=4)
            ax2.set_ylim(0, 80)
            ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
            ax2.set_ylabel("Delivered MTD  (Million Troy Oz)", color=COMEX_COLOR_COMB)
            ax2.tick_params(axis="y", labelcolor=COMEX_COLOR_COMB)
            # Merge legends from both axes
            lines2, labels2 = ax2.get_legend_handles_labels()

    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_minor_locator(mdates.DayLocator())
    ax.set_ylim(0, 400)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax.set_ylabel("Million Troy Oz")
    month_name = datetime(year, month, 1).strftime("%B %Y")
    ax.set_xlabel(month_name)
    lines1, labels1 = ax.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", framealpha=0.9, fontsize=9)
    ax.set_title(
        f"COMEX – Deliveries, OI & Warehouse Inventory  ({month_name})  (Plot 0)",
        fontsize=14, fontweight="bold", pad=12,
    )

    fig.autofmt_xdate(rotation=30, ha="right")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "00a_comex_oi_warehouse.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 0b: SHFE OI & Warehouse – Current Month ────────────────────
SHFE_AG_CONTRACT_OZ = 15 * 32.15074657   # 15 kg per contract → ~482.26 troy oz


def _lookup_shfe_oi_by_month(df: pd.DataFrame, mon_abbr: str, yr2: str) -> pd.Series:
    """Return a Series of OI (oz) for the SHFE contract matching mon_abbr + yr2.
    Scans oi_rank_1..6_month columns and multiplies matching contracts × 482.26 oz."""
    target = f"{mon_abbr.upper()} {yr2}"
    n_ranks = 6
    month_cols    = [f"oi_rank_{i}_month"     for i in range(1, n_ranks + 1)]
    contract_cols = [f"oi_rank_{i}_contracts" for i in range(1, n_ranks + 1)]
    result = pd.Series(0.0, index=df.index)
    for mc, cc in zip(month_cols, contract_cols):
        if mc in df.columns and cc in df.columns:
            matched = df[mc].str.upper().str.strip() == target
            result += matched * pd.to_numeric(df[cc], errors="coerce").fillna(0) * SHFE_AG_CONTRACT_OZ
    return result


def plot_shfe_oi_warehouse(shfe: pd.DataFrame) -> str:
    """
    Combined intra-month view for SHFE (current calendar month):
      • Registered silver in warehouse  (red)
      • Eligible silver in warehouse    (light red)
      • OI summed for coming two months (orange, dashed)
    X-axis : day 1 → last day of current month
    Y-axis : million troy ounces
    """
    import calendar
    today    = date.today()
    year     = today.year
    month    = today.month
    last_day = calendar.monthrange(year, month)[1]
    x_start  = datetime(year, month, 1)
    x_end    = datetime(year, month, last_day)

    # Coming two months
    m1_date = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1)
    m2_date = (m1_date + timedelta(days=32)).replace(day=1)
    m1_abbr, m1_yr2 = m1_date.strftime("%b").upper(), m1_date.strftime("%y")
    m2_abbr, m2_yr2 = m2_date.strftime("%b").upper(), m2_date.strftime("%y")
    lbl1 = f"{m1_abbr} {m1_yr2}"
    lbl2 = f"{m2_abbr} {m2_yr2}"

    fig, ax = plt.subplots(figsize=(14, 6))

    if shfe is not None and not shfe.empty:
        mask = (shfe["date"].dt.year == year) & (shfe["date"].dt.month == month)
        df = shfe[mask].copy()

        if not df.empty:
            dates = df["date"]

            # Warehouse registered
            reg_moz = df["warehouse_registered_oz"] / 1e6
            ax.plot(dates, reg_moz, color=SHFE_COLOR_REG, linewidth=2.5,
                    marker="o", markersize=6, label="Registered", zorder=4)

            # Warehouse eligible
            elig_moz = df["warehouse_eligible_oz"] / 1e6
            ax.plot(dates, elig_moz, color=SHFE_COLOR_ELIG, linewidth=2.5,
                    marker="D", markersize=5, label="Eligible", zorder=4)

            # OI summed for coming two months
            oi_combined_moz = (
                _lookup_shfe_oi_by_month(df, m1_abbr, m1_yr2) +
                _lookup_shfe_oi_by_month(df, m2_abbr, m2_yr2)
            ) / 1e6
            ax.plot(dates, oi_combined_moz, color="#ff7f0e", linewidth=2,
                    linestyle="--", marker="^", markersize=5,
                    label=f"OI {lbl1} + {lbl2}", zorder=3)

    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_minor_locator(mdates.DayLocator())
    ax.set_ylim(0, 200)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax.set_ylabel("Million Troy Oz")
    month_name = datetime(year, month, 1).strftime("%B %Y")
    ax.set_xlabel(month_name)
    ax.legend(loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title(
        f"SHFE – OI & Warehouse Inventory  ({month_name})  (Plot 0b)",
        fontsize=14, fontweight="bold", pad=12,
    )

    fig.autofmt_xdate(rotation=30, ha="right")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "00b_shfe_oi_warehouse.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── Plot 9: Total OI Trend ────────────────────────────────────────────
def plot_total_oi_trend(comex: pd.DataFrame, shfe: pd.DataFrame) -> str:
    """
    Full-year time series of total open interest for COMEX and SHFE.
    Left Y-axis  : COMEX OI in million troy oz (400 – 1000)
    Right Y-axis : SHFE OI in million troy oz  (0 – 600)
    X-axis : Jan 1 → Dec 31 of the current year
    """
    year    = date.today().year
    x_start = datetime(year, 1, 1)
    x_end   = datetime(year, 12, 31)

    fig, ax = plt.subplots(figsize=(14, 6))
    ax2 = ax.twinx()

    if comex is not None and not comex.empty:
        comex_moz = comex["all_oi_contracts"] * 5_000 / 1e6
        ax.plot(comex["date"], comex_moz,
                color=COMEX_COLOR_REG, linewidth=2.5,
                marker="o", markersize=6, label="COMEX OI", zorder=4)

    if shfe is not None and not shfe.empty:
        shfe_moz = shfe["total_oi_contracts"] * SHFE_AG_CONTRACT_OZ / 1e6
        ax2.plot(shfe["date"], shfe_moz,
                 color=SHFE_COLOR_REG, linewidth=2.5,
                 marker="s", markersize=6, label="SHFE OI", zorder=3)

    ax.set_xlim(x_start, x_end)
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
    ax.set_ylim(400, 1000)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax.set_ylabel("COMEX OI  (Million Troy Oz)", color=COMEX_COLOR_REG)
    ax.tick_params(axis="y", labelcolor=COMEX_COLOR_REG)
    ax.set_xlabel(str(year))

    ax2.set_ylim(0, 600)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax2.set_ylabel("SHFE OI  (Million Troy Oz)", color=SHFE_COLOR_REG)
    ax2.tick_params(axis="y", labelcolor=SHFE_COLOR_REG)

    # Merged legend
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper right", framealpha=0.9, fontsize=9)
    ax.set_title(
        f"COMEX & SHFE – Total Open Interest Trend  ({year})  (Plot 9)",
        fontsize=14, fontweight="bold", pad=12,
    )

    fig.autofmt_xdate(rotation=0, ha="center")
    fig.tight_layout()
    out = os.path.join(PLOT_DIR, "09_total_oi_trend.png")
    fig.savefig(out, dpi=150)
    plt.close(fig)
    return out


# ── main ───────────────────────────────────────────────────────────
def main():
    os.makedirs(PLOT_DIR, exist_ok=True)

    print("Loading CSVs …")
    comex = load_csv(COMEX_CSV)
    shfe  = load_csv(SHFE_CSV)
    etc   = load_csv(ETC_CSV)       # loaded for future plots

    print("\n── Plot 1: Registered vs Eligible ──")
    saved = plot_registered_vs_eligible(comex, shfe)
    for out in saved:
        print(f"  ✓  saved → {out}")

    print("\n── Plot 2: OI by Contract Month (bars) ──")
    saved = plot_oi_vs_warehouse(comex, shfe)
    for out in saved:
        print(f"  ✓  saved → {out}")

    print("\n── Plot 3: OI per Contract Month Over Time ──")
    saved = plot_oi_timeseries(comex, shfe)
    for out in saved:
        print(f"  ✓  saved → {out}")

    if etc is not None and not etc.empty:
        print("\n── Plot 4: ETC Holdings ──")
        out = plot_etc_holdings(etc)
        print(f"  ✓  saved → {out}")

        print("\n── Plot 5: ETC Collateral Ratio ──")
        out = plot_etc_collateral(etc)
        print(f"  ✓  saved → {out}")

        print("\n── Plot 6: ETC Verification Gap ──")
        out = plot_verification_gap(etc)
        print(f"  ✓  saved → {out}")

    print("\n── Plot 7: OI-to-Warehouse Ratio ──")
    saved = plot_oi_warehouse_ratio(comex, shfe)
    print(f"  ✓  saved → {saved}")

    print("\n── Plot 8: Registered Silver / OI ──")
    saved = plot_registered_to_oi(comex, shfe)
    print(f"  ✓  saved → {saved}")

    print("\n── Plot 0: Deliveries, OI & Warehouse (current month) ──")
    out = plot_deliveries_oi_warehouse(comex)
    print(f"  ✓  saved → {out}")

    print("\n── Plot 0b: SHFE OI & Warehouse (current month) ──")
    out = plot_shfe_oi_warehouse(shfe)
    print(f"  ✓  saved → {out}")

    print("\n── Plot 9: Total OI Trend ──")
    out = plot_total_oi_trend(comex, shfe)
    print(f"  ✓  saved → {out}")

    print("\nDone.")


if __name__ == "__main__":
    main()
