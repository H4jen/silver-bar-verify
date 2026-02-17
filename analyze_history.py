#!/usr/bin/env python3
"""
Analyze Silver ETC Verification History
========================================
Scans all dated verification reports in comex_data/ and shows:
  - Day-by-day changes in bar count, physical oz, expected oz, gap %
  - Bars added / removed between consecutive runs
  - Trend in overcollateralization

Usage:
    python analyze_history.py              # scan comex_data/
    python analyze_history.py test_data     # scan test_data/
"""

from __future__ import annotations

import glob
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def load_reports(data_dir: str | None = None) -> list[dict]:
	"""Find all dated report files, load and sort by generated_utc."""
	if data_dir is None:
		data_dir = os.path.join(SCRIPT_DIR, "comex_data")
	pattern = os.path.join(data_dir, "etc_silver_inventory_verification_*.json")
	paths = sorted(glob.glob(pattern))
	# Exclude the 'latest' symlink/copy to avoid double-counting
	paths = [p for p in paths if "latest" not in os.path.basename(p)]
	reports = []
	for path in paths:
		try:
			with open(path, "r", encoding="utf-8") as f:
				data = json.load(f)
			data["_file"] = os.path.basename(path)
			reports.append(data)
		except (json.JSONDecodeError, KeyError) as exc:
			print(f"  skipping {path}: {exc}", file=sys.stderr)
	reports.sort(key=lambda r: r.get("generated_utc", ""))
	return reports


def extract_fund_summary(report: dict, fund_key: str) -> dict | None:
	"""Pull key numbers for a fund from a report."""
	results = report.get("results", {})
	fund = results.get(fund_key)
	if not fund:
		return None
	agg = fund.get("aggregates") or {}
	ver = fund.get("verification") or {}
	return {
		"date": report.get("generated_utc", "?")[:10],
		"file": report.get("_file", "?"),
		"bar_count": agg.get("bar_count", 0),
		"total_gross_oz": agg.get("total_gross_oz", 0),
		"total_fine_oz": agg.get("total_fine_oz", 0),
		"expected_oz": ver.get("expected_oz", 0),
		"physical_oz": ver.get("physical_oz_from_bar_list", 0),
		"difference_pct": ver.get("difference_pct"),
		"status": ver.get("status", "?"),
		"vaults": agg.get("vaults", {}),
		"unique_refiners": agg.get("unique_refiners", 0),
	}


def bar_serial_set(report: dict, fund_key: str) -> set[str]:
	"""Extract set of bar serial numbers for a fund from a report."""
	results = report.get("results", {})
	fund = results.get(fund_key)
	if not fund:
		return set()
	bars = fund.get("bars", [])
	return {b["serial_number"] for b in bars if isinstance(b, dict) and "serial_number" in b}


def bar_lookup(report: dict, fund_key: str) -> dict[str, dict]:
	"""Build serial->bar dict for a fund from a report."""
	results = report.get("results", {})
	fund = results.get(fund_key)
	if not fund:
		return {}
	bars = fund.get("bars", [])
	return {b["serial_number"]: b for b in bars if isinstance(b, dict) and "serial_number" in b}


def print_bar_table(bars: list[dict], label: str, max_rows: int = 50) -> None:
	"""Print a flat one-line-per-bar table sorted by vault then serial."""
	if not bars:
		return
	total_oz = sum(b.get("gross_oz", 0) for b in bars)
	print(f"\n  {label} ({len(bars):,} bars, {total_oz:,.1f} gross oz):")
	print(f"    {'Vault':<20} {'Serial':<12} {'Refiner':<20} {'Gross oz':>9} {'Fine oz':>9} {'Fin':>6}")
	print(f"    {'─'*20} {'─'*12} {'─'*20} {'─'*9} {'─'*9} {'─'*6}")
	sorted_bars = sorted(bars, key=lambda b: (b.get("vault", ""), b.get("serial_number", "")))
	for i, b in enumerate(sorted_bars):
		if i >= max_rows:
			print(f"    ... and {len(bars) - max_rows:,} more")
			return
		fine = b.get("fine_oz", 0)
		fine_str = f"{fine:>9,.1f}" if fine else "      —"
		vault = b.get("vault", "?")
		if len(vault) > 20:
			vault = vault[:18] + ".."
		refiner = b.get("refiner", "?")
		if len(refiner) > 20:
			refiner = refiner[:18] + ".."
		print(
			f"    {vault:<20} "
			f"{b.get('serial_number','?'):<12} "
			f"{refiner:<20} "
			f"{b.get('gross_oz',0):>9,.1f} "
			f"{fine_str} "
			f"{b.get('fineness',0):>.4f}"
		)


def fmt_oz(val: float | None) -> str:
	if val is None:
		return "N/A"
	return f"{val:>16,.1f}"


def fmt_pct(val: float | None) -> str:
	if val is None:
		return "  N/A"
	return f"{val:>+8.4f}%"


def fmt_delta(cur: float | None, prev: float | None) -> str:
	if cur is None or prev is None:
		return ""
	diff = cur - prev
	if abs(diff) < 0.01:
		return "  (unchanged)"
	return f"  ({diff:>+,.1f})"


def print_fund_history(reports: list[dict], fund_key: str) -> None:
	"""Print historical table for one fund."""
	summaries = []
	for r in reports:
		s = extract_fund_summary(r, fund_key)
		if s:
			summaries.append((r, s))

	if not summaries:
		print(f"  No data for {fund_key}")
		return

	display_name = reports[0].get("results", {}).get(fund_key, {}).get("display_name", fund_key)
	print(f"\n{'=' * 78}")
	print(f"  {display_name}")
	print(f"{'=' * 78}")

	print()
	print(f"  {'Date':<12}{'Bars':>7} {'Physical oz':>15} {'Expected oz':>15} {'Gap %':>9} Status")
	print(f"  {'─'*10}  {'─'*7} {'─'*15} {'─'*15} {'─'*9} {'─'*14}")

	prev_summary = None
	prev_report_ref = None
	for report, s in summaries:
		delta_info = ""
		if prev_summary and prev_report_ref:
			prev_bars = bar_lookup(prev_report_ref, fund_key)
			curr_bars = bar_lookup(report, fund_key)
			if prev_bars and curr_bars:
				n_added = len(set(curr_bars) - set(prev_bars))
				n_removed = len(set(prev_bars) - set(curr_bars))
				if n_added or n_removed:
					delta_info = f"  +{n_added} added, -{n_removed} removed"
			else:
				db = s["bar_count"] - prev_summary["bar_count"]
				if db != 0:
					delta_info = f"  Δ bars {db:+d}"

		status_short = (s["status"]
			.replace("match_within_0.25pct", "OK ≤0.25%")
			.replace("overcollateralized_gt_1pct", "OVER >1%")
			.replace("undercollateralized_gt_1pct", "UNDER >1%")
			.replace("match_within_1pct", "OK ≤1%")
			.replace("_", " "))
		print(
			f"  {s['date']:<12}"
			f"{s['bar_count']:>7} "
			f"{s['physical_oz']:>15,.0f} "
			f"{s['expected_oz']:>15,.0f} "
			f"{fmt_pct(s['difference_pct'])} "
			f"{status_short}"
			f"{delta_info}"
		)
		prev_summary = s
		prev_report_ref = report

	# Bar-level diff between every consecutive pair
	if len(summaries) >= 2:
		for i in range(1, len(summaries)):
			prev_report, prev_s = summaries[i - 1]
			curr_report, curr_s = summaries[i]

			print(f"\n  --- Bar changes: {prev_s['date']} → {curr_s['date']} ---")

			prev_bars = bar_lookup(prev_report, fund_key)
			curr_bars = bar_lookup(curr_report, fund_key)

			if not prev_bars or not curr_bars:
				print("  (bar-level data not available — run with full output to enable)")
				continue

			added_serials = set(curr_bars) - set(prev_bars)
			removed_serials = set(prev_bars) - set(curr_bars)
			unchanged = set(prev_bars) & set(curr_bars)
			print(f"  Unchanged bars: {len(unchanged):,}")
			print(f"  Bars added:     {len(added_serials):,}")
			print(f"  Bars removed:   {len(removed_serials):,}")

			if added_serials:
				added_list = [curr_bars[s] for s in added_serials]
				print_bar_table(added_list, "ADDED")

			if removed_serials:
				removed_list = [prev_bars[s] for s in removed_serials]
				print_bar_table(removed_list, "REMOVED")

	# Vault breakdown for latest run
	_, latest = summaries[-1]
	if latest["vaults"]:
		print(f"\n  --- Vault breakdown (latest) ---")
		for vault, info in sorted(latest["vaults"].items()):
			if isinstance(info, dict):
				print(f"  {vault:<45} {info.get('bars', '?'):>7} bars  {info.get('gross_oz', 0):>14,.1f} oz")


def main() -> int:
	data_dir = None
	if len(sys.argv) > 1:
		data_dir = os.path.join(SCRIPT_DIR, sys.argv[1]) if not os.path.isabs(sys.argv[1]) else sys.argv[1]
	reports = load_reports(data_dir)
	if not reports:
		print(f"No verification reports found in {data_dir or 'comex_data/'}")
		print("Run fetch_and_verify_barlists.py first to generate reports.")
		return 1

	print(f"Found {len(reports)} report(s)")

	# Determine which funds appear across all reports
	fund_keys: set[str] = set()
	for r in reports:
		fund_keys.update(r.get("results", {}).keys())

	for fund_key in sorted(fund_keys):
		print_fund_history(reports, fund_key)

	print()
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
