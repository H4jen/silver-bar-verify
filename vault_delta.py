#!/usr/bin/env python3
"""
Vault Delta Analysis
====================

Track silver bar adds/removes/re-entries between bar-list snapshots.
Maintains a persistent JSON database of every bar ever seen per fund,
flags bars that leave and re-enter the vault, and detects vault transfers.

Usage (standalone):
    python vault_delta.py                      # analyse current bar lists
    python vault_delta.py --funds invesco      # single fund
    python vault_delta.py --reset              # wipe history DBs and start fresh

Imported by verify_silver_etcs.py for integrated reporting.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
#  Resolve shared constants / types from the main verification script
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(SCRIPT_DIR, "comex_data")

# Lazy import to avoid circular imports when used as a library
_BarRecord = None
_DEFAULT_FUNDS: dict[str, dict[str, Any]] | None = None


def _ensure_imports() -> None:
    """Lazy-import BarRecord and DEFAULT_FUNDS from the main script."""
    global _BarRecord, _DEFAULT_FUNDS
    if _BarRecord is None:
        from verify_silver_etcs import BarRecord, DEFAULT_FUNDS
        _BarRecord = BarRecord
        _DEFAULT_FUNDS = DEFAULT_FUNDS


def _fund_display_name(fund_key: str) -> str:
    _ensure_imports()
    return (_DEFAULT_FUNDS or {}).get(fund_key, {}).get("display_name", fund_key)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
#  Bar identity
# ---------------------------------------------------------------------------

def bar_key(bar) -> str:
    """Return a canonical identity key for a bar: ``serial_number|refiner``.

    Works with a BarRecord dataclass instance, a plain dict (as stored in
    JSON output), or any object with serial_number/refiner attributes.
    """
    if isinstance(bar, dict):
        serial = bar.get("serial_number", "")
        refiner = bar.get("refiner") or ""
    else:
        serial = bar.serial_number
        refiner = bar.refiner or ""
    return f"{serial}|{refiner}"


# ---------------------------------------------------------------------------
#  Persistent bar-history database
# ---------------------------------------------------------------------------

def _bar_history_path(fund_key: str) -> str:
    """Return the path to the persistent bar-history DB for a fund."""
    return os.path.join(CACHE_DIR, f"bar_history_{fund_key}.json")


def load_bar_history(fund_key: str) -> dict[str, Any]:
    """Load the persistent bar-history database for *fund_key*.

    Returns a dict with keys:
      fund, last_updated, snapshots (list of date-tags), bars (dict keyed
      by bar_key, each with first_seen, last_seen, appearances, re_entries,
      status, and last-known bar attributes).
    """
    path = _bar_history_path(fund_key)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {
        "fund": fund_key,
        "last_updated": None,
        "snapshots": [],
        "bars": {},
    }


def save_bar_history(fund_key: str, history: dict[str, Any]) -> str:
    """Persist the bar-history DB and return the file path."""
    path = _bar_history_path(fund_key)
    history["last_updated"] = _now_iso()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(history, fh, indent=2)
    return path


def reset_bar_history(fund_key: str) -> None:
    """Delete the persistent bar-history DB for *fund_key*."""
    path = _bar_history_path(fund_key)
    if os.path.exists(path):
        os.remove(path)
        print(f"  Deleted {path}")


# ---------------------------------------------------------------------------
#  Update history + compute delta
# ---------------------------------------------------------------------------

def update_bar_history(
    fund_key: str,
    bars: list,
    date_tag: str,
) -> dict[str, Any]:
    """Merge a new snapshot into the persistent bar-history DB.

    *bars* can be a list of BarRecord or list of dicts.
    *date_tag* should be YYYYMMDD (the as-of date of the bar list).
    Returns a delta report dict (see ``compute_vault_delta``).
    """
    history = load_bar_history(fund_key)

    # Build a lookup of current bars
    current_keys: dict[str, Any] = {}
    for b in bars:
        current_keys[bar_key(b)] = b

    # If this exact date_tag was already recorded, skip re-processing
    if date_tag in history["snapshots"]:
        delta = compute_vault_delta(history, current_keys, date_tag, is_repeat=True)
        return delta

    # --- compute delta BEFORE mutating history ---
    delta = compute_vault_delta(history, current_keys, date_tag)

    # --- Update bar entries ---
    for key, b in current_keys.items():
        if isinstance(b, dict):
            serial = b.get("serial_number", "")
            refiner = b.get("refiner")
            gross = b.get("gross_oz")
            fine = b.get("fine_oz")
            vault = b.get("vault")
        else:
            serial = b.serial_number
            refiner = b.refiner
            gross = b.gross_oz
            fine = b.fine_oz
            vault = b.vault

        if key in history["bars"]:
            entry = history["bars"][key]
            was_absent = entry["status"] == "removed"
            entry["last_seen"] = date_tag
            if date_tag not in entry["appearances"]:
                entry["appearances"].append(date_tag)
            if was_absent:
                entry["re_entries"] = entry.get("re_entries", 0) + 1
            entry["status"] = "present"
            # Update latest attributes
            entry["gross_oz"] = gross
            entry["fine_oz"] = fine
            entry["vault"] = vault
        else:
            history["bars"][key] = {
                "serial_number": serial,
                "refiner": refiner,
                "first_seen": date_tag,
                "last_seen": date_tag,
                "appearances": [date_tag],
                "gross_oz": gross,
                "fine_oz": fine,
                "vault": vault,
                "re_entries": 0,
                "status": "present",
            }

    # Mark bars NOT in the current snapshot as removed
    for key, entry in history["bars"].items():
        if key not in current_keys and entry["status"] == "present":
            entry["status"] = "removed"

    history["snapshots"].append(date_tag)
    save_bar_history(fund_key, history)
    return delta


def compute_vault_delta(
    history: dict[str, Any],
    current_keys: dict[str, Any],
    date_tag: str,
    is_repeat: bool = False,
) -> dict[str, Any]:
    """Compute adds/removes/re-entries between last snapshot and *current_keys*.

    Returns a dict:
      date_tag, prev_date, added[], removed[], re_entered[], vault_changes[],
      summary counts, and all_time stats.
    """
    prev_date = history["snapshots"][-1] if history["snapshots"] else None
    hist_bars = history["bars"]

    added: list[dict] = []          # brand new bars never seen before
    returned: list[dict] = []       # bars coming back after removal
    re_entered: list[dict] = []     # bars with re_entries > 0 in this snapshot
    removed: list[dict] = []        # bars present in last snapshot, now gone
    vault_changes: list[dict] = []  # same bar, different vault
    unchanged = 0

    if is_repeat or prev_date is None:
        # First snapshot or repeat — everything is "new"
        for key, b in current_keys.items():
            if isinstance(b, dict):
                added.append({"key": key, "serial": b.get("serial_number", ""),
                              "refiner": b.get("refiner"), "gross_oz": b.get("gross_oz"),
                              "vault": b.get("vault")})
            else:
                added.append({"key": key, "serial": b.serial_number,
                              "refiner": b.refiner, "gross_oz": b.gross_oz,
                              "vault": b.vault})
        return {
            "date_tag": date_tag,
            "prev_date": prev_date,
            "is_first_snapshot": prev_date is None,
            "is_repeat": is_repeat,
            "added": added if prev_date is None else [],
            "removed": [],
            "returned": [],
            "re_entered": [],
            "vault_changes": [],
            "unchanged": len(current_keys) if is_repeat else 0,
            "total_current": len(current_keys),
            "total_ever_seen": len(hist_bars) + (len(current_keys) if prev_date is None else 0),
            "total_re_entry_bars": sum(1 for e in hist_bars.values() if e.get("re_entries", 0) > 0),
        }

    # Normal delta — compare with previous snapshot
    # Previous-snapshot bar keys = all bars that were "present" in history
    prev_keys = {k for k, e in hist_bars.items() if e["status"] == "present"}

    for key, b in current_keys.items():
        if isinstance(b, dict):
            info = {"key": key, "serial": b.get("serial_number", ""),
                    "refiner": b.get("refiner"), "gross_oz": b.get("gross_oz"),
                    "vault": b.get("vault")}
        else:
            info = {"key": key, "serial": b.serial_number,
                    "refiner": b.refiner, "gross_oz": b.gross_oz,
                    "vault": b.vault}

        if key not in hist_bars:
            # Completely new bar
            added.append(info)
        elif key in prev_keys:
            # Was present last time — check vault change
            old_vault = hist_bars[key].get("vault")
            new_vault = info["vault"]
            if old_vault and new_vault and old_vault != new_vault:
                vault_changes.append({**info, "old_vault": old_vault, "new_vault": new_vault})
            else:
                unchanged += 1
        else:
            # Was removed before, now returning
            entry = hist_bars[key]
            info["re_entries"] = entry.get("re_entries", 0) + 1
            info["first_seen"] = entry.get("first_seen")
            info["last_seen_before"] = entry.get("last_seen")
            returned.append(info)

    # Bars removed this time
    for key in prev_keys:
        if key not in current_keys:
            entry = hist_bars[key]
            removed.append({
                "key": key,
                "serial": entry.get("serial_number", ""),
                "refiner": entry.get("refiner"),
                "gross_oz": entry.get("gross_oz"),
                "vault": entry.get("vault"),
                "first_seen": entry.get("first_seen"),
                "last_seen": entry.get("last_seen"),
            })

    # Bars with any re-entry history currently in the vault
    for key in current_keys:
        if key in hist_bars and hist_bars[key].get("re_entries", 0) > 0:
            entry = hist_bars[key]
            re_entered.append({
                "key": key,
                "serial": entry.get("serial_number", ""),
                "refiner": entry.get("refiner"),
                "re_entries": entry.get("re_entries", 0),
                "first_seen": entry.get("first_seen"),
            })

    # Also count re-entries from the "returned" list (they haven't been
    # written to history yet, so add them to the re_entered list too)
    for r in returned:
        re_entered.append({
            "key": r["key"],
            "serial": r["serial"],
            "refiner": r["refiner"],
            "re_entries": r.get("re_entries", 1),
            "first_seen": r.get("first_seen"),
        })

    return {
        "date_tag": date_tag,
        "prev_date": prev_date,
        "is_first_snapshot": False,
        "is_repeat": False,
        "added": added,
        "removed": removed,
        "returned": returned,
        "re_entered": re_entered,
        "vault_changes": vault_changes,
        "unchanged": unchanged,
        "total_current": len(current_keys),
        "total_ever_seen": len(hist_bars) + len(added),
        "total_re_entry_bars": (
            sum(1 for e in hist_bars.values() if e.get("re_entries", 0) > 0)
            + len(returned)
        ),
    }


# ---------------------------------------------------------------------------
#  Human-readable report
# ---------------------------------------------------------------------------

def format_delta_report(fund_key: str, delta: dict[str, Any]) -> str:
    """Build a human-readable text report of vault delta analysis."""
    lines: list[str] = []
    _p = lines.append

    display = _fund_display_name(fund_key)
    W = 78

    _p("=" * W)
    _p(f"  VAULT DELTA ANALYSIS — {display}")
    _p("=" * W)
    _p(f"  Snapshot date:   {delta['date_tag']}")
    _p(f"  Previous date:   {delta.get('prev_date') or '(none — first snapshot)'}")
    _p(f"  Total bars now:  {delta['total_current']:,}")
    _p(f"  All-time bars:   {delta['total_ever_seen']:,}")
    _p("")

    if delta.get("is_first_snapshot"):
        _p("  This is the FIRST snapshot — all bars are new.")
        _p(f"  Bars recorded: {len(delta.get('added', []))}")
        _p("")
        return "\n".join(lines)

    if delta.get("is_repeat"):
        _p("  This snapshot date was already recorded — no delta to compute.")
        _p("")
        return "\n".join(lines)

    added = delta.get("added", [])
    removed = delta.get("removed", [])
    returned = delta.get("returned", [])
    re_entered = delta.get("re_entered", [])
    vault_changes = delta.get("vault_changes", [])

    _p("  " + "-" * (W - 2))
    _p("  SUMMARY:")
    _p(f"    Bars added (new):        {len(added):>8,}")
    _p(f"    Bars removed:            {len(removed):>8,}")
    _p(f"    Bars returned (re-entry):{len(returned):>8,}")
    _p(f"    Vault transfers:         {len(vault_changes):>8,}")
    _p(f"    Unchanged:               {delta.get('unchanged', 0):>8,}")
    _p(f"    Bars with re-entry flag: {len(re_entered):>8,}")
    _p(f"    Lifetime bars tracked:   {delta['total_ever_seen']:>8,}")
    _p("")

    # --- Added bars ---
    if added:
        _p("  " + "-" * (W - 2))
        _p(f"  BARS ADDED ({len(added)}):")
        _p(f"    {'Serial':<16} {'Refiner':<28} {'Gross oz':>12} {'Vault'}")
        _p(f"    {'-'*16} {'-'*28} {'-'*12} {'-'*18}")
        for b in added[:200]:
            gross = f"{b['gross_oz']:,.1f}" if b.get("gross_oz") else "N/A"
            _p(f"    {b['serial']:<16} {(b.get('refiner') or ''):<28}"
               f" {gross:>12} {(b.get('vault') or '')}")
        if len(added) > 200:
            _p(f"    ... and {len(added) - 200:,} more")
        _p("")

    # --- Removed bars ---
    if removed:
        _p("  " + "-" * (W - 2))
        _p(f"  BARS REMOVED ({len(removed)}):")
        _p(f"    {'Serial':<16} {'Refiner':<28} {'Gross oz':>12} {'Vault'}")
        _p(f"    {'-'*16} {'-'*28} {'-'*12} {'-'*18}")
        for b in removed[:200]:
            gross = f"{b['gross_oz']:,.1f}" if b.get("gross_oz") else "N/A"
            _p(f"    {b['serial']:<16} {(b.get('refiner') or ''):<28}"
               f" {gross:>12} {(b.get('vault') or '')}")
        if len(removed) > 200:
            _p(f"    ... and {len(removed) - 200:,} more")
        _p("")

    # --- Returned bars (re-entries) ---
    if returned:
        _p("  " + "-" * (W - 2))
        _p(f"  ⚠ BARS RETURNED TO VAULT ({len(returned)}):")
        _p(f"    These bars were previously removed and have re-entered.")
        _p(f"    {'Serial':<16} {'Refiner':<24} {'Re-entries':>10}"
           f" {'First seen':<12} {'Last seen'}")
        _p(f"    {'-'*16} {'-'*24} {'-'*10} {'-'*12} {'-'*12}")
        for b in returned:
            _p(f"    {b['serial']:<16} {(b.get('refiner') or ''):<24}"
               f" {b.get('re_entries', 1):>10}"
               f" {b.get('first_seen', '?'):<12} {b.get('last_seen_before', '?')}")
        _p("")

    # --- All bars with re-entry history ---
    if re_entered:
        _p("  " + "-" * (W - 2))
        _p(f"  ⚠ ALL BARS WITH RE-ENTRY HISTORY ({len(re_entered)}):")
        _p(f"    {'Serial':<16} {'Refiner':<28} {'Re-entries':>10}"
           f" {'First seen'}")
        _p(f"    {'-'*16} {'-'*28} {'-'*10} {'-'*12}")
        for b in sorted(re_entered, key=lambda x: -x.get("re_entries", 0)):
            _p(f"    {b['serial']:<16} {(b.get('refiner') or ''):<28}"
               f" {b.get('re_entries', 0):>10}"
               f" {b.get('first_seen', '?')}")
        _p("")

    # --- Vault transfers ---
    if vault_changes:
        _p("  " + "-" * (W - 2))
        _p(f"  VAULT TRANSFERS ({len(vault_changes)}):")
        _p(f"    {'Serial':<16} {'Refiner':<20} {'From vault':<18} {'To vault'}")
        _p(f"    {'-'*16} {'-'*20} {'-'*18} {'-'*18}")
        for b in vault_changes:
            _p(f"    {b['serial']:<16} {(b.get('refiner') or ''):<20}"
               f" {(b.get('old_vault') or ''):<18} {(b.get('new_vault') or '')}")
        _p("")

    # --- No changes ---
    if not added and not removed and not returned and not vault_changes:
        _p("  No changes detected between snapshots.")
        _p("")

    return "\n".join(lines)


def format_delta_summary_lines(
    deltas: dict[str, dict[str, Any]],
) -> list[str]:
    """Return summary lines for inclusion in the verification summary table.

    *deltas* maps fund_key → delta dict (from ``update_bar_history``).
    """
    _ensure_imports()
    lines: list[str] = []
    _p = lines.append
    W = 78

    _p("")
    _p("  " + "-" * (W - 2))
    _p("  Vault Delta Analysis:")
    _p(f"    {'Fund':<22} {'Added':>7} {'Removed':>8} {'Return':>7}"
       f" {'VltXfr':>7} {'Same':>7} {'ReEntry':>7}")
    _p(f"    {'-'*22} {'-'*7} {'-'*8} {'-'*7}"
       f" {'-'*7} {'-'*7} {'-'*7}")
    for fund_key, d in deltas.items():
        name = _fund_display_name(fund_key)
        short = name[:22]
        if d.get("is_first_snapshot"):
            _p(f"    {short:<22} {'(first snapshot — all bars are new)':}")
        elif d.get("is_repeat"):
            _p(f"    {short:<22} {'(no change — same snapshot date)':}")
        else:
            _p(f"    {short:<22} {len(d.get('added',[])):>7,}"
               f" {len(d.get('removed',[])):>8,}"
               f" {len(d.get('returned',[])):>7,}"
               f" {len(d.get('vault_changes',[])):>7,}"
               f" {d.get('unchanged',0):>7,}"
               f" {len(d.get('re_entered',[])):>7,}")
    _p("")
    for fund_key, d in deltas.items():
        _p(f"    {fund_key}: {d['total_current']:,} bars now"
           f" / {d['total_ever_seen']:,} lifetime tracked")
        if d.get("re_entered"):
            _p(f"    ⚠ {len(d['re_entered'])} bar(s) with re-entry history")
    _p("")
    return lines


# ---------------------------------------------------------------------------
#  Standalone CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vault delta analysis — track bar adds/removes/re-entries",
    )
    parser.add_argument(
        "--funds",
        nargs="+",
        choices=["invesco", "wisdomtree"],
        default=["invesco", "wisdomtree"],
        help="Which funds to analyse",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete all bar-history databases and exit",
    )
    return parser.parse_args()


def main() -> int:
    """Standalone entry point — runs delta analysis on current bar lists."""
    _ensure_imports()
    args = _parse_args()

    if args.reset:
        for fund in args.funds:
            reset_bar_history(fund)
        print("  Bar history databases reset.")
        return 0

    # Load the latest verification JSON to get current bar lists
    latest_json = os.path.join(CACHE_DIR, "etc_silver_inventory_verification_latest.json")
    if not os.path.exists(latest_json):
        print(f"  ERROR: {latest_json} not found.")
        print("  Run verify_silver_etcs.py first to generate bar list data.")
        return 1

    with open(latest_json, "r", encoding="utf-8") as fh:
        report = json.load(fh)

    from verify_silver_etcs import _normalise_date_tag

    delta_results: dict[str, dict[str, Any]] = {}
    for fund in args.funds:
        fund_result = report.get("results", {}).get(fund, {})
        bars = fund_result.get("bars", [])
        if not bars:
            print(f"  {fund}: no bars in latest report — skipping")
            continue

        hm = (fund_result.get("parse") or {}).get("header_metadata") or {}
        as_of = hm.get("as_of_date", "")
        date_tag = _normalise_date_tag(as_of) if as_of else datetime.now().strftime("%Y%m%d")

        delta = update_bar_history(fund, bars, date_tag)
        delta_results[fund] = delta

        n_add = len(delta.get("added", []))
        n_rem = len(delta.get("removed", []))
        n_ret = len(delta.get("returned", []))
        if delta.get("is_first_snapshot"):
            print(f"  {fund}: first snapshot — {len(bars):,} bars recorded")
        elif delta.get("is_repeat"):
            print(f"  {fund}: same snapshot date ({date_tag}) — no delta")
        else:
            print(f"  {fund}: +{n_add:,} added, -{n_rem:,} removed, {n_ret:,} returned")

    # Write delta reports
    time_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    for fund_key, delta in delta_results.items():
        text = format_delta_report(fund_key, delta)
        print(text)
        delta_file = os.path.join(CACHE_DIR, f"vault_delta_{fund_key}_{time_tag}.txt")
        with open(delta_file, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(f"  Saved: {delta_file}")

    # Print history stats
    print()
    for fund in args.funds:
        history = load_bar_history(fund)
        n_bars = len(history.get("bars", {}))
        n_snaps = len(history.get("snapshots", []))
        n_re = sum(1 for e in history.get("bars", {}).values()
                   if e.get("re_entries", 0) > 0)
        print(f"  {fund}: {n_bars:,} bars tracked across"
              f" {n_snaps} snapshot(s), {n_re} with re-entry history")

    # Regenerate time-series CSV
    print()
    try:
        from generate_csv import generate_csv
        generate_csv(funds=args.funds)
    except Exception as exc:
        print(f"  WARNING: CSV generation failed: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
