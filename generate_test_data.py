#!/usr/bin/env python3
"""
Generate simulated verification reports for testing analyze_history.py.

Creates 5 days of reports in test_data/ with realistic bar-level changes:
  - Feb 10 (Mon): baseline
  - Feb 11 (Tue): small redemption (bars leave)
  - Feb 12 (Wed): large creation (bars arrive)
  - Feb 13 (Thu): quiet day (no changes)
  - Feb 14 (Fri): mixed (some bars swapped)

Usage:
    python generate_test_data.py          # generate into test_data/
    python analyze_history.py test_data   # analyse the test data
"""

import json
import os
import copy
import random

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "test_data")

REFINERS_INVESCO = ["Aurubis", "KGHM Polska Miedz", "Umicore", "Asahi Refining", "Heraeus"]
REFINERS_WT_HSBC = ["Asahi Refining", "DOE RUN PERU SRL", "Umicore", "Penoles"]
REFINERS_WT_MALCA = ["DOE RUN PERU SRL", "KGHM Polska Miedz", "Asahi Refining", "Penoles"]


def make_bar(serial: str, refiner: str, vault: str, gross_oz: float,
             fineness: float = 0.999, fine_oz: float | None = None) -> dict:
    return {
        "serial_number": serial,
        "refiner": refiner,
        "gross_oz": round(gross_oz, 1),
        "fine_oz": round(fine_oz if fine_oz is not None else gross_oz * fineness, 1),
        "fineness": fineness,
        "vault": vault,
        "year": None,
        "source_page": 1,
        "raw_line": "",
    }


def make_invesco_bar(serial_num: int) -> dict:
    refiner = random.choice(REFINERS_INVESCO)
    gross = round(random.uniform(850, 1150), 1)
    return make_bar(f"INV-{serial_num:05d}", refiner, "JPM London B (VLTB)", gross)


def make_wt_bar(serial_num: int, vault: str) -> dict:
    if vault == "HSBC VAULT":
        refiner = random.choice(REFINERS_WT_HSBC)
    else:
        refiner = random.choice(REFINERS_WT_MALCA)
    gross = round(random.uniform(950, 1100), 1)
    return make_bar(f"WT-{serial_num:05d}", refiner, vault, gross, fineness=0.9999, fine_oz=0.0)


def build_baseline() -> dict:
    """Create a baseline report with ~200 Invesco bars and ~300 WisdomTree bars."""
    random.seed(42)  # reproducible

    # Invesco: 200 bars in JPM London
    inv_bars = [make_invesco_bar(i) for i in range(1, 201)]
    inv_gross = sum(b["gross_oz"] for b in inv_bars)
    inv_fine = sum(b["fine_oz"] for b in inv_bars)
    inv_expected = round(inv_fine * 0.998, 2)  # ~0.2% overcollateralized

    # WisdomTree: 120 in HSBC, 180 in MALCA
    wt_bars = []
    for i in range(1, 121):
        wt_bars.append(make_wt_bar(i, "HSBC VAULT"))
    for i in range(121, 301):
        wt_bars.append(make_wt_bar(i, "MALCA AMIT COMMODITIES LTD LONDON"))
    wt_gross = sum(b["gross_oz"] for b in wt_bars)
    wt_fine_computed = sum(b["gross_oz"] * b["fineness"] for b in wt_bars)
    wt_expected = round(wt_fine_computed * 0.97, 2)  # ~3% overcollateralized

    return {
        "generated_utc": "2026-02-10T16:00:00Z",
        "script": "generate_test_data.py",
        "funds_requested": ["invesco", "wisdomtree"],
        "inputs": {"etc_fund_metrics": "simulated"},
        "results": {
            "invesco": {
                "display_name": "Invesco Physical Silver ETC",
                "source": {"pdf": "simulated"},
                "parse": {"bars_parsed": len(inv_bars)},
                "aggregates": {
                    "bar_count": len(inv_bars),
                    "total_gross_oz": inv_gross,
                    "total_fine_oz": inv_fine,
                    "unique_refiners": len(set(b["refiner"] for b in inv_bars)),
                    "vaults": {
                        "JPM London B (VLTB)": {
                            "bars": len(inv_bars),
                            "gross_oz": inv_gross,
                        }
                    },
                },
                "verification": {
                    "expected_oz": inv_expected,
                    "physical_oz_from_bar_list": inv_fine,
                    "difference_pct": round((inv_fine / inv_expected - 1) * 100, 6),
                    "status": "match_within_0.25pct",
                },
                "bars": inv_bars,
                "errors": [],
            },
            "wisdomtree": {
                "display_name": "WisdomTree Physical Silver ETC",
                "source": {"pdf": "simulated"},
                "parse": {"bars_parsed": len(wt_bars)},
                "aggregates": {
                    "bar_count": len(wt_bars),
                    "total_gross_oz": wt_gross,
                    "total_fine_oz": wt_fine_computed,
                    "unique_refiners": len(set(b["refiner"] for b in wt_bars)),
                    "vaults": {
                        "HSBC VAULT": {
                            "bars": 120,
                            "gross_oz": sum(b["gross_oz"] for b in wt_bars[:120]),
                        },
                        "MALCA AMIT COMMODITIES LTD LONDON": {
                            "bars": 180,
                            "gross_oz": sum(b["gross_oz"] for b in wt_bars[120:]),
                        },
                    },
                },
                "verification": {
                    "expected_oz": wt_expected,
                    "physical_oz_from_bar_list": wt_fine_computed,
                    "difference_pct": round((wt_fine_computed / wt_expected - 1) * 100, 6),
                    "status": "overcollateralized_gt_1pct",
                },
                "bars": wt_bars,
                "errors": [],
            },
        },
        "summary": {"funds_processed": 2, "runtime_seconds": 0.0},
    }


def mutate_report(report: dict, date_str: str, utc_str: str,
                  inv_remove: int, inv_add: int,
                  wt_remove: int, wt_add: int,
                  next_inv_serial: int, next_wt_serial: int) -> tuple[dict, int, int]:
    """Create a new report by removing/adding bars."""
    r = copy.deepcopy(report)
    r["generated_utc"] = utc_str

    # --- Invesco ---
    inv = r["results"]["invesco"]
    removed_inv = inv["bars"][:inv_remove]
    inv["bars"] = inv["bars"][inv_remove:]
    for i in range(inv_add):
        inv["bars"].append(make_invesco_bar(next_inv_serial + i))
    next_inv_serial += inv_add
    inv["aggregates"]["bar_count"] = len(inv["bars"])
    inv["aggregates"]["total_gross_oz"] = sum(b["gross_oz"] for b in inv["bars"])
    inv["aggregates"]["total_fine_oz"] = sum(b["fine_oz"] for b in inv["bars"])
    inv["verification"]["physical_oz_from_bar_list"] = inv["aggregates"]["total_fine_oz"]
    inv["verification"]["difference_pct"] = round(
        (inv["verification"]["physical_oz_from_bar_list"] / inv["verification"]["expected_oz"] - 1) * 100, 6
    )

    # --- WisdomTree ---
    wt = r["results"]["wisdomtree"]
    removed_wt = wt["bars"][:wt_remove]
    wt["bars"] = wt["bars"][wt_remove:]
    for i in range(wt_add):
        vault = random.choice(["HSBC VAULT", "MALCA AMIT COMMODITIES LTD LONDON"])
        wt["bars"].append(make_wt_bar(next_wt_serial + i, vault))
    next_wt_serial += wt_add
    wt["aggregates"]["bar_count"] = len(wt["bars"])
    wt["aggregates"]["total_gross_oz"] = sum(b["gross_oz"] for b in wt["bars"])
    fine = sum(b["gross_oz"] * b["fineness"] for b in wt["bars"])
    wt["aggregates"]["total_fine_oz"] = fine
    wt["verification"]["physical_oz_from_bar_list"] = fine
    wt["verification"]["difference_pct"] = round(
        (fine / wt["verification"]["expected_oz"] - 1) * 100, 6
    )

    # Update vault counts
    hsbc = [b for b in wt["bars"] if b["vault"] == "HSBC VAULT"]
    malca = [b for b in wt["bars"] if b["vault"] != "HSBC VAULT"]
    wt["aggregates"]["vaults"] = {
        "HSBC VAULT": {"bars": len(hsbc), "gross_oz": sum(b["gross_oz"] for b in hsbc)},
        "MALCA AMIT COMMODITIES LTD LONDON": {"bars": len(malca), "gross_oz": sum(b["gross_oz"] for b in malca)},
    }

    return r, next_inv_serial, next_wt_serial


def save_report(report: dict, date_tag: str) -> str:
    path = os.path.join(OUTPUT_DIR, f"etc_silver_inventory_verification_{date_tag}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=1)
    return path


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    random.seed(42)

    # Day 1: Feb 10 (Mon) — baseline
    day1 = build_baseline()
    save_report(day1, "20260210")
    inv_count = day1["results"]["invesco"]["aggregates"]["bar_count"]
    wt_count = day1["results"]["wisdomtree"]["aggregates"]["bar_count"]
    print(f"Feb 10: Invesco {inv_count} bars, WisdomTree {wt_count} bars  (baseline)")

    next_inv = 201
    next_wt = 301

    # Day 2: Feb 11 (Tue) — small redemption
    day2, next_inv, next_wt = mutate_report(
        day1, "20260211", "2026-02-11T16:00:00Z",
        inv_remove=3, inv_add=0,   # 3 bars redeemed
        wt_remove=5, wt_add=0,     # 5 bars redeemed
        next_inv_serial=next_inv, next_wt_serial=next_wt,
    )
    save_report(day2, "20260211")
    print(f"Feb 11: Invesco {day2['results']['invesco']['aggregates']['bar_count']} bars (-3), "
          f"WisdomTree {day2['results']['wisdomtree']['aggregates']['bar_count']} bars (-5)  (redemption)")

    # Day 3: Feb 12 (Wed) — large creation
    day3, next_inv, next_wt = mutate_report(
        day2, "20260212", "2026-02-12T16:00:00Z",
        inv_remove=0, inv_add=10,   # 10 new bars
        wt_remove=0, wt_add=20,     # 20 new bars
        next_inv_serial=next_inv, next_wt_serial=next_wt,
    )
    save_report(day3, "20260212")
    print(f"Feb 12: Invesco {day3['results']['invesco']['aggregates']['bar_count']} bars (+10), "
          f"WisdomTree {day3['results']['wisdomtree']['aggregates']['bar_count']} bars (+20)  (creation)")

    # Day 4: Feb 13 (Thu) — quiet day, no changes
    day4, next_inv, next_wt = mutate_report(
        day3, "20260213", "2026-02-13T16:00:00Z",
        inv_remove=0, inv_add=0,
        wt_remove=0, wt_add=0,
        next_inv_serial=next_inv, next_wt_serial=next_wt,
    )
    save_report(day4, "20260213")
    print(f"Feb 13: Invesco {day4['results']['invesco']['aggregates']['bar_count']} bars (unchanged), "
          f"WisdomTree {day4['results']['wisdomtree']['aggregates']['bar_count']} bars (unchanged)  (quiet)")

    # Day 5: Feb 14 (Fri) — mixed: some bars swapped
    day5, next_inv, next_wt = mutate_report(
        day4, "20260214", "2026-02-14T16:00:00Z",
        inv_remove=4, inv_add=6,    # swap: 4 out, 6 in
        wt_remove=8, wt_add=12,     # swap: 8 out, 12 in
        next_inv_serial=next_inv, next_wt_serial=next_wt,
    )
    save_report(day5, "20260214")
    print(f"Feb 14: Invesco {day5['results']['invesco']['aggregates']['bar_count']} bars (+6 -4), "
          f"WisdomTree {day5['results']['wisdomtree']['aggregates']['bar_count']} bars (+12 -8)  (mixed)")

    print(f"\nGenerated 5 reports in {OUTPUT_DIR}/")
    print(f"Run:  python analyze_history.py test_data")


if __name__ == "__main__":
    main()
