#!/usr/bin/env python3
"""
Silver Bar Verify — Daily Runner
=================================

Wrapper script that runs the full pipeline in order:
  1) fetch_and_verify_barlists.py — fetch ETC data, verify bar inventories
  2) comex_silver_report2.py — fetch COMEX silver market data
  3) generate_csv.py         — produce ETC time-series CSV
  4) generate_comex_csv.py   — produce COMEX time-series CSV

Designed to be run via cron.  Checks Python dependencies on startup
and exits with a clear message if any are missing.

Cron example (daily at 22:00):
  0 22 * * * /home/efrepud/projects/silver-bar-verify/run_all.py >> /home/efrepud/projects/silver-bar-verify/comex_data/run_all.log 2>&1

Usage:
    ./run_all.py              # run full pipeline
    ./run_all.py --skip 1     # skip step 1 (fetch_and_verify_barlists)
    ./run_all.py --only 2     # run only step 2 (comex_silver_report2)
    ./run_all.py --dry-run    # check dependencies only, don't run anything
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
import time
from datetime import datetime


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
#  Required Python packages
# ---------------------------------------------------------------------------
#  (module_name, pip_name, required_by)
REQUIRED_PACKAGES = [
    ("requests",   "requests",   "comex_silver_report2.py, fetch_invesco.py"),
    ("pandas",     "pandas",     "comex_silver_report2.py"),
    ("pdfplumber", "pdfplumber", "comex_silver_report2.py, fetch_and_verify_barlists.py"),
    ("pypdf",      "pypdf",      "fetch_and_verify_barlists.py"),
    ("curl_cffi",  "curl_cffi",  "download_documents.py"),
    ("xlrd",       "xlrd",       "comex_silver_report2.py (warehouse stocks XLS)"),
]

OPTIONAL_PACKAGES = [
    ("yfinance", "yfinance", "comex_silver_report2.py (fallback price/settlements)"),
]

# ---------------------------------------------------------------------------
#  Pipeline steps
# ---------------------------------------------------------------------------
STEPS = [
    {
        "num": 1,
        "name": "Fetch Invesco Metrics",
        "script": "fetch_invesco.py",
        "args": ["--update-metrics"],
        "description": "Scrape Invesco fund metrics and update etc_fund_metrics.json",
    },
    {
        "num": 2,
        "name": "Fetch WisdomTree Metrics",
        "script": "fetch_wisdomtree.py",
        "args": ["--update-metrics"],
        "description": "Scrape WisdomTree fund metrics and update etc_fund_metrics.json",
    },
    {
        "num": 3,
        "name": "Silver ETC Verification",
        "script": "fetch_and_verify_barlists.py",
        "description": "Fetch ETC data, download bar lists, verify inventories",
    },
    {
        "num": 4,
        "name": "COMEX Silver Report",
        "script": "comex_silver_report2.py",
        "description": "Fetch COMEX delivery reports, warehouse stocks, settlements",
    },
    {
        "num": 5,
        "name": "ETC Time-Series CSV",
        "script": "generate_csv.py",
        "description": "Generate silver_etcs_timeseries.csv from verification data",
    },
    {
        "num": 6,
        "name": "COMEX Time-Series CSV",
        "script": "generate_comex_csv.py",
        "description": "Generate comex_silver_timeseries.csv from COMEX data",
    },
]


# ---------------------------------------------------------------------------
#  Dependency checker
# ---------------------------------------------------------------------------
def check_dependencies() -> bool:
    """Check all required Python packages.  Returns True if all present."""
    print("Checking Python dependencies...")
    missing = []
    for module_name, pip_name, used_by in REQUIRED_PACKAGES:
        try:
            importlib.import_module(module_name)
            print(f"  ✓ {module_name}")
        except ImportError:
            print(f"  ✗ {module_name}  — required by {used_by}")
            missing.append(pip_name)

    for module_name, pip_name, used_by in OPTIONAL_PACKAGES:
        try:
            importlib.import_module(module_name)
            print(f"  ✓ {module_name} (optional)")
        except ImportError:
            print(f"  ~ {module_name} (optional) — used by {used_by}")

    if missing:
        print()
        print("ERROR: Missing required packages:")
        print(f"  pip install {' '.join(missing)}")
        print()
        print("Or install all at once:")
        all_pkgs = [p for _, p, _ in REQUIRED_PACKAGES]
        print(f"  pip install {' '.join(all_pkgs)}")
        return False

    print("  All required dependencies satisfied.")
    return True


# ---------------------------------------------------------------------------
#  Step runner
# ---------------------------------------------------------------------------
def run_step(step: dict) -> tuple[bool, float]:
    """Run a single pipeline step.  Returns (success, elapsed_seconds)."""
    script_path = os.path.join(SCRIPT_DIR, step["script"])

    if not os.path.exists(script_path):
        print(f"  ERROR: Script not found: {script_path}")
        return False, 0.0

    cmd = [sys.executable, script_path] + step.get("args", [])

    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=SCRIPT_DIR,
            timeout=600,  # 10 minute timeout per step
        )
        elapsed = time.time() - start
        success = result.returncode == 0
        return success, elapsed
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        print(f"  ERROR: Script timed out after {elapsed:.0f}s")
        return False, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"  ERROR: {e}")
        return False, elapsed


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------
def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description="Silver Bar Verify — run full daily pipeline",
    )
    parser.add_argument(
        "--skip", type=int, nargs="+", metavar="N",
        help="Step number(s) to skip (1-6)",
    )
    parser.add_argument(
        "--only", type=int, nargs="+", metavar="N",
        help="Run only these step number(s) (1-6)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Check dependencies only, don't run any scripts",
    )
    args = parser.parse_args()

    now = datetime.now()
    print()
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║          Silver Bar Verify — Daily Pipeline                     ║")
    print(f"║          {now.strftime('%Y-%m-%d %H:%M:%S'):<45}       ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    # Check dependencies
    deps_ok = check_dependencies()
    print()

    if not deps_ok:
        return 1

    if args.dry_run:
        print("Dry run — all dependencies OK, exiting without running scripts.")
        return 0

    # Determine which steps to run
    skip_set = set(args.skip) if args.skip else set()
    only_set = set(args.only) if args.only else None

    steps_to_run = []
    for step in STEPS:
        if only_set is not None:
            if step["num"] in only_set:
                steps_to_run.append(step)
        elif step["num"] not in skip_set:
            steps_to_run.append(step)

    if not steps_to_run:
        print("No steps to run.")
        return 0

    print(f"Pipeline: {len(steps_to_run)} step(s) to run")
    for step in steps_to_run:
        print(f"  {step['num']}) {step['name']}")
    print()

    # Run pipeline
    results = []
    total_start = time.time()

    for step in steps_to_run:
        print("─" * 66)
        print(f"  Step {step['num']}/{len(STEPS)}: {step['name']}")
        print(f"  Script: {step['script']}")
        print(f"  {step['description']}")
        print("─" * 66)
        print()

        success, elapsed = run_step(step)
        results.append((step, success, elapsed))

        status = "OK" if success else "FAILED"
        print()
        print(f"  → Step {step['num']} {status} ({elapsed:.1f}s)")
        print()

    total_elapsed = time.time() - total_start

    # Summary
    print("═" * 66)
    print("  PIPELINE SUMMARY")
    print("═" * 66)
    print()
    print(f"  {'Step':<5} {'Name':<30} {'Status':<10} {'Time':>8}")
    print(f"  {'─'*3:<5} {'─'*28:<30} {'─'*6:<10} {'─'*6:>8}")

    all_ok = True
    for step, success, elapsed in results:
        status = "OK" if success else "FAILED"
        if not success:
            all_ok = False
        print(f"  {step['num']:<5} {step['name']:<30} {status:<10} {elapsed:>7.1f}s")

    print(f"  {'─'*3:<5} {'─'*28:<30} {'─'*6:<10} {'─'*6:>8}")
    print(f"  {'':>35} {'Total:':>10} {total_elapsed:>7.1f}s")
    print()

    if all_ok:
        print(f"  All steps completed successfully.")
    else:
        failed = [s["name"] for s, ok, _ in results if not ok]
        print(f"  WARNING: {len(failed)} step(s) failed: {', '.join(failed)}")

    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
