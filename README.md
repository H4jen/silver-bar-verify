# Silver Bar Verify

A Python pipeline that independently verifies physical silver holdings across major exchanges and ETC funds by fetching and cross-referencing public data sources daily.

## What It Does

| Layer | Data Source | What Is Checked |
|---|---|---|
| **ETC bar lists** | Invesco, WisdomTree | Individual bar serial numbers, weights, vault locations — compared against fund metrics to detect discrepancies |
| **COMEX** | CME Group (official APIs) | Silver futures deliveries, open interest, warehouse stocks (registered vs. eligible) |
| **SHFE** | Shanghai Futures Exchange (official APIs) | Silver (AG) futures, warrant vs. cargo weights per depository |

The results are stored as time-series CSVs and rendered as charts, making it easy to spot unusual changes in inventory or open interest over time.

## Pipeline Steps

```
run_all.py
  1. fetch_and_verify_barlists.py   — download ETC bar PDFs, verify bar inventories
  2. comex_silver_report2.py        — COMEX delivery reports & futures data
  3. generate_csv.py                — build ETC time-series CSV
  4. generate_comex_csv.py          — build COMEX time-series CSV
  5. fetch_shfe_silver.py           — SHFE silver inventory & futures data
  6. generate_shfe_csv.py           — build SHFE time-series CSV
  9. generate_plots.py              — render all charts to comex_data/plots/
```

## Installation

```bash
git clone https://github.com/<you>/silver-bar-verify.git
cd silver-bar-verify
pip install -r requirements.txt
```

Python 3.10+ is recommended.

## Usage

```bash
# Run the full pipeline
./run_all.py

# Skip a step (e.g. skip ETC bar fetching)
./run_all.py --skip 1

# Run only one step
./run_all.py --only 2

# Check dependencies without running anything
./run_all.py --dry-run
```

Individual scripts can also be run standalone:

```bash
python fetch_and_verify_barlists.py
python comex_silver_report2.py
python fetch_shfe_silver.py --history 5
python generate_plots.py
```

## Cron (daily automation)

```cron
# Run full pipeline every day at 22:00
0 22 * * * /path/to/silver-bar-verify/run_all.py >> /path/to/silver-bar-verify/comex_data/run_all.log 2>&1
```

## Output

| Path | Contents |
|---|---|
| `comex_data/etc_silver_inventory_verification_<date>.json` | ETC bar verification results |
| `comex_data/comex_silver_timeseries.csv` | COMEX daily time-series |
| `comex_data/shfe_silver_timeseries.csv` | SHFE daily time-series |
| `comex_data/silver_etcs_timeseries.csv` | ETC fund metrics time-series |
| `comex_data/plots/` | PNG charts |
| `comex_data/reports/` | Plain-text SHFE daily reports |

## Notes on ETC Bar List Downloads

Many ETC providers geo-block or rate-limit automated downloads. The scripts support falling back to manually placed local PDF files. See the comments in `fetch_and_verify_barlists.py` for details.

## License

[MIT](LICENSE)
