# InstacallToolkit 🚀

A small collection of handy utilities for phone-number workflows: a high-speed async phone reputation scraper (`DIDRepChecker.py`) and a simple CSV/Excel merger (`Excel_Merger.py`).

Repository: https://github.com/siamakanda/InstacallToolkit

---

## Contents
- **DIDRepChecker.py** — Async scraper that looks up phone numbers and writes results to CSV.
- **Excel_Merger.py** — Find and merge CSV / Excel files in a directory into one output file.

---

## Requirements ✅
- Python 3.8+
- Install runtime deps:

```powershell
python -m pip install -r requirements.txt
# For Excel merging (pandas):
python -m pip install pandas openpyxl
```

Files in this repo:
- `DIDRepChecker.py`
- `Excel_Merger.py`
- `numbers.csv` (example input)
- `results.csv` (default output)
- `requirements.txt`

---

## DIDRepChecker.py 🔎

Purpose: asynchronously query phone-number lookup pages and extract quick reputation data for each number, writing results to a CSV.

Key defaults (edit the `CONFIG` dict in the file to change):
- `input_file`: `numbers.csv`
- `output_file`: `results.csv`
- `concurrent_requests`: 30
- `timeout`: 15
- `max_retries`: 2
- `requests_per_second`: 5
- `batch_size`: 100

Input format: a CSV with one phone number per row. The script auto-detects and skips a header row if present. Numbers are cleaned to digits only and expected to be 10-digit US numbers by default.

Output columns written to CSV:
- `phone_number`, `reputation`, `user_reports`, `total_calls`, `last_call`, `scraped_at`

Usage examples:

```powershell
# Run with defaults
python DIDRepChecker.py

# Edit CONFIG in the file to change input/output files or rate/timeout settings
```

Notes & hints:
- The scraper queries `https://lookup.robokiller.com/search?q=<number>` (see source). Be mindful of the target site's terms of service and rate limits.
- The script handles common blocking responses (403/429/404) and prints useful progress stats.
- If you plan to run large jobs, reduce `requests_per_second` and `concurrent_requests` to avoid being blocked.

---

## Excel_Merger.py 🔧

Purpose: find all `.csv`, `.xlsx`, and `.xls` files in the current directory and merge them into a single output file.

CLI usage examples (also printed by the script):

```powershell
# Basic merge (output: combined_reports.csv)
python Excel_Merger.py

# Include source tracking columns (Source_File, Source_Sheet)
python Excel_Merger.py --include-source

# Write Excel output
python Excel_Merger.py -o final_report.xlsx
```

Options:
- `--include-source` (`-s`): adds `Source_File` and `Source_Sheet` columns to the merged output.
- `--output` (`-o`): specify output filename (CSV or XLSX).

Notes:
- Requires `pandas` and an engine like `openpyxl` to write XLSX files.

---

## Contributing & Extending ✨
- Fixes/improvements welcome — fork the repo and open a PR.
- Suggested improvements: add CLI flags to `DIDRepChecker.py`, friendlier logging, and CSV header validation.

---

## Security & Legal ⚖️
- Use these scripts responsibly. Scraping third-party sites may violate their terms of service.
- Respect rate limits and robots.txt of target sites.

---

## License
No license file is included in this repository. Add a `LICENSE` if you want to set explicit terms (e.g., MIT).

---

If you'd like, I can add command-line flags to `DIDRepChecker.py` to override the `CONFIG` values or add a sample `numbers.csv` with example rows—tell me which change you'd prefer. ✅