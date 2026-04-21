"""
00_wrds_check.py

First-run WRDS connection test.
(1) Prompts for WRDS username/password, caches them in ~/.pgpass
(2) Verifies Tulane's entitlement for each library this project needs
(3) Lists tables in each entitled library for reference

Run interactively:
    venv/Scripts/python.exe scripts/00_wrds_check.py
"""

import sys
from pathlib import Path

try:
    import wrds
except ImportError:
    sys.exit("wrds not installed — run: venv/Scripts/pip install -r requirements.txt")


REQUIRED_LIBS = {
    "ciq_transcripts": "Earnings call transcripts (Capital IQ)",
    "ciq_keydev":      "Key developments / guidance (Capital IQ)",
    "ciq":             "Capital IQ linker / company master",
    "ibes":            "Analyst forecasts (I/B/E/S)",
    "comp":            "Compustat fundamentals",
    "crsp":            "CRSP prices / returns",
    "wrdssec":         "SEC filings (optional — WRDS SEC Analytics Suite)",
}


def main():
    print("Connecting to WRDS — you'll be prompted for username + password on first run.")
    print()

    db = wrds.Connection()

    pgpass = Path.home() / ".pgpass"
    if not pgpass.exists():
        print("Caching credentials in ~/.pgpass so future scripts don't prompt...")
        db.create_pgpass_file()
        print("  OK")

    print()
    print(f"{'Library':20s}  {'Status':8s}  {'Tables':>7s}  Description")
    print(f"{'-' * 20}  {'-' * 8}  {'-' * 7}  {'-' * 50}")

    entitled = {}
    for lib, desc in REQUIRED_LIBS.items():
        try:
            tables = db.list_tables(library=lib)
            status = "OK" if tables else "EMPTY"
            entitled[lib] = tables
            print(f"{lib:20s}  {status:8s}  {len(tables):>7d}  {desc}")
        except Exception as e:
            print(f"{lib:20s}  {'FAIL':8s}  {'-':>7s}  {desc}")
            print(f"  -> {type(e).__name__}: {str(e)[:120]}")

    print()
    print("Sampling a few tables from entitled libraries for the record:")
    for lib, tables in entitled.items():
        if not tables:
            continue
        sample = ", ".join(tables[:5])
        print(f"  {lib}: {sample}{' ...' if len(tables) > 5 else ''}")

    db.close()
    print()
    print("Done. If all required libs show OK, we're clear to start pulling.")


if __name__ == "__main__":
    main()
