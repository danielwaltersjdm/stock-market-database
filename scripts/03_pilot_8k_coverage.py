"""
03_pilot_8k_coverage.py

Test how much earnings-call-related content is filed in 8-K exhibits on SEC
EDGAR for S&P 500-equivalent firms. The empirical question:

    Of the top 500 firms by market cap (as of late 2023), how many file
    (a) an earnings press release as an 8-K Item 2.02 exhibit?
    (b) the full conference call transcript as an 8-K exhibit?

(a) is expected to be near-universal; (b) is expected to be a small minority.
Knowing the actual numbers tells us whether 8-K scraping is a useful free
supplement to a CIQ transcript pull.

Pipeline:
    1. Take top 500 firms by mktcap from the universe at latest 2023 quarter
    2. Map ticker -> CIK via SEC's company_tickers.json
    3. For each CIK, pull recent 8-K submissions for calendar year 2023
       from EDGAR's submissions API
    4. For each 8-K: download the filing index, parse exhibits, classify
       each exhibit as press release / transcript / other based on
       description keywords
    5. Aggregate per-firm and report coverage

Output:
    processed_data/public/pilot_8k_coverage_summary.csv  (per-firm)
    processed_data/public/pilot_8k_filings_detail.csv    (per-filing)

Notes:
    - SEC EDGAR rate limit: 10 requests/second; User-Agent header required
    - Tulane proxy intercepts SSL: use verify=False + suppress warnings
    - Coverage is intentionally a smoke test (only 2023, only top 500)
"""

import re
import sys
import time
import urllib3
from pathlib import Path

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).parent.parent
UNIVERSE_FILE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
PUBLIC_DIR = PROJECT_ROOT / "processed_data" / "public"
SUMMARY_OUT = PUBLIC_DIR / "pilot_8k_coverage_summary.csv"
DETAIL_OUT = PUBLIC_DIR / "pilot_8k_filings_detail.csv"

PILOT_YEAR_END = 2023
TOP_N = 500
TARGET_YEAR = 2023

USER_AGENT = "Daniel Walters Tulane University djw307@gmail.com"
SEC_HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"

# Pace: SEC asks for <=10 req/sec. We'll do ~5-7 to be safe.
REQ_INTERVAL = 0.15

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# Keywords for classifying exhibits
TRANSCRIPT_PATTERNS = [
    r"\btranscript\b",
    r"\bconference call transcript\b",
    r"\bearnings call transcript\b",
]
PRESS_RELEASE_PATTERNS = [
    r"\bpress release\b",
    r"\bearnings release\b",
    r"\bquarterly results press release\b",
]


def classify_exhibit(description: str, exhibit_type: str = "") -> str:
    text = f"{description} {exhibit_type}".lower()
    for p in TRANSCRIPT_PATTERNS:
        if re.search(p, text):
            return "transcript"
    for p in PRESS_RELEASE_PATTERNS:
        if re.search(p, text):
            return "press_release"
    return "other"


def get_pilot_firms() -> pd.DataFrame:
    universe = pd.read_parquet(UNIVERSE_FILE)
    year_slice = universe[universe["quarter_end"].dt.year == PILOT_YEAR_END]
    pilot_quarter = year_slice["quarter_end"].max()
    firms = (
        universe[universe["quarter_end"] == pilot_quarter]
        .nlargest(TOP_N, "mktcap_usd")
        [["permno", "gvkey", "ticker", "comnam", "mktcap_usd"]]
        .reset_index(drop=True)
    )
    print(f"Pilot quarter: {pilot_quarter.date()}   firms: {len(firms)}")
    return firms


def fetch_ticker_to_cik() -> dict:
    print("Fetching SEC ticker->CIK map...")
    r = requests.get(TICKER_MAP_URL, headers=SEC_HEADERS, verify=False, timeout=30)
    r.raise_for_status()
    data = r.json()
    # SEC returns {row_id: {"cik_str": int, "ticker": str, "title": str}}
    mapping = {}
    for entry in data.values():
        mapping[entry["ticker"].upper()] = (str(entry["cik_str"]).zfill(10), entry["title"])
    print(f"  {len(mapping):,} tickers mapped")
    return mapping


def fetch_8k_filings(cik: str, year: int) -> list[dict]:
    """Return list of dicts (accession, filingDate, primaryDocument) for 8-K filings in `year`."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    r = requests.get(url, headers=SEC_HEADERS, verify=False, timeout=30)
    if r.status_code != 200:
        return []
    data = r.json()
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accs = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    items = recent.get("items", [])

    out = []
    for i, form in enumerate(forms):
        if form != "8-K":
            continue
        if not dates[i].startswith(str(year)):
            continue
        out.append({
            "accession": accs[i],
            "filing_date": dates[i],
            "primary_document": primary_docs[i] if i < len(primary_docs) else "",
            "items": items[i] if i < len(items) else "",
        })
    return out


def fetch_filing_exhibits(cik: str, accession: str) -> list[dict]:
    """Return list of exhibits from the filing index. Each: {seq, doc, type, description}."""
    acc_clean = accession.replace("-", "")
    url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}"  # not used
    # Actually use the index.json endpoint
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/index.json"
    r = requests.get(url, headers=SEC_HEADERS, verify=False, timeout=30)
    if r.status_code != 200:
        return []
    data = r.json()
    items = data.get("directory", {}).get("item", [])
    exhibits = []
    for it in items:
        name = it.get("name", "")
        # Skip non-document files
        if name.endswith(".xml") or name.endswith(".htm.xml") or name == "filing-summary.xml":
            continue
        exhibits.append({
            "name": name,
            "type": it.get("type", ""),
            "description": it.get("description", ""),
            "size": it.get("size", 0),
            "last_modified": it.get("last-modified", ""),
        })
    return exhibits


def main():
    if not UNIVERSE_FILE.exists():
        sys.exit(f"Universe file not found: {UNIVERSE_FILE}")

    firms = get_pilot_firms()
    ticker_to_cik = fetch_ticker_to_cik()
    time.sleep(REQ_INTERVAL)

    # Map firms -> CIK
    firms["ticker_clean"] = firms["ticker"].astype(str).str.upper().str.strip()
    firms["cik"] = firms["ticker_clean"].map(lambda t: ticker_to_cik.get(t, (None, None))[0])
    firms["sec_title"] = firms["ticker_clean"].map(lambda t: ticker_to_cik.get(t, (None, None))[1])

    matched = firms.dropna(subset=["cik"]).copy()
    print(f"Ticker->CIK matched: {len(matched)}/{len(firms)}")
    if len(matched) < len(firms):
        unmatched = firms[firms["cik"].isna()]["ticker"].head(10).tolist()
        print(f"  unmatched tickers (first 10): {unmatched}")

    # --- per-firm 8-K fetch ------------------------------------------------
    all_filings = []
    coverage = []
    print(f"\nFetching 8-K filings for {len(matched)} firms (year {TARGET_YEAR})...")
    t0 = time.time()
    for idx, row in matched.reset_index(drop=True).iterrows():
        if (idx + 1) % 25 == 0:
            print(f"  [{idx+1}/{len(matched)}] {time.time()-t0:.1f}s elapsed")
        try:
            filings = fetch_8k_filings(row["cik"], TARGET_YEAR)
        except Exception as e:
            print(f"  ERROR ticker={row['ticker']} cik={row['cik']}: {e}")
            filings = []
        time.sleep(REQ_INTERVAL)

        n_filings = len(filings)
        n_with_2_02 = sum(1 for f in filings if "2.02" in (f.get("items") or ""))

        # Sample a few exhibits per firm — only the 2.02 filings to limit requests
        n_press_release = 0
        n_transcript = 0
        for f in filings:
            if "2.02" not in (f.get("items") or ""):
                continue
            try:
                exhibits = fetch_filing_exhibits(row["cik"], f["accession"])
            except Exception:
                exhibits = []
            time.sleep(REQ_INTERVAL)

            f["exhibits"] = exhibits
            classifications = [classify_exhibit(e["description"], e["type"]) for e in exhibits]
            if "press_release" in classifications:
                n_press_release += 1
            if "transcript" in classifications:
                n_transcript += 1

            for e in exhibits:
                all_filings.append({
                    "ticker": row["ticker"],
                    "cik": row["cik"],
                    "comnam": row["comnam"],
                    "filing_date": f["filing_date"],
                    "accession": f["accession"],
                    "items": f["items"],
                    "exhibit_name": e["name"],
                    "exhibit_type": e["type"],
                    "exhibit_description": e["description"],
                    "exhibit_size": e["size"],
                    "exhibit_class": classify_exhibit(e["description"], e["type"]),
                })

        coverage.append({
            "ticker": row["ticker"],
            "cik": row["cik"],
            "comnam": row["comnam"],
            "mktcap_usd": row["mktcap_usd"],
            "n_8k_filings": n_filings,
            "n_8k_item_2_02": n_with_2_02,
            "n_with_press_release": n_press_release,
            "n_with_transcript": n_transcript,
            "any_press_release": n_press_release > 0,
            "any_transcript": n_transcript > 0,
        })

    cov_df = pd.DataFrame(coverage)
    det_df = pd.DataFrame(all_filings)

    cov_df.to_csv(SUMMARY_OUT, index=False)
    det_df.to_csv(DETAIL_OUT, index=False)
    print(f"\nWrote: {SUMMARY_OUT}")
    print(f"Wrote: {DETAIL_OUT}")

    # --- aggregate report --------------------------------------------------
    n_firms = len(cov_df)
    n_with_filings = (cov_df["n_8k_filings"] > 0).sum()
    n_with_2_02 = (cov_df["n_8k_item_2_02"] > 0).sum()
    n_with_pr = cov_df["any_press_release"].sum()
    n_with_tr = cov_df["any_transcript"].sum()
    print()
    print(f"=== S&P 500 8-K coverage, {TARGET_YEAR} ===")
    print(f"Firms in pilot:                          {n_firms}")
    print(f"  with any 8-K filing:                   {n_with_filings} ({n_with_filings/n_firms:.0%})")
    print(f"  with at least one Item 2.02 filing:    {n_with_2_02} ({n_with_2_02/n_firms:.0%})")
    print(f"  with at least one earnings release:    {n_with_pr} ({n_with_pr/n_firms:.0%})")
    print(f"  with at least one full transcript:     {n_with_tr} ({n_with_tr/n_firms:.0%})")
    print()
    print(f"Total filings indexed: {len(det_df):,}")
    if not det_df.empty:
        print(f"Filings by exhibit class:")
        print(det_df["exhibit_class"].value_counts().to_string())


if __name__ == "__main__":
    main()
