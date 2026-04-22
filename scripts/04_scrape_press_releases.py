"""
04_scrape_press_releases.py

Pull earnings-release press releases from SEC EDGAR 8-K Item 2.02 exhibits
for the built Russell 3000 universe, 2005–2025.

Documents are saved to raw_data/filings/press_releases/ (gitignored, licensed-
content path). A metadata index is maintained at
processed_data/public/press_releases_index.parquet with one row per document.

Design
------
Resumable: firms already indexed are skipped on re-runs. State lives in
.checkpoint/press_release_progress.json and the index parquet itself.
Rate-limited: target ~6 req/sec against SEC EDGAR (10 req/sec is the cap).
Scoped: run on a configurable subset (TOP_N) first; set to None for full universe.

Pipeline per firm:
    1. Fetch submissions JSON, list all 8-K filings in year range with Item 2.02
    2. For each Item 2.02 filing, fetch the filing directory
    3. Identify the exhibit 99.1 HTM/HTML file (press release)
    4. Download and save to raw_data/filings/press_releases/{ticker}/
    5. Append row to index and checkpoint

Usage
-----
    # Smoke test (top 50 firms at 2023Q4, single year)
    python scripts/04_scrape_press_releases.py --top 50 --start-year 2023 --end-year 2023

    # Scale run (top 500 firms, full range)
    python scripts/04_scrape_press_releases.py --top 500

    # Full universe, full range (~60 hours)
    python scripts/04_scrape_press_releases.py
"""

import argparse
import json
import re
import sys
import time
import urllib3
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).parent.parent
UNIVERSE_FILE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
INDEX_FILE = PROJECT_ROOT / "processed_data" / "public" / "press_releases_index.parquet"
DOCS_DIR = PROJECT_ROOT / "raw_data" / "filings" / "press_releases"
CHECKPOINT_FILE = PROJECT_ROOT / ".checkpoint" / "press_release_progress.json"

USER_AGENT = "Daniel Walters Tulane University djw307@gmail.com"
SEC_HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}
TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
REQ_INTERVAL = 0.18  # ~5.5 req/sec

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

EX99_1_RE = re.compile(
    r"(?:^|[/_\-])ex[-_]?99[-_]?1\b|ex991\b|exhibit[-_]?99[-_]?1",
    re.IGNORECASE,
)
IGNORE_SUFFIXES = (".xml", ".xsd", ".css", ".js", ".jpg", ".gif",
                   ".png", ".xlsx", ".zip", ".txt")


def load_checkpoint() -> set[str]:
    if CHECKPOINT_FILE.exists():
        return set(json.loads(CHECKPOINT_FILE.read_text()).get("done_tickers", []))
    return set()


def save_checkpoint(done: set[str]) -> None:
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({"done_tickers": sorted(done)}))


def fetch_json(url: str) -> dict | None:
    try:
        r = requests.get(url, headers=SEC_HEADERS, verify=False, timeout=30)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_bytes(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=SEC_HEADERS, verify=False, timeout=60)
        if r.status_code != 200:
            return None
        return r.content
    except Exception:
        return None


def get_universe_firms(top_n: int | None) -> pd.DataFrame:
    if not UNIVERSE_FILE.exists():
        sys.exit(f"Universe not found: {UNIVERSE_FILE}. Run 01_build_universe.py first.")
    u = pd.read_parquet(UNIVERSE_FILE)
    latest = u[u["quarter_end"].dt.year == u["quarter_end"].dt.year.max()]
    latest_q = latest["quarter_end"].max()
    cross_section = u[u["quarter_end"] == latest_q]
    if top_n:
        cross_section = cross_section.nlargest(top_n, "mktcap_usd")
    # Return one row per ticker with latest-quarter ranking
    return (cross_section[["permno", "gvkey", "ticker", "comnam", "mktcap_usd"]]
            .dropna(subset=["ticker"])
            .drop_duplicates(subset=["ticker"])
            .reset_index(drop=True))


def fetch_ticker_cik_map() -> dict[str, str]:
    data = fetch_json(TICKER_MAP_URL)
    if not data:
        sys.exit("Failed to fetch SEC ticker->CIK map")
    out = {}
    for row in data.values():
        t = str(row["ticker"]).upper().strip()
        out[t] = str(row["cik_str"]).zfill(10)
    return out


def fetch_filing_index_html(cik: str, accession: str) -> list[dict]:
    """
    Parse the filing's -index.htm page which lists exhibits with official Type
    labels (EX-99.1, 8-K, etc.) from the Description/Type columns.

    Returns list of dicts: {seq, description, document, type, size_bytes}.
    """
    acc_clean = accession.replace("-", "")
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{accession}-index.htm"
    try:
        r = requests.get(url, headers=SEC_HEADERS, verify=False, timeout=30)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
    except Exception:
        return []

    entries = []
    for table in soup.find_all("table", class_="tableFile"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            # Cells: Seq | Description | Document | Type | Size
            seq = cells[0].get_text(strip=True)
            description = cells[1].get_text(strip=True)
            doc_link = cells[2].find("a")
            document = doc_link.get_text(strip=True) if doc_link else cells[2].get_text(strip=True)
            # Strip trailing "iXBRL" inline label some filings add
            document = re.sub(r"\s*iXBRL\s*$", "", document, flags=re.IGNORECASE).strip()
            type_ = cells[3].get_text(strip=True)
            try:
                size_bytes = int(cells[4].get_text(strip=True))
            except ValueError:
                size_bytes = 0
            entries.append({
                "seq": seq,
                "description": description,
                "document": document,
                "type": type_,
                "size_bytes": size_bytes,
            })
    return entries


def pick_press_release(entries: list[dict]) -> dict | None:
    """
    Preference order:
      1. Type == 'EX-99.1' (canonical earnings press release exhibit)
      2. Type starts with 'EX-99' AND description mentions 'press release'
      3. Description contains 'press release' or 'earnings release'
      4. Type == '8-K' main document (last resort; release may be inline)
    """
    by_type = {}
    for e in entries:
        t = (e.get("type") or "").upper().strip()
        if t and t not in by_type:
            by_type[t] = e

    # 1
    if "EX-99.1" in by_type:
        return {**by_type["EX-99.1"], "match_reason": "type_ex99_1"}

    # 2
    for e in entries:
        t = (e.get("type") or "").upper()
        d = (e.get("description") or "").lower()
        if t.startswith("EX-99") and ("press release" in d or "earnings" in d):
            return {**e, "match_reason": "type_ex99_and_desc"}

    # 3
    for e in entries:
        d = (e.get("description") or "").lower()
        if "press release" in d or "earnings release" in d:
            return {**e, "match_reason": "desc_only"}

    # 4
    if "8-K" in by_type:
        return {**by_type["8-K"], "match_reason": "main_8k_fallback"}

    return None


def process_firm(row: pd.Series, cik: str, start_year: int, end_year: int,
                 index_rows: list[dict]) -> int:
    """Returns count of press releases saved for this firm."""
    ticker = row["ticker"]
    firm_dir = DOCS_DIR / ticker
    firm_dir.mkdir(parents=True, exist_ok=True)

    # Fetch submissions (all filings)
    submissions = fetch_json(f"https://data.sec.gov/submissions/CIK{cik}.json")
    time.sleep(REQ_INTERVAL)
    if not submissions:
        return 0

    recent = submissions.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    accs = recent.get("accessionNumber", [])
    items_list = recent.get("items", [])

    # Also check older filings stored in separate files
    older_files = submissions.get("filings", {}).get("files", [])

    # For each Item 2.02 8-K in year range, download its press-release exhibit
    def process_filing_list(forms_, dates_, accs_, items_, saved):
        for i, form in enumerate(forms_):
            if form != "8-K":
                continue
            items = items_[i] if i < len(items_) else ""
            if "2.02" not in (items or ""):
                continue
            filing_date = dates_[i]
            year = int(filing_date[:4])
            if year < start_year or year > end_year:
                continue

            accession = accs_[i]
            acc_clean = accession.replace("-", "")
            entries = fetch_filing_index_html(cik, accession)
            time.sleep(REQ_INTERVAL)
            if not entries:
                continue

            exhibit = pick_press_release(entries)
            if not exhibit:
                continue

            exhibit_name = exhibit["document"]
            if not exhibit_name or exhibit_name.lower().endswith(IGNORE_SUFFIXES):
                continue

            doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{exhibit_name}"
            local_filename = f"{filing_date}_{accession}_{exhibit_name}"
            local_path = firm_dir / local_filename

            if not local_path.exists():
                body = fetch_bytes(doc_url)
                time.sleep(REQ_INTERVAL)
                if not body:
                    continue
                local_path.write_bytes(body)

            index_rows.append({
                "ticker": ticker,
                "gvkey": row.get("gvkey"),
                "permno": row.get("permno"),
                "comnam": row.get("comnam"),
                "cik": cik,
                "filing_date": filing_date,
                "accession": accession,
                "items": items,
                "exhibit_name": exhibit_name,
                "exhibit_type": exhibit.get("type", ""),
                "exhibit_description": exhibit.get("description", ""),
                "match_reason": exhibit.get("match_reason", ""),
                "local_path": str(local_path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
                "size_bytes": int(exhibit.get("size_bytes", 0) or 0),
            })
            saved[0] += 1

    saved = [0]
    process_filing_list(forms, dates, accs, items_list, saved)

    # Older filings (beyond recent 1000)
    for older_ref in older_files:
        older_url = f"https://data.sec.gov/submissions/{older_ref['name']}"
        older_data = fetch_json(older_url)
        time.sleep(REQ_INTERVAL)
        if not older_data:
            continue
        process_filing_list(
            older_data.get("form", []),
            older_data.get("filingDate", []),
            older_data.get("accessionNumber", []),
            older_data.get("items", []),
            saved,
        )

    return saved[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=None,
                    help="Restrict to top N firms by market cap at latest universe quarter")
    ap.add_argument("--start-year", type=int, default=2005)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--force", action="store_true",
                    help="Ignore checkpoint and re-process all firms")
    args = ap.parse_args()

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)

    firms = get_universe_firms(args.top)
    print(f"Universe cross-section: {len(firms)} firms "
          f"(top {args.top or 'ALL'} by mktcap)")

    ticker_cik = fetch_ticker_cik_map()
    time.sleep(REQ_INTERVAL)

    firms["ticker_up"] = firms["ticker"].astype(str).str.upper().str.strip()
    firms["cik"] = firms["ticker_up"].map(ticker_cik)
    unmatched = firms["cik"].isna().sum()
    firms = firms.dropna(subset=["cik"]).reset_index(drop=True)
    print(f"Matched to SEC CIKs: {len(firms)} (unmatched: {unmatched})")

    done = set() if args.force else load_checkpoint()
    existing_index = pd.read_parquet(INDEX_FILE) if INDEX_FILE.exists() else pd.DataFrame()
    new_rows: list[dict] = []

    t0 = time.time()
    total_saved = 0
    for idx, row in firms.iterrows():
        ticker = row["ticker"]
        if ticker in done:
            continue
        try:
            n = process_firm(row, row["cik"],
                             args.start_year, args.end_year, new_rows)
            total_saved += n
        except Exception as e:
            print(f"  ERROR {ticker}: {type(e).__name__}: {e}", flush=True)
            continue

        done.add(ticker)

        # Periodic persistence
        if (idx + 1) % 10 == 0 or idx == len(firms) - 1:
            if new_rows:
                new_df = pd.DataFrame(new_rows)
                combined = pd.concat([existing_index, new_df], ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=["ticker", "accession", "exhibit_name"], keep="last"
                )
                combined.to_parquet(INDEX_FILE, index=False)
                existing_index = combined
                new_rows = []
            save_checkpoint(done)
            elapsed = time.time() - t0
            print(f"  [{idx+1}/{len(firms)}] firms done  "
                  f"docs={total_saved}  {elapsed:.0f}s elapsed", flush=True)

    print(f"\nDone. Total press releases saved this run: {total_saved}")
    if INDEX_FILE.exists():
        final = pd.read_parquet(INDEX_FILE)
        print(f"Index file: {INDEX_FILE}  rows={len(final):,}  tickers={final['ticker'].nunique()}")


if __name__ == "__main__":
    main()
