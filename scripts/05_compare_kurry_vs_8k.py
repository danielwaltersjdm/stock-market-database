"""
05_compare_kurry_vs_8k.py

Once both the kurry dataset download (script 04) and the 8-K coverage pilot
(script 03) have completed, compare their coverage against our Russell 3000
proxy universe. Specifically:

  A) What fraction of our top-500 pilot firms are covered by kurry?
  B) What fraction are covered by 8-K press releases?
  C) What fraction are covered by 8-K full transcripts (rare)?
  D) Overlap / complementarity between kurry and 8-K

Output:
    processed_data/public/coverage_comparison.csv  (per-firm yes/no matrix)
    processed_data/public/coverage_summary.txt     (aggregate stats)

Runs offline against files produced by scripts 03 and 04.
"""

import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent
UNIVERSE_FILE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
EIGHTK_SUMMARY = PROJECT_ROOT / "processed_data" / "public" / "pilot_8k_coverage_summary.csv"
KURRY_META = PROJECT_ROOT / "processed_data" / "public" / "kurry_metadata_only.csv"

COMP_OUT = PROJECT_ROOT / "processed_data" / "public" / "coverage_comparison.csv"
SUMMARY_OUT = PROJECT_ROOT / "processed_data" / "public" / "coverage_summary.txt"

PILOT_YEAR_END = 2023
TOP_N = 500
TARGET_YEAR = 2023


def require(path: Path, name: str) -> None:
    if not path.exists():
        sys.exit(f"{name} not found at {path}. Run its upstream script first.")


def main():
    require(UNIVERSE_FILE, "universe")
    require(EIGHTK_SUMMARY, "8-K pilot summary")
    require(KURRY_META, "kurry metadata")

    universe = pd.read_parquet(UNIVERSE_FILE)
    year_slice = universe[universe["quarter_end"].dt.year == PILOT_YEAR_END]
    pilot_quarter = year_slice["quarter_end"].max()
    pilot_firms = (
        universe[universe["quarter_end"] == pilot_quarter]
        .nlargest(TOP_N, "mktcap_usd")
        [["permno", "ticker", "comnam", "mktcap_usd"]]
        .copy()
    )
    pilot_firms["ticker_clean"] = pilot_firms["ticker"].astype(str).str.upper().str.strip()

    eightk = pd.read_csv(EIGHTK_SUMMARY)
    eightk["ticker_clean"] = eightk["ticker"].astype(str).str.upper().str.strip()

    kurry = pd.read_csv(KURRY_META)
    kurry["ticker_clean"] = kurry["symbol"].astype(str).str.upper().str.strip()
    if "year" in kurry.columns:
        kurry_year = kurry[kurry["year"] == TARGET_YEAR]
    elif "date" in kurry.columns:
        kurry["date"] = pd.to_datetime(kurry["date"], errors="coerce")
        kurry_year = kurry[kurry["date"].dt.year == TARGET_YEAR]
    else:
        kurry_year = kurry
    kurry_tickers = set(kurry_year["ticker_clean"].unique())
    kurry_all_tickers = set(kurry["ticker_clean"].unique())

    # --- per-firm coverage matrix -----------------------------------------
    def firm_row(row):
        t = row["ticker_clean"]
        e = eightk[eightk["ticker_clean"] == t]
        has_pr = bool(e["any_press_release"].any()) if not e.empty else False
        has_tr = bool(e["any_transcript"].any()) if not e.empty else False
        n_kurry_2023 = int((kurry_year["ticker_clean"] == t).sum())
        n_kurry_all = int((kurry["ticker_clean"] == t).sum())
        return pd.Series({
            "ticker": row["ticker"],
            "comnam": row["comnam"],
            "mktcap_usd": row["mktcap_usd"],
            "has_8k_press_release_2023": has_pr,
            "has_8k_transcript_2023": has_tr,
            "kurry_transcripts_2023": n_kurry_2023,
            "kurry_transcripts_alltime": n_kurry_all,
            "in_kurry_corpus": n_kurry_all > 0,
        })

    comp = pilot_firms.apply(firm_row, axis=1)
    comp.to_csv(COMP_OUT, index=False)
    print(f"Wrote per-firm comparison: {COMP_OUT}")

    # --- aggregate summary -------------------------------------------------
    n = len(comp)
    stats = [
        f"Universe pilot: top {n} firms by mktcap at {pilot_quarter.date()}",
        "",
        f"8-K coverage ({TARGET_YEAR}):",
        f"  firms with >=1 press release:   {comp['has_8k_press_release_2023'].sum():>4d} / {n}  ({comp['has_8k_press_release_2023'].mean():.0%})",
        f"  firms with >=1 transcript:      {comp['has_8k_transcript_2023'].sum():>4d} / {n}  ({comp['has_8k_transcript_2023'].mean():.0%})",
        "",
        f"kurry coverage:",
        f"  firms in corpus (any time):     {comp['in_kurry_corpus'].sum():>4d} / {n}  ({comp['in_kurry_corpus'].mean():.0%})",
        f"  firms with {TARGET_YEAR} transcripts:   {(comp['kurry_transcripts_2023'] > 0).sum():>4d} / {n}",
        f"  mean transcripts/firm {TARGET_YEAR}:    {comp['kurry_transcripts_2023'].mean():.2f}",
        f"  mean transcripts/firm alltime:  {comp['kurry_transcripts_alltime'].mean():.2f}",
        "",
        f"Overlap (2023):",
    ]
    both_pr = comp[comp["has_8k_press_release_2023"] & (comp["kurry_transcripts_2023"] > 0)]
    only_8k = comp[comp["has_8k_press_release_2023"] & (comp["kurry_transcripts_2023"] == 0)]
    only_kurry = comp[~comp["has_8k_press_release_2023"] & (comp["kurry_transcripts_2023"] > 0)]
    neither = comp[~comp["has_8k_press_release_2023"] & (comp["kurry_transcripts_2023"] == 0)]
    stats += [
        f"  in both (8-K PR + kurry):       {len(both_pr):>4d}",
        f"  only in 8-K PR:                 {len(only_8k):>4d}",
        f"  only in kurry:                  {len(only_kurry):>4d}",
        f"  in neither:                     {len(neither):>4d}",
        "",
        f"Total kurry corpus: {len(kurry):,} transcript records across {len(kurry_all_tickers):,} tickers",
        f"  Target-year records: {len(kurry_year):,}",
    ]

    body = "\n".join(stats)
    print()
    print(body)
    with open(SUMMARY_OUT, "w", encoding="utf-8") as f:
        f.write(body)
    print(f"\nWrote summary: {SUMMARY_OUT}")


if __name__ == "__main__":
    main()
