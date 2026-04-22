"""
04_download_kurry_transcripts.py

Downloads the kurry/sp500_earnings_transcripts Hugging Face dataset into
raw_data/transcripts_kurry/ (gitignored) as a single parquet file, plus a
lightweight metadata CSV for joining with the project universe.

INTERIM USE ONLY. This dataset's provenance is not clearly documented —
see `.claude/.../open_investigation_kurry_dataset.md`. Treat it as a
stopgap for pipeline development while Tulane WRDS entitlement and
Kensho SPGISpeech access are pending. All work built on top of this
corpus must be labeled `*_kurry_*` so it can be re-run on clean WRDS
data without commingling.

Outputs:
    raw_data/transcripts_kurry/transcripts.parquet   (full data; gitignored)
    raw_data/transcripts_kurry/metadata.csv          (ticker, date, quarter;
                                                      no text; for inspection)
    processed_data/public/kurry_metadata_only.csv    (metadata without any
                                                      transcript text; safe
                                                      to publish for
                                                      reproducibility of
                                                      coverage checks)
"""

import sys
from pathlib import Path

import pandas as pd

try:
    from datasets import load_dataset
except ImportError:
    sys.exit("datasets not installed — run: venv/Scripts/pip install -r requirements.txt")


PROJECT_ROOT = Path(__file__).parent.parent
RAW_DIR = PROJECT_ROOT / "raw_data" / "transcripts_kurry"
PUBLIC_DIR = PROJECT_ROOT / "processed_data" / "public"
RAW_DIR.mkdir(parents=True, exist_ok=True)
PUBLIC_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_OUT = RAW_DIR / "transcripts.parquet"
META_OUT_FULL = RAW_DIR / "metadata.csv"
META_OUT_PUBLIC = PUBLIC_DIR / "kurry_metadata_only.csv"

DATASET_ID = "kurry/sp500_earnings_transcripts"


def main():
    print(f"Downloading {DATASET_ID} ...")
    print("(first run caches ~1.8 GB under ~/.cache/huggingface/)")
    ds = load_dataset(DATASET_ID, split="train")
    print(f"  loaded: {len(ds):,} records")
    print(f"  fields: {list(ds.features.keys())}")

    print("Converting to pandas and writing parquet...")
    df = ds.to_pandas()
    df.to_parquet(PARQUET_OUT, index=False)
    print(f"  wrote {PARQUET_OUT} ({PARQUET_OUT.stat().st_size / 1e6:.1f} MB)")

    # --- build metadata-only views (no transcript text) --------------------
    meta_cols = [c for c in df.columns if c not in ("content", "structured_content")]
    meta = df[meta_cols].copy()

    meta.to_csv(META_OUT_FULL, index=False)
    meta.to_csv(META_OUT_PUBLIC, index=False)
    print(f"  wrote {META_OUT_FULL}")
    print(f"  wrote {META_OUT_PUBLIC}")

    # --- quick coverage summary -------------------------------------------
    print()
    print(f"=== Coverage summary ===")
    print(f"Unique tickers: {df['symbol'].nunique():,}")
    print(f"Unique companies: {df['company_name'].nunique():,}")
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
    if "year" in df.columns:
        print(f"\nTranscripts per year:")
        print(df["year"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()
