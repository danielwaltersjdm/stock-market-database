"""
02_pilot_forecasts.py

Pilot end-to-end pull for I/B/E/S analyst forecasts.

Takes top 50 firms by market cap at year-end 2023 from the built universe,
maps PERMNO -> CUSIP -> IBES ticker via ibes.id, and pulls analyst-level
quarterly EPS forecasts for 2023.

Purpose:
    - Validate I/B/E/S detail-forecast schema
    - Validate PERMNO <-> IBES ticker linkage via ibes.id
    - Produce a small sample so we know field layouts before scaling

Outputs:
    processed_data/public/pilot_forecasts_schema.txt
    processed_data/public/pilot_forecasts_detail.csv
    processed_data/public/pilot_forecasts_summary.csv
"""

import sys
from pathlib import Path

import pandas as pd
import wrds

PROJECT_ROOT = Path(__file__).parent.parent
UNIVERSE_FILE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
PUBLIC_DIR = PROJECT_ROOT / "processed_data" / "public"
SCHEMA_OUTPUT = PUBLIC_DIR / "pilot_forecasts_schema.txt"
DET_OUTPUT = PUBLIC_DIR / "pilot_forecasts_detail.csv"
SUM_OUTPUT = PUBLIC_DIR / "pilot_forecasts_summary.csv"

PILOT_YEAR_END = 2023
TOP_N = 50
YEAR = 2023


def main():
    if not UNIVERSE_FILE.exists():
        sys.exit(f"Universe file not found: {UNIVERSE_FILE}\nRun 01_build_universe.py first.")

    universe = pd.read_parquet(UNIVERSE_FILE)
    year_slice = universe[universe["quarter_end"].dt.year == PILOT_YEAR_END]
    pilot_quarter = year_slice["quarter_end"].max()
    pilot_firms = (
        universe[universe["quarter_end"] == pilot_quarter]
        .nlargest(TOP_N, "mktcap_usd")
        [["permno", "gvkey", "ticker", "comnam", "mktcap_usd"]]
        .reset_index(drop=True)
    )
    print(f"Pilot quarter: {pilot_quarter.date()}   firms: {len(pilot_firms)}")

    permnos = pilot_firms["permno"].astype(int).unique().tolist()
    db = wrds.Connection(wrds_username="dwalters")

    # --- Save schemas of the three target tables ---------------------------
    with open(SCHEMA_OUTPUT, "w", encoding="utf-8") as f:
        for tbl in ["id", "det_epsus", "statsum_epsus"]:
            desc = db.describe_table(library="ibes", table=tbl)
            f.write(f"ibes.{tbl}\n")
            f.write(desc.to_string())
            f.write("\n\n")
    print(f"Schemas saved: {SCHEMA_OUTPUT}")

    # --- PERMNO -> CUSIP (via CRSP msenames) -------------------------------
    permno_list = ", ".join(str(p) for p in permnos)
    cusip_map = db.raw_sql(f"""
        SELECT DISTINCT permno, ncusip AS cusip
        FROM crsp.msenames
        WHERE permno IN ({permno_list})
          AND ncusip IS NOT NULL AND ncusip <> ''
    """)
    cusips = cusip_map["cusip"].unique().tolist()
    print(f"PERMNO -> CUSIP: {len(cusip_map)} rows, {len(cusips)} unique CUSIPs")

    # --- CUSIP -> IBES ticker (via ibes.id) --------------------------------
    cusip_list = ", ".join(f"'{c}'" for c in cusips)
    ibes_map = db.raw_sql(f"""
        SELECT DISTINCT ticker, cusip, cname, sdates
        FROM ibes.id
        WHERE cusip IN ({cusip_list})
    """, date_cols=["sdates"])
    tickers = ibes_map["ticker"].unique().tolist()
    print(f"CUSIP -> IBES ticker: {len(ibes_map)} rows, {len(tickers)} unique tickers")

    if not tickers:
        sys.exit("No IBES tickers matched — check ibes.id schema or CUSIP mapping.")

    ticker_list = ", ".join(f"'{t}'" for t in tickers)

    # --- Detail forecasts (analyst × firm × forecast period) ---------------
    print(f"\nPulling ibes.det_epsus for {YEAR}...")
    detail = db.raw_sql(f"""
        SELECT *
        FROM ibes.det_epsus
        WHERE ticker IN ({ticker_list})
          AND anndats BETWEEN '{YEAR}-01-01' AND '{YEAR}-12-31'
        ORDER BY ticker, fpedats, anndats
    """, date_cols=["fpedats", "anndats", "anndats_act", "actdats_act"])
    print(f"  {len(detail):,} detail forecast rows")

    # --- Consensus summary (firm × forecast period × statistics period) ----
    print(f"Pulling ibes.statsum_epsus for {YEAR}...")
    summary = db.raw_sql(f"""
        SELECT *
        FROM ibes.statsum_epsus
        WHERE ticker IN ({ticker_list})
          AND statpers BETWEEN '{YEAR}-01-01' AND '{YEAR}-12-31'
        ORDER BY ticker, fpedats, statpers
    """, date_cols=["fpedats", "statpers"])
    print(f"  {len(summary):,} summary rows")

    db.close()

    # --- Merge firm identifiers back onto both outputs ---------------------
    firm_cols = pilot_firms[["permno", "gvkey", "ticker", "comnam", "mktcap_usd"]].rename(
        columns={"ticker": "crsp_ticker"}
    )
    cross = cusip_map.merge(ibes_map[["ticker", "cusip"]], on="cusip", how="inner") \
                     .merge(firm_cols, on="permno", how="left")

    detail_out = detail.merge(
        cross[["ticker", "permno", "gvkey", "crsp_ticker", "comnam", "mktcap_usd"]].drop_duplicates(),
        on="ticker", how="left",
    )
    summary_out = summary.merge(
        cross[["ticker", "permno", "gvkey", "crsp_ticker", "comnam", "mktcap_usd"]].drop_duplicates(),
        on="ticker", how="left",
    )

    detail_out.to_csv(DET_OUTPUT, index=False)
    summary_out.to_csv(SUM_OUTPUT, index=False)
    print(f"\nSaved: {DET_OUTPUT}")
    print(f"Saved: {SUM_OUTPUT}")

    # --- Sanity-check summary ---------------------------------------------
    print(f"\nDetail coverage: {detail_out['ticker'].nunique()} tickers, "
          f"{detail_out['estimator'].nunique()} brokers, "
          f"{detail_out['analys'].nunique()} analysts")
    print(f"Forecast periods (fpi):")
    print(detail_out["fpi"].value_counts().head(10).to_string())
    print(f"\nTop 10 firms by forecast count:")
    print(detail_out["ticker"].value_counts().head(10).to_string())


if __name__ == "__main__":
    main()
