"""
01_build_universe.py

Builds the working universe for the project: a quarterly panel of firm-quarters
approximating Russell 3000 membership from 2005 through 2025.

Approach: CRSP-based approximation. At each calendar quarter-end, rank US common
stocks (sharecode 10 or 11) by market cap and take the top 3000. This is a
standard research-grade proxy for Russell 3000 that avoids FTSE Russell
registration. Documented caveat: excludes non-CRSP securities and treats index
reconstitution smoothly rather than at FTSE's annual June rebalance.

Output:
    processed_data/public/universe_russell3000_proxy.parquet
        one row per (permno, quarter_end_date) with:
        permno, quarter_end, mktcap_usd, prc, shrout, ticker, comnam,
        gvkey (via CCM link), permco, siccd, exchcd

Runtime: ~2-5 minutes against WRDS.
"""

import sys
from pathlib import Path

import pandas as pd
import wrds

PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = PROJECT_ROOT / "processed_data" / "public"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "universe_russell3000_proxy.parquet"

START_YEAR = 2005
END_YEAR = 2025
TOP_N = 3000


def main():
    db = wrds.Connection()

    print(f"Pulling CRSP monthly stock file {START_YEAR}-01 through {END_YEAR}-12...")
    # msf is the workhorse table: monthly prices, shares outstanding, returns.
    # We restrict to common stock (share codes 10, 11) on NYSE/AMEX/NASDAQ (exchcd 1, 2, 3).
    msf = db.raw_sql(
        f"""
        SELECT permno, permco, date, prc, shrout, cfacpr, cfacshr,
               ret, vol
        FROM crsp.msf
        WHERE date BETWEEN '{START_YEAR}-01-01' AND '{END_YEAR}-12-31'
        """,
        date_cols=["date"],
    )

    # Pull firm-level metadata (share code, exchange, SIC, ticker, name)
    # msenames has start/end dates per (permno, [nameendt range]); we'll merge by date.
    print("Pulling CRSP msenames for share codes and names...")
    msenames = db.raw_sql(
        """
        SELECT permno, namedt, nameendt, shrcd, exchcd, siccd, ticker, comnam
        FROM crsp.msenames
        """,
        date_cols=["namedt", "nameendt"],
    )

    print("Pulling CCM linking table (GVKEY <-> PERMNO)...")
    ccm = db.raw_sql(
        """
        SELECT gvkey, lpermno AS permno, linkdt, linkenddt, linktype, linkprim
        FROM crsp.ccmxpf_linktable
        WHERE linktype IN ('LU', 'LC')
          AND linkprim IN ('P', 'C')
        """,
        date_cols=["linkdt", "linkenddt"],
    )

    db.close()
    print(f"  msf rows: {len(msf):>10,}")
    print(f"  msenames: {len(msenames):>10,}")
    print(f"  ccm:      {len(ccm):>10,}")

    # --- merge msenames onto msf by date range -----------------------------
    print("Merging names/metadata onto msf...")
    m = msf.merge(msenames, on="permno", how="left")
    m = m[(m["date"] >= m["namedt"]) & (m["date"] <= m["nameendt"].fillna(pd.Timestamp("2099-12-31")))]

    # Keep only US common stock on major exchanges
    m = m[m["shrcd"].isin([10, 11])]
    m = m[m["exchcd"].isin([1, 2, 3])]

    # Market cap (|prc| because CRSP negates prc when it's a bid/ask midpoint)
    m["mktcap_usd"] = m["prc"].abs() * m["shrout"] * 1000  # shrout is in thousands

    # --- restrict to calendar quarter-ends ---------------------------------
    # CRSP msf date is month-end. Keep only March/June/Sept/Dec.
    m["month"] = m["date"].dt.month
    m = m[m["month"].isin([3, 6, 9, 12])]
    m = m.rename(columns={"date": "quarter_end"})

    # --- rank by market cap within each quarter, keep top N ----------------
    print(f"Taking top {TOP_N} by market cap at each quarter-end...")
    m["rank"] = m.groupby("quarter_end")["mktcap_usd"].rank(method="first", ascending=False)
    universe = m[m["rank"] <= TOP_N].copy()

    # --- merge GVKEY via CCM -----------------------------------------------
    print("Merging GVKEY via CCM...")
    ccm_expanded = ccm.copy()
    ccm_expanded["linkenddt"] = ccm_expanded["linkenddt"].fillna(pd.Timestamp("2099-12-31"))
    universe = universe.merge(ccm_expanded, on="permno", how="left")
    universe = universe[
        (universe["quarter_end"] >= universe["linkdt"].fillna(pd.Timestamp("1900-01-01")))
        & (universe["quarter_end"] <= universe["linkenddt"])
    ]

    # One row per (permno, quarter_end); if multiple GVKEYs link, take primary
    universe = universe.sort_values(
        ["permno", "quarter_end", "linkprim"]
    ).drop_duplicates(["permno", "quarter_end"], keep="first")

    cols = [
        "permno", "permco", "gvkey", "quarter_end",
        "ticker", "comnam", "siccd", "exchcd", "shrcd",
        "prc", "shrout", "mktcap_usd", "ret", "vol",
    ]
    universe = universe[cols].reset_index(drop=True)

    print()
    print(f"Universe: {len(universe):,} firm-quarter observations")
    print(f"  quarters: {universe['quarter_end'].nunique()}")
    print(f"  unique permnos: {universe['permno'].nunique():,}")
    print(f"  unique gvkeys: {universe['gvkey'].nunique():,}  "
          f"({universe['gvkey'].isna().sum():,} unlinked)")

    universe.to_parquet(OUTPUT_FILE, index=False)
    print(f"\nWritten: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
