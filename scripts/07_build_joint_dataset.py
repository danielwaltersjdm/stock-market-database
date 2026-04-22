"""
07_build_joint_dataset.py

Build the per-MAEC-call joint dataset combining:

    - MAEC call metadata (ticker, call date, transcript path)
    - Standing CIQ management guidance for the fiscal year being reported
    - Standing CIQ guidance for the next fiscal year (what they're now projecting)
    - I/B/E/S analyst consensus just before the call
    - I/B/E/S reported actual for the fiscal period being reported
    - Derived: beat/miss vs guidance, surprise vs consensus

This is the core research-ready artifact. One row per MAEC call.

Output
------
    processed_data/public/maec_joint_dataset.parquet
    processed_data/public/maec_joint_dataset.csv

Notes
-----
    - Period inference: we use the I/B/E/S summary file itself to identify
      which fiscal period (fpedats) each call is reporting — the most recent
      statpers before the call with fpedats < call date is the "reporting
      period" and the most recent statpers after with fpedats > call date
      is the "next period being guided."
    - Ticker mapping: MAEC tickers are matched to I/B/E/S tickers directly
      (both are standard US exchange tickers). Coverage reported.
    - Guidance is joined on fiscal-year text ('FY2015' etc.) inferred from
      the call's reporting period.
"""

import re
from pathlib import Path
from collections import Counter

import pandas as pd
import wrds

PROJECT_ROOT = Path(__file__).parent.parent
MAEC_ROOT = PROJECT_ROOT / "raw_data" / "transcripts" / "maec" / "MAEC_Dataset"
GUIDANCE_ALL = PROJECT_ROOT / "processed_data" / "public" / "ciq_guidance_all.csv"
UNIVERSE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
OUT_PARQUET = PROJECT_ROOT / "processed_data" / "public" / "maec_joint_dataset.parquet"
OUT_CSV = PROJECT_ROOT / "processed_data" / "public" / "maec_joint_dataset.csv"


def load_maec_calls() -> pd.DataFrame:
    """One row per MAEC call folder: ticker, call_date, path."""
    rows = []
    for p in MAEC_ROOT.iterdir():
        if not p.is_dir() or "_" not in p.name:
            continue
        datestr, ticker = p.name.split("_", 1)
        if not (len(datestr) == 8 and datestr.isdigit()):
            continue
        rows.append({
            "ticker": ticker,
            "call_date": pd.Timestamp(f"{datestr[:4]}-{datestr[4:6]}-{datestr[6:]}"),
            "maec_folder": p.name,
            "text_path": str((p / "text.txt").relative_to(PROJECT_ROOT)).replace("\\", "/"),
        })
    return pd.DataFrame(rows).sort_values(["ticker", "call_date"]).reset_index(drop=True)


def load_guidance() -> pd.DataFrame:
    """CIQ guidance long -> wide: one row per (ticker, period) with all measures."""
    g = pd.read_csv(GUIDANCE_ALL)
    # Keep only real values
    g = g[g["category"].isin(["number", "range", "bound"])].copy()
    # Normalize columns; parsed_value is the midpoint for ranges
    # Pivot to wide: measures become columns
    measure_to_col = {
        "EPS Excl. Excep/GW (point)": "guidance_eps_excl",
        "EPS Excl. Excep/GW High":    "guidance_eps_excl_high",
        "EPS Excl. Excep/GW Low":     "guidance_eps_excl_low",
        "Revenue (point)":            "guidance_revenue",
        "Revenue High":               "guidance_revenue_high",
        "Revenue Low":                "guidance_revenue_low",
        "EBITDA (point)":             "guidance_ebitda",
    }
    g["col"] = g["Measure"].map(measure_to_col)
    g = g.dropna(subset=["col"])
    wide = g.pivot_table(
        index=["Ticker", "Period"],
        columns="col",
        values="parsed_value",
        aggfunc="first",
    ).reset_index()
    wide = wide.rename(columns={"Ticker": "ticker", "Period": "period"})
    return wide


def wrds_pull_ibes(tickers: list[str], start: str = "2014-01-01",
                   end: str = "2020-06-30") -> pd.DataFrame:
    """
    Pull I/B/E/S summary (statsum_epsus) for all MAEC tickers. This table
    already contains BOTH the consensus stats (meanest, stdev, numest, ...)
    AND the realized `actual` value with its announcement date, so we get
    consensus + actual in one query.
    """
    print(f"Connecting to WRDS...")
    db = wrds.Connection(wrds_username="dwalters")

    def chunked(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    print(f"Pulling I/B/E/S summary (consensus + actual) for {len(tickers)} tickers...")
    parts = []
    for ck in chunked(tickers, 500):
        ticker_in = ", ".join(f"'{t}'" for t in ck)
        # Pull BOTH quarterly and annual — no fpi filter, keep fiscalp so we
        # can separate downstream.
        s = db.raw_sql(f"""
            SELECT ticker, cusip, cname, statpers, measure, fiscalp, fpi,
                   numest, medest, meanest, stdev, highest, lowest,
                   fpedats, actual, anndats_act
            FROM ibes.statsum_epsus
            WHERE ticker IN ({ticker_in})
              AND measure = 'EPS'
              AND fiscalp IN ('QTR', 'ANN')
              AND statpers BETWEEN '{start}' AND '{end}'
        """, date_cols=["fpedats", "statpers", "anndats_act"])
        parts.append(s)
    db.close()
    summary = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    print(f"  I/B/E/S summary rows: {len(summary):,}")
    print(f"    quarterly: {(summary['fiscalp'] == 'QTR').sum():,}")
    print(f"    annual:    {(summary['fiscalp'] == 'ANN').sum():,}")
    return summary


def infer_reporting_periods(calls: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    """
    For each call, identify TWO fiscal periods:
      - reporting_period_end_q : most recent quarterly fpedats <= call_date
      - reporting_period_end_a : most recent annual fpedats <= call_date (the FY
        containing this call; used to match to CIQ FY guidance labels)
    """
    print("Inferring reporting periods (quarterly + annual)...")

    def pick_latest(fp: str, col_name: str):
        s = summary[(summary["fiscalp"] == fp) &
                    summary["fpedats"].notna()][["ticker", "fpedats"]].drop_duplicates()
        merged = calls[["ticker", "call_date"]].merge(s, on="ticker", how="left")
        merged["days_offset"] = (merged["call_date"] - merged["fpedats"]).dt.days
        # quarterly lag <=120 days; annual lag <=400 days (call may happen
        # up to ~13 months after the FY end in some quarterly reports)
        cap = 120 if fp == "QTR" else 400
        merged = merged[(merged["days_offset"] >= 0) & (merged["days_offset"] <= cap)]
        idx = merged.groupby(["ticker", "call_date"])["days_offset"].idxmin()
        picked = merged.loc[idx, ["ticker", "call_date", "fpedats"]].rename(
            columns={"fpedats": col_name}
        )
        return picked

    q_pick = pick_latest("QTR", "reporting_period_end_q")
    a_pick = pick_latest("ANN", "reporting_period_end_a")
    out = calls.merge(q_pick, on=["ticker", "call_date"], how="left") \
               .merge(a_pick, on=["ticker", "call_date"], how="left")
    # keep a legacy column name for downstream compatibility
    out["reporting_period_end"] = out["reporting_period_end_q"]
    print(f"  Calls with quarterly period: {out['reporting_period_end_q'].notna().sum()} of {len(out)}")
    print(f"  Calls with annual period:    {out['reporting_period_end_a'].notna().sum()} of {len(out)}")
    return out


def attach_consensus(calls: pd.DataFrame, summary: pd.DataFrame,
                     fiscalp: str, period_col: str, prefix: str) -> pd.DataFrame:
    """Most recent consensus as of the day before the call, for the given fpedats."""
    s = summary[(summary["fiscalp"] == fiscalp)
                & summary["fpedats"].notna()
                & summary["statpers"].notna()].copy()
    joined = calls[["ticker", "call_date", period_col]].merge(
        s,
        left_on=["ticker", period_col],
        right_on=["ticker", "fpedats"],
        how="left",
    )
    joined = joined[joined["statpers"] < joined["call_date"]]
    idx = joined.groupby(["ticker", "call_date"])["statpers"].idxmax()
    picked = joined.loc[idx, [
        "ticker", "call_date", "statpers", "numest", "meanest",
        "medest", "stdev", "highest", "lowest",
    ]].rename(columns={
        "statpers":    f"{prefix}_consensus_statpers",
        "numest":      f"{prefix}_consensus_n_analysts",
        "meanest":     f"{prefix}_consensus_eps_mean",
        "medest":      f"{prefix}_consensus_eps_median",
        "stdev":       f"{prefix}_consensus_eps_stdev",
        "highest":     f"{prefix}_consensus_eps_high",
        "lowest":      f"{prefix}_consensus_eps_low",
    })
    out = calls.merge(picked, on=["ticker", "call_date"], how="left")
    mean_col = f"{prefix}_consensus_eps_mean"
    print(f"  Calls with {prefix} consensus: {out[mean_col].notna().sum()} of {len(out)}")
    return out


def attach_actuals(calls: pd.DataFrame, summary: pd.DataFrame,
                   fiscalp: str, period_col: str, prefix: str) -> pd.DataFrame:
    """Join the realized actual for the given (fiscalp, fpedats)."""
    s = summary[(summary["fiscalp"] == fiscalp)
                & summary["fpedats"].notna()
                & summary["actual"].notna()][[
        "ticker", "fpedats", "actual", "anndats_act"
    ]].drop_duplicates(["ticker", "fpedats"], keep="first")
    merged = calls.merge(
        s,
        left_on=["ticker", period_col],
        right_on=["ticker", "fpedats"],
        how="left",
    ).drop(columns=["fpedats"])
    merged = merged.rename(columns={
        "actual": f"{prefix}_actual_eps",
        "anndats_act": f"{prefix}_actual_announcement_date",
    })
    col = f"{prefix}_actual_eps"
    print(f"  Calls with {prefix} actual: {merged[col].notna().sum()} of {len(merged)}")
    return merged


def attach_guidance(calls: pd.DataFrame, guidance: pd.DataFrame) -> pd.DataFrame:
    """Attach guidance for current FY (being reported) and next FY."""
    # Fiscal year from the ANNUAL reporting period end (falls back to quarterly year)
    calls = calls.copy()
    fy_series = calls["reporting_period_end_a"].dt.year
    fy_series = fy_series.fillna(calls["reporting_period_end_q"].dt.year)
    calls["reporting_fy_int"] = fy_series
    calls["current_fy_label"] = "FY" + calls["reporting_fy_int"].astype("Int64").astype(str)
    calls["next_fy_label"] = "FY" + (calls["reporting_fy_int"] + 1).astype("Int64").astype(str)

    g_cur = guidance.rename(columns={c: f"cur_{c}" for c in guidance.columns if c.startswith("guidance_")})
    g_next = guidance.rename(columns={c: f"next_{c}" for c in guidance.columns if c.startswith("guidance_")})

    out = calls.merge(g_cur, left_on=["ticker", "current_fy_label"], right_on=["ticker", "period"], how="left")
    out = out.drop(columns=["period"])
    out = out.merge(g_next, left_on=["ticker", "next_fy_label"], right_on=["ticker", "period"], how="left",
                    suffixes=("", "_next"))
    out = out.drop(columns=["period"])

    print(f"  Calls with current-FY guidance: "
          f"{out['cur_guidance_eps_excl'].notna().sum()} of {len(out)}")
    print(f"  Calls with next-FY guidance: "
          f"{out['next_guidance_eps_excl'].notna().sum()} of {len(out)}")
    return out


def compute_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Beat/miss and surprise measures at both quarterly and annual level."""
    df = df.copy()

    def mid(row, hi, lo, point):
        h, l, p = row.get(hi), row.get(lo), row.get(point)
        if pd.notna(h) and pd.notna(l):
            return (h + l) / 2
        if pd.notna(p):
            return p
        return None

    df["guidance_eps_midpoint"] = df.apply(
        lambda r: mid(r, "cur_guidance_eps_excl_high",
                      "cur_guidance_eps_excl_low",
                      "cur_guidance_eps_excl"),
        axis=1,
    )

    # --- Quarterly: actual vs analyst consensus (analyst surprise) --------
    df["qtr_surprise_vs_consensus"] = df["q_actual_eps"] - df["q_consensus_eps_mean"]
    df["qtr_beat_consensus"] = df["qtr_surprise_vs_consensus"] > 0

    # --- Annual: actual vs analyst consensus -----------------------------
    df["ann_surprise_vs_consensus"] = df["a_actual_eps"] - df["a_consensus_eps_mean"]
    df["ann_beat_consensus"] = df["ann_surprise_vs_consensus"] > 0

    # --- Annual: actual vs management guidance (guidance-hit) ------------
    df["ann_surprise_vs_guidance"] = df["a_actual_eps"] - df["guidance_eps_midpoint"]
    df["ann_beat_guidance_midpoint"] = df["ann_surprise_vs_guidance"] > 0

    def in_range(r):
        h, l, a = (r.get("cur_guidance_eps_excl_high"),
                   r.get("cur_guidance_eps_excl_low"),
                   r.get("a_actual_eps"))
        if pd.isna(h) or pd.isna(l) or pd.isna(a):
            return None
        return bool(l <= a <= h)
    df["annual_actual_in_guidance_range"] = df.apply(in_range, axis=1)
    return df


def main():
    calls = load_maec_calls()
    print(f"MAEC calls loaded: {len(calls):,} across {calls['ticker'].nunique()} tickers")

    guidance = load_guidance()
    print(f"CIQ guidance (wide): {len(guidance):,} ticker-period rows, "
          f"{guidance['ticker'].nunique()} firms")

    summary = wrds_pull_ibes(sorted(calls["ticker"].unique().tolist()))

    calls = infer_reporting_periods(calls, summary)
    calls = attach_consensus(calls, summary, "QTR", "reporting_period_end_q", "q")
    calls = attach_actuals(calls, summary, "QTR", "reporting_period_end_q", "q")
    calls = attach_consensus(calls, summary, "ANN", "reporting_period_end_a", "a")
    calls = attach_actuals(calls, summary, "ANN", "reporting_period_end_a", "a")
    calls = attach_guidance(calls, guidance)
    calls = compute_derived(calls)

    print()
    print(f"Final rows: {len(calls):,}")
    print(f"  with quarterly consensus+actual: {(calls['q_actual_eps'].notna() & calls['q_consensus_eps_mean'].notna()).sum():>5}")
    print(f"  with annual consensus+actual:    {(calls['a_actual_eps'].notna() & calls['a_consensus_eps_mean'].notna()).sum():>5}")
    print(f"  with any CIQ guidance:           {calls['cur_guidance_eps_excl'].notna().sum():>5}")
    print(f"  with annual in-range flag:       {calls['annual_actual_in_guidance_range'].notna().sum():>5}")
    print(f"  with annual beat-guidance flag:  {calls['ann_beat_guidance_midpoint'].notna().sum():>5}")

    try:
        calls.to_parquet(OUT_PARQUET, index=False)
    except Exception as e:
        print(f"  parquet failed ({e}); CSV only")
    calls.to_csv(OUT_CSV, index=False)
    print(f"\nSaved: {OUT_CSV}")


if __name__ == "__main__":
    main()
