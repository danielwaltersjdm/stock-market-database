"""
02_pilot_transcripts.py

Pilot end-to-end transcript pull. Picks the top 50 firms by market cap from
the built universe as of 2023 Q4, then pulls their earnings call transcripts
from Capital IQ's transcript dataset for that quarter.

Purpose:
    - Validate WRDS CIQ transcript schema (table/column names)
    - Validate GVKEY <-> CIQ company_id linkage
    - Produce a handful of text files and a metadata CSV
    - Time the pull so we can extrapolate to full universe

Outputs:
    raw_data/transcripts/pilot/{transcriptid}.txt
    processed_data/public/pilot_transcripts_metadata.csv

Note on CIQ schema:
    Table/column names in ciq_transcripts vary by WRDS release. This script
    uses the common 2023+ layout:
      wrds_transcript_detail — one row per transcript (transcriptid, date,
        companyid, companyname, keydeveventid, keydevtypeid, keydevtypename)
      ciqtranscriptcomponent — text chunks (transcriptid, componentorder,
        componenttext, transcriptcomponenttypeid, transcriptpersonid)
      ciqtranscriptperson — speaker info (transcriptpersonid, personname,
        personfirstname, personlastname, title, companyname, speakertypeid)
    If a column is missing at runtime, inspect with:
        db.describe_table(library="ciq_transcripts", table="wrds_transcript_detail")
"""

import sys
import time
from pathlib import Path

import pandas as pd
import wrds

PROJECT_ROOT = Path(__file__).parent.parent
UNIVERSE_FILE = PROJECT_ROOT / "processed_data" / "public" / "universe_russell3000_proxy.parquet"
TEXT_OUTPUT_DIR = PROJECT_ROOT / "raw_data" / "transcripts" / "pilot"
META_OUTPUT = PROJECT_ROOT / "processed_data" / "public" / "pilot_transcripts_metadata.csv"

TEXT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
META_OUTPUT.parent.mkdir(parents=True, exist_ok=True)

PILOT_QUARTER_END = pd.Timestamp("2023-12-31")
TOP_N = 50

# Capital IQ key-development type for earnings call transcripts
# (48 = "Earnings Calls" in CIQ keydev taxonomy as of recent schemas; verify in checker)
EARNINGS_CALL_KEYDEV_TYPE = 48


def main():
    if not UNIVERSE_FILE.exists():
        sys.exit(f"Universe file not found: {UNIVERSE_FILE}\nRun 01_build_universe.py first.")

    universe = pd.read_parquet(UNIVERSE_FILE)
    pilot_firms = (
        universe[universe["quarter_end"] == PILOT_QUARTER_END]
        .nlargest(TOP_N, "mktcap_usd")
        [["permno", "gvkey", "ticker", "comnam", "mktcap_usd"]]
        .reset_index(drop=True)
    )
    if pilot_firms.empty:
        sys.exit(f"No universe rows at {PILOT_QUARTER_END.date()} — check universe file.")

    gvkeys = pilot_firms["gvkey"].dropna().unique().tolist()
    print(f"Pilot: top {TOP_N} firms by mktcap at {PILOT_QUARTER_END.date()}")
    print(f"  {len(pilot_firms)} firms; {len(gvkeys)} with GVKEY link")
    print()

    db = wrds.Connection()

    # --- map GVKEY -> CIQ companyid ----------------------------------------
    # CIQ's own linker: ciq.wrds_gvkey relates gvkey to companyid
    print("Mapping GVKEY -> CIQ companyid...")
    gvkey_list = ", ".join(f"'{g}'" for g in gvkeys)
    link = db.raw_sql(f"""
        SELECT gvkey, companyid
        FROM ciq.wrds_gvkey
        WHERE gvkey IN ({gvkey_list})
    """)
    print(f"  {len(link)} gvkey <-> companyid rows")

    companyids = link["companyid"].dropna().astype(int).unique().tolist()
    if not companyids:
        sys.exit("No CIQ companyids found for pilot firms — check ciq.wrds_gvkey schema.")

    # --- find earnings call transcripts for pilot quarter ------------------
    print(f"Querying CIQ transcript detail for {PILOT_QUARTER_END.date()} (±90 days)...")
    companyid_list = ", ".join(str(c) for c in companyids)
    detail = db.raw_sql(f"""
        SELECT transcriptid, companyid, companyname,
               mostimportantdateutc AS event_date,
               keydeveventid, keydevtypeid, keydevtypename,
               transcriptcollectiontypename
        FROM ciq_transcripts.wrds_transcript_detail
        WHERE companyid IN ({companyid_list})
          AND mostimportantdateutc BETWEEN
              '{(PILOT_QUARTER_END - pd.Timedelta(days=90)).date()}'
          AND '{(PILOT_QUARTER_END + pd.Timedelta(days=90)).date()}'
          AND keydevtypeid = {EARNINGS_CALL_KEYDEV_TYPE}
    """, date_cols=["event_date"])

    print(f"  {len(detail)} transcripts found")
    if detail.empty:
        sys.exit("No transcripts returned — verify keydevtypeid for earnings calls.")

    # --- pull text components ----------------------------------------------
    print("Pulling transcript components (speaker-tagged text)...")
    transcriptids = detail["transcriptid"].astype(int).unique().tolist()
    tid_list = ", ".join(str(t) for t in transcriptids)

    components = db.raw_sql(f"""
        SELECT c.transcriptid,
               c.componentorder,
               c.componenttext,
               c.transcriptcomponenttypeid,
               c.transcriptpersonid,
               p.personfirstname,
               p.personlastname,
               p.title AS person_title,
               p.speakertypeid,
               p.companyname AS person_companyname
        FROM ciq_transcripts.ciqtranscriptcomponent c
        LEFT JOIN ciq_transcripts.ciqtranscriptperson p
          USING (transcriptpersonid)
        WHERE c.transcriptid IN ({tid_list})
        ORDER BY c.transcriptid, c.componentorder
    """)
    print(f"  {len(components):,} component rows across {components['transcriptid'].nunique()} transcripts")
    db.close()

    # --- write speaker-tagged text files -----------------------------------
    print()
    print("Writing text files...")
    t0 = time.time()
    written = 0
    for tid, group in components.groupby("transcriptid"):
        meta = detail[detail["transcriptid"] == tid].iloc[0]
        header = [
            f"# Transcript {tid}",
            f"# Company: {meta['companyname']} (companyid {int(meta['companyid'])})",
            f"# Event date: {meta['event_date'].date()}",
            f"# Event: {meta['keydevtypename']}  |  Collection: {meta['transcriptcollectiontypename']}",
            "",
        ]
        lines = ["\n".join(header)]
        for _, row in group.iterrows():
            speaker = " ".join(filter(None, [
                row.get("personfirstname") or "",
                row.get("personlastname") or "",
            ])).strip() or "UNKNOWN"
            title = row.get("person_title") or ""
            affil = row.get("person_companyname") or ""
            tag = speaker
            if title or affil:
                tag += f" — {title}{', ' if title and affil else ''}{affil}"
            text = (row.get("componenttext") or "").strip()
            if text:
                lines.append(f"[{tag}]\n{text}\n")
        (TEXT_OUTPUT_DIR / f"{tid}.txt").write_text("\n".join(lines), encoding="utf-8")
        written += 1
    elapsed = time.time() - t0
    print(f"  {written} text files written in {elapsed:.1f}s")

    # --- metadata CSV ------------------------------------------------------
    meta_out = detail.merge(link, on="companyid", how="left").merge(
        pilot_firms[["gvkey", "permno", "ticker", "comnam"]], on="gvkey", how="left"
    )
    meta_out.to_csv(META_OUTPUT, index=False)
    print(f"Metadata: {META_OUTPUT}")
    print()
    print(f"Pilot complete. {written} transcripts, {len(components):,} components.")


if __name__ == "__main__":
    main()
