# Data sources

## Four target document types → where to get them

### 1. Earnings call transcripts

- **Primary: WRDS → `ciq_transcripts`** — full-text, speaker-tagged, timestamped. Covers US large/mid-caps from ~2003 forward; coverage broadens into small-caps over time.
- Key tables (verify on first connection):
  - `ciq_transcripts.wrds_transcript_detail` — transcript metadata (event ID, date, company, event type)
  - `ciq_transcripts.ciqtranscript` — transcript header
  - `ciq_transcripts.ciqtranscriptcomponent` — speaker-level text blocks
  - `ciq_transcripts.ciqtranscriptperson` — speaker identity (exec vs. analyst)
- Cross-walk to GVKEY via `ciq.wrds_gvkey` or Compustat's CIQ linker
- **Secondary:** Capital IQ web UI → Documents & Reports (for validation only)

### 2. 10-K / 10-Q filings

- **Primary: SEC EDGAR** (free). Use the full-text index at `https://www.sec.gov/cgi-bin/browse-edgar` and submission JSON at `https://data.sec.gov/submissions/CIK{padded}.json`. Rate limit: 10 requests/second, include a `User-Agent` header.
- **Cross-walk:** CIK ↔ ticker via `https://www.sec.gov/files/company_tickers.json`, then ticker → GVKEY via Compustat.
- Alternate WRDS path: `wrds_sec_suite` / `sec_analytics` if entitled — has pre-parsed filings metadata and some derivatives (word counts, readability).
- No entitlement risk for EDGAR — it's public, authoritative, and reproducible.

### 3. Company guidance

- **Primary: WRDS → `ciq_keydev`** — "Key Developments" events including guidance, pre-announcements, management changes. Structured fields: event date, event type, summary text.
- Filter on event type codes for guidance (typically `KeyDevEventTypeId` values for "Corporate Guidance" categories — verify exact codes on connection).
- **Also:** Capital IQ Excel plug-in has dedicated `IQ_GUIDANCE_*` functions for structured ranges (EPS guide low/high, revenue guide low/high, etc.). Useful for validation and for numeric guidance ranges not cleanly in `ciq_keydev`.

### 4. Analyst forecasts

Two separate products with different use cases:

**4a. Structured forecasts (I/B/E/S) — academic standard**
- **Primary: WRDS → `ibes`**
- Detail (analyst × firm × date): `ibes.det_epsus`, `ibes.det_xepsus` (unadjusted)
- Summary (consensus): `ibes.statsum_epsus`, `ibes.statsum_xepsus`
- Recommendations: `ibes.recddet`
- Cross-walk: `ibes.id` (TICKER to CUSIP), then CUSIP to PERMNO to GVKEY
- Use for all quantitative forecast analysis (accuracy, dispersion, revisions, optimism bias)

**4b. Raw analyst reports (PDFs) — for text analysis**
- **Primary: Refinitiv Workspace for Students** — Investext research library, manual export within per-day caps (~100–500/day before rate-limiting)
- **Secondary: Capital IQ web → Research → Equity Research** — per-document download, strict entitlement caps
- **Legacy: `PDFzips/`** — 2016–2017 batch of ~2.4 GB already on disk. Inventory before relying on.
- **Not on WRDS** — WRDS hosts structured data, not analyst PDFs
- Strategy deferred until research-design mechanism is set (anchoring vs. tone transmission vs. attribution vs. reference dependence)

## Identifier cross-walk map

```
Capital IQ company_id ──┐
                        ├─► GVKEY (Compustat) ─┬─► PERMNO (CRSP) ─► CUSIP
I/B/E/S ticker ─► CUSIP ┘                      │
                                               └─► Ticker / Name
SEC CIK ◄── ticker (via SEC mapping)
```

Linking tables:
- `crsp.ccmxpf_linktable` — GVKEY ↔ PERMNO (use with `linktype IN ('LU','LC')`)
- `wrdsapps.ibcrsphist` — I/B/E/S ↔ CRSP
- Compustat CIQ linker (exact name varies by WRDS release)

## Universe construction

Russell 3000 historical constituents are not on WRDS directly. Options:
1. FTSE Russell publishes monthly constituent files (requires registration)
2. CRSP index constituents: approximate with top 3000 by market cap at each rebalance
3. Commercial providers (FactSet, Refinitiv) if licensed

For 2005–2025 reconstruction, option 1 is cleanest; option 2 is a workable approximation.
