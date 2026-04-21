# Stock market database

A reusable corpus of financial documents for behavioral-economics research on judgment, forecasting, and decision biases.

---

## Research goal

Build a standing corpus of firm-level disclosures and sell-side outputs that can be reused across multiple projects on managerial and analyst judgment. Primary research designs anticipated: text analysis of conference calls and analyst reports (tone transmission, anchoring, attribution asymmetry, reference dependence).

Not a one-off dataset — pipelines must be reproducible and documented.

---

## Scope

- **Universe:** Russell 3000
- **Time range:** 2005–2025
- **Unit of analysis:** firm-quarter (primary); doc-level for text analysis
- **Four target document types:**
  1. Earnings call transcripts
  2. 10-K / 10-Q filings
  3. Management guidance (forward-looking disclosures)
  4. Analyst forecasts (structured + raw text reports)

---

## Architecture

```
Stock market database/
├── CLAUDE.md                    # this file — stable knowledge
├── STATE.md                     # task tracker
├── requirements.txt             # Python deps
├── raw_data/                    # gitignored — licensed text
│   ├── transcripts/             # Capital IQ Transcripts via WRDS
│   ├── filings/                 # 10-K / 10-Q text from EDGAR
│   ├── analyst_reports/         # PDFs pulled from Refinitiv / CIQ
│   ├── guidance/                # CIQ Key Developments / Guidance
│   └── ibes/                    # I/B/E/S detail + summary
├── PDFzips/                     # gitignored legacy — see below
├── processed_data/
│   ├── text/                    # gitignored — cleaned full text
│   ├── private/                 # gitignored — anything else licensed
│   └── public/                  # shareable: metadata, aggregated features
├── outputs/                     # charts, tables for humans
├── notebooks/                   # Jupyter analysis
├── scripts/                     # ETL and analysis pipelines
├── docs/                        # reference notes, specs
└── sessions/                    # per-session logs
```

---

## Data sources

Primary — via WRDS (PostgreSQL, `wrds` Python package):

| Dataset | WRDS library | Use |
|---|---|---|
| Capital IQ Transcripts | `ciq_transcripts` | Full-text earnings calls, speaker-tagged |
| Capital IQ Key Developments | `ciq_keydev` | Guidance events, pre-announcements |
| I/B/E/S Detail | `ibes.det_epsus` etc. | Analyst-level forecast records |
| I/B/E/S Summary | `ibes.statsum_epsus` | Consensus statistics |
| Compustat | `comp.funda`, `comp.fundq` | Fundamentals for linking |
| CRSP | `crsp.dsf`, `crsp.msf` | Prices, returns, identifiers |
| Linking tables | `crsp.ccmxpf_linktable` | GVKEY ↔ PERMNO ↔ CUSIP |

Secondary — outside WRDS:

| Dataset | Source | Use |
|---|---|---|
| 10-K / 10-Q filings | SEC EDGAR (free, scripted) | Authoritative filings text |
| Analyst research PDFs | Refinitiv Workspace for Students | Text analysis of sell-side narratives |
| Analyst research PDFs (legacy) | `PDFzips/` in this directory | Pre-existing 2016–2017 batch; may be usable |

---

## Licensing — critical

Capital IQ transcripts and analyst reports are **licensed for academic use only**. They cannot be redistributed.

- `raw_data/` and `PDFzips/` are `.gitignore`d and excluded from OSF sync
- `processed_data/text/` and `processed_data/private/` are also excluded
- What goes public: code, metadata tables (doc_id, ticker, date, broker), aggregated features (sentiment scores per firm-quarter), documentation

The public repo exists so the methodology is reproducible; the licensed inputs stay local.

---

## Conventions

- Python venv at `venv/`; executable is `venv/Scripts/python.exe` (Windows)
- Tulane corporate proxy intercepts SSL: use `verify=False` + `urllib3.disable_warnings()` for Python requests; `--insecure` for curl
- WRDS connection: credentials in `~/.pgpass` (see `docs/wrds_connection.md`)
- Identifier conventions: GVKEY (primary firm ID), PERMNO (CRSP), CUSIP (cross-walk), ticker (human-readable only)
- Date conventions: all dates stored as ISO `YYYY-MM-DD`
- File naming: `{source}_{date}_{doc_id}.{ext}` (e.g. `ciq_2024-02-14_AAPL_Q1_earnings.txt`)

---

## Legacy asset — `PDFzips/`

Pre-existing folder of analyst report PDFs from a 2016–2017 download batch (~2.4 GB across 4 zip files plus extracted `Output3/`, `output 2/`, `combined/` subfolders). Source and exact coverage TBD; inventory this before relying on it. May overlap with or predate the Russell 3000 / 2005–2025 scope.

---

## Environment

- OS: Windows 11
- Shell: bash
- Python: venv at project root
- Primary GitHub: `danielwaltersjdm/stock-market-database`
- OSF node: [set after creation]
