# STATE

## Done

- Project scoped: Russell 3000, 2005–2025, four document types
- Tulane data resources mapped (see `docs/data_sources.md`)
- WRDS individual account obtained (2026-04-21)
- Standard project scaffolding: directory structure, `.gitignore`, `CLAUDE.md`, `requirements.txt`
- GitHub repo created
- OSF project created
- OSF sync hook installed

## In Progress

- Initial WRDS connection test
- Inventory of legacy `PDFzips/` contents

## Blocked

- Analyst PDF bulk-pull strategy — requires decision on research-design mechanism (anchoring vs. tone transmission vs. attribution vs. reference dependence). Determines sample size and selection rule.

## Next

1. Connect to WRDS; verify entitlements for `ciq_transcripts`, `ciq_keydev`, `ibes`, `comp`, `crsp`
2. Pull Russell 3000 historical constituents → `processed_data/public/russell3000_constituents.parquet`
3. Build linking table (GVKEY ↔ PERMNO ↔ CIQ company_id) for the universe
4. First transcript pull: small pilot (~50 firms, 2023 Q4) to validate pipeline before scaling
5. I/B/E/S pull: detail + summary for universe + time range
6. EDGAR 10-K/Q downloader
7. Inventory legacy PDFzips and assess overlap with target universe
