# STATE

## Done

- Project scaffolded (directory, git repo, OSF project `gv5qt`, OSF sync hook)
- Python 3.12 venv with wrds 3.5, pandas 2.2, SQLAlchemy 2.0
- WRDS account active; entitlements verified
- Russell 3000 proxy universe built: 236,981 firm-quarters, 8,026 GVKEYs, 80 quarters 2005Q1-2024Q4 → `processed_data/public/universe_russell3000_proxy.parquet`
- I/B/E/S analyst forecasts pilot: 50 firms × 2023 → 59,842 detail forecasts, 5,843 consensus rows, 722 analysts, 116 brokers. **End-to-end pipeline validated.**

## Blocked — NOT entitled at Tulane's WRDS tier

- `ciq_transcripts` (earnings call transcripts)
- `ciq_keydev` (key developments)
- `ibes.det_guidance` / `id_guidance` (I/B/E/S Guidance)
- `wrdssec` (SEC Analytics)

Entitlement request drafted at `docs/tulane_wrds_entitlement_request.md` for transcripts + I/B/E/S Guidance.

## In Progress

- Awaiting user action: send entitlement request email to Tulane WRDS admin
- Analyst-report PDF strategy (deferred until research-design mechanism is specified)

## Next (immediate)

1. **Scale up I/B/E/S:** extend forecast pull from pilot (50 firms, 2023) to full universe + 2005–2025
2. **I/B/E/S recommendations:** pull `ibes.recddet` for same universe+range (buy/hold/sell + target prices)
3. **EDGAR 10-K/Q downloader:** scripted pull, stored in `raw_data/filings/`
4. **Compustat fundamentals:** pull `comp.fundq` for universe (firm-quarter panel of accounting variables)

## Next (pending entitlement or decision)

5. **Transcripts:** when entitlement arrives (or fallback to Refinitiv Workspace manual pulls)
6. **Guidance:** when entitlement arrives (or fallback to Capital IQ web / IQ_GUIDANCE_* Excel functions)
7. **Analyst PDFs:** design-dependent, scope with user after behavioral-mechanism decision
8. **Inventory legacy `PDFzips/`:** identify 2016–2017 source, coverage, overlap with target universe
