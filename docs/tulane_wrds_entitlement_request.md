# Email — requesting CIQ Transcripts + I/B/E/S Guidance entitlement

**To:** Tulane WRDS institutional representative (check the WRDS welcome email for the listed contact; Freeman School IT/data services or the Howard-Tilton business librarian are the usual routes).

**Subject:** Request: add Capital IQ Transcripts and I/B/E/S Guidance to Tulane WRDS subscription

---

Hi,

I'm a faculty member at Tulane (Freeman School of Business) and recently activated my WRDS account — thanks for sponsoring the institutional subscription.

I'm starting a text-analysis research project on earnings disclosures and analyst behavior, and I've hit two entitlement gaps that I'd like to ask about. Running `SELECT` probes on the target tables returns `permission denied` even though they appear in the catalog:

1. **Capital IQ Transcripts** (WRDS library `ciq_transcripts`) — earnings call transcripts. This is the academic-standard source for conference-call NLP research; without it, the only programmatic path to a Russell 3000 transcript corpus is Refinitiv Workspace manual export, which caps at a few hundred documents per day and would take months.

2. **I/B/E/S Guidance** (WRDS schema `tr_ibes_guidance`, exposed via `ibes.det_guidance` / `ibes.id_guidance`) — management guidance events. I have access to the core I/B/E/S forecast tables (`det_epsus`, `statsum_epsus`, etc.) but the Guidance add-on is gated. The Guidance file is the standard source for papers studying forward-looking managerial disclosures.

Is either of these in scope for a subscription add-on this cycle? If not, I'd appreciate any guidance on whether Tulane has considered them in prior renewal decisions, or whether there's a usage threshold that would justify adding them.

For reference, the libraries I already have access to at current entitlement are `crsp`, `comp`, `ibes` (core), and `ciq` (base) — sufficient for the analyst-forecast side of the project but not the managerial-disclosure side.

Happy to provide additional context on the research or letters of support if that helps the case.

Thanks,
Daniel Walters
Freeman School of Business, Tulane University
djw307@gmail.com | ORCID 0000-0002-0121-7178
