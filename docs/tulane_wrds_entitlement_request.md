# Email draft — requesting CIQ Transcripts entitlement from Tulane WRDS admin

**To send to:** Tulane's WRDS institutional administrator (likely Freeman School IT or Howard-Tilton business librarian — check the WRDS welcome email for the listed contact).

**Subject:** Request: add `ciq_transcripts` to Tulane WRDS subscription

Hi,

I'm a faculty member in [department] and recently activated my WRDS account. Thanks for sponsoring the institutional subscription.

I'm running a text-analysis research project on earnings-call transcripts and would like to pull them programmatically via WRDS. Running `db.list_tables(library="ciq_transcripts")` returns `NotSubscribedError`, which I understand means this dataset isn't in Tulane's current subscription.

Is it possible to add the following add-ons to Tulane's WRDS subscription?

1. **Capital IQ Transcripts** (WRDS library: `ciq_transcripts`) — for earnings call transcripts
2. **I/B/E/S Guidance** (WRDS schema: `tr_ibes_guidance`, exposed via `ibes.det_guidance` and `ibes.id_guidance`) — for management forecast events. The table catalog is visible but SELECT returns "permission denied for schema tr_ibes_guidance"
3. **Capital IQ Key Developments** (`ciq_keydev`) — optional second source for guidance events, less important if I/B/E/S Guidance is added

For context:
- Tulane's Capital IQ Pro web seat *does* give me transcript access through the UI, but downloading them one-by-one isn't feasible for a Russell 3000 × 2005–2025 corpus (~250,000 transcripts).
- The WRDS-hosted dataset is the academic standard used in recent behavioral-finance NLP work (Bochkay et al., Druz et al., Huang-Zang-Zheng, etc.), so having it would also support future grad-student projects.
- I already have access to `ibes`, `comp`, `crsp`, and `ciq` (base), which are the other libraries I'll be using.

If a subscription change isn't possible this cycle, I'd also appreciate any guidance on Tulane's Refinitiv Workspace for Students limits — that's my fallback for bulk transcript pulls.

Thanks,
Daniel Walters
Freeman School of Business, Tulane University
djw307@gmail.com | ORCID 0000-0002-0121-7178
