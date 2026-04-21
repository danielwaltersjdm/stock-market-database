# WRDS connection

## Credentials

- **WRDS username:** `dwalters` (not `dwalters1` — note the difference from the Windows OS username)
- **pgpass location on Windows:** `%APPDATA%\postgresql\pgpass.conf` (= `C:\Users\dwalters1\AppData\Roaming\postgresql\pgpass.conf`)
- In Python, always connect with `wrds.Connection(wrds_username="dwalters")`. Without the explicit username, the `wrds` package defaults to the OS username (`dwalters1`), which doesn't match `pgpass.conf` and forces an interactive re-prompt.

## One-time setup

1. Install requirements:
   ```bash
   venv/Scripts/pip install -r requirements.txt
   ```

2. First connection — creates `~/.pgpass` with credentials cached:
   ```python
   import wrds
   db = wrds.Connection()   # prompts for WRDS username + password
   db.create_pgpass_file()
   db.close()
   ```

   After this, subsequent `wrds.Connection()` calls authenticate silently.

3. Verify entitlements for the libraries this project needs:
   ```python
   import wrds
   db = wrds.Connection()
   for lib in ["ciq", "ciq_transcripts", "ciq_keydev", "ibes", "comp", "crsp"]:
       try:
           tables = db.list_tables(library=lib)
           print(f"OK   {lib:20s}  {len(tables)} tables")
       except Exception as e:
           print(f"FAIL {lib:20s}  {e}")
   ```

## Tulane proxy notes

WRDS connects via PostgreSQL on port 9737, not HTTPS — the Tulane SSL-intercepting proxy does not interfere. No `verify=False` needed for WRDS itself.

## Standard query pattern

```python
import wrds
db = wrds.Connection()

# SQL — preferred for large pulls
df = db.raw_sql("""
    SELECT gvkey, datadate, atq, revtq
    FROM comp.fundq
    WHERE datadate BETWEEN '2005-01-01' AND '2025-12-31'
      AND indfmt = 'INDL' AND datafmt = 'STD' AND consol = 'C'
""", date_cols=["datadate"])

# Or the Pythonic helper for smaller pulls:
df = db.get_table(library="ibes", table="statsum_epsus",
                  columns=["ticker", "statpers", "meanest"],
                  obs=1000)
```

## Common pitfalls

- Library names use lowercase with underscores (`ciq_transcripts` not `CIQ_Transcripts`)
- Some libraries are entitlement-gated — if `list_tables` returns empty or errors, the library isn't subscribed at Tulane's tier
- `db.close()` at the end of scripts to release the connection
- WRDS query timeouts apply at ~2 hours; for large pulls, batch by year or GVKEY range
