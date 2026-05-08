# INSEE SIRENE — `bronze_fr.companies` + `bronze_fr.etablissements`

French companies & establishments registry. Mirrors how Switzerland's
`bronze_ch.zefix_companies` works for Swiss companies — same UPSERT discipline,
same PostgREST `batch_upsert` pattern, same registry conventions.

## Tables

| Table | Conflict key | Purpose |
|---|---|---|
| `bronze_fr.companies` | `siren` | Legal entities (UniteLegale). 1 row per SIREN. |
| `bronze_fr.etablissements` | `siret` | Establishments (Etablissement). N rows per SIREN, ≥1 with `est_siege=true`. |

Indexes are deferred — they're created via `migrations/003_create_indexes.sql`
**after** the bulk load completes (avoids ~30 min of index churn during inserts).

## Two modes

```
python pipelines/sirene/import.py --mode bulk         # one-time, parquet stock files
python pipelines/sirene/import.py --mode incremental  # daily, INSEE API delta
```

### Bulk (`--mode bulk`)

- **Source:** [data.gouv.fr Sirene parquet stock](https://www.data.gouv.fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)
- **Files:** `StockUniteLegale_*.parquet` (~691 MB) + `StockEtablissement_*.parquet` (~2.17 GB)
- **Cadence:** monthly publication, 1st of each month
- **Filter:** rows where `activitePrincipaleUniteLegale ∈ NAF_WHITELIST` (real-estate sector + development + holdings, see `naf_filter.py`)
- **Etablissements filter:** SIREN must be in `bronze_fr.companies` (loaded in step 1). Establishments themselves are NOT NAF-filtered — an RE holding company can have non-RE branches and we want all of them.
- **Runtime estimate:** 30–60 min (download dominates)
- **Local-preferred:** designed to run on a developer Mac so it can be interrupted. Cache lives in `$SIRENE_CACHE_DIR` (default `/tmp/sirene_cache`).

CLI flags:

```
--unite-legale-only      # skip etablissement load
--etablissement-only     # skip UL load (assumes companies table populated)
--limit-rows N           # stop after N matched rows per stage (smoke tests)
--batch-size N           # default 500
```

### Incremental (`--mode incremental`)

- **Source:** [INSEE API v3.11](https://api.insee.fr/api-sirene/3.11)
- **Auth:** OAuth2 client_credentials at `https://auth.insee.fr/auth/realms/apim-gravitee/protocol/openid-connect/token`
- **Cadence:** daily 05:00 UTC (workflow cron currently disabled — see "Credentials" below)
- **Window:** `dateDernierTraitementUniteLegale:[<last_sync_at - 1day> TO *]` (1-day buffer guard)
- **Filter:** post-fetch — companies NOT pre-filtered by NAF (a company can change *into* RE), but only RE-sector ones are upserted into `bronze_fr.companies`. Establishments fetched only for the SIRENs that made it into `companies`.
- **Rate limit:** 30 req/min. Client throttles to 28 req/min defensively, exponential back-off on 429 / 5xx, re-auths on 401.

## Environment variables

| Var | Required for | Notes |
|---|---|---|
| `RE_LLM_SUPABASE_URL` | both modes | re-LLM project URL (`https://znrvddgmczdqoucmykij.supabase.co`) |
| `RE_LLM_SUPABASE_SERVICE_ROLE_KEY` | both modes | service_role key |
| `RE_LLM_SCHEMA` | both modes | default `bronze_fr` |
| `INSEE_CLIENT_ID` | incremental | OAuth2 client id |
| `INSEE_CLIENT_SECRET` | incremental | OAuth2 client secret |
| `SIRENE_CACHE_DIR` | bulk | local download cache (default `/tmp/sirene_cache`) |
| `SIRENE_LOOKBACK_DAYS` | incremental | window if no prior sync (default 1) |

## Credentials status

⚠️ As of 2026-05-08, INSEE OAuth2 credentials have **not yet been provisioned**. Tracking
bug `cbb09d8c-56ab-4374-9846-edcf8418432b` in `camelote_data.bugs`.

Until creds arrive:
- Incremental workflow `.github/workflows/sirene-incremental.yml` cron stays **disabled**
  (only `workflow_dispatch` enabled).
- Bulk load is independent of the API — runs immediately with just the re-LLM
  Supabase secrets.

To unblock incremental sync:
1. Register at [portail-api.insee.fr](https://portail-api.insee.fr/)
2. Subscribe to `api-sirene` v3.11
3. Copy `client_id` and `client_secret` into the `data-pipelines` repo as
   GitHub Actions secrets `INSEE_CLIENT_ID` and `INSEE_CLIENT_SECRET`
4. Uncomment the `schedule:` block in `sirene-incremental.yml`

## Files

```
pipelines/sirene/
├── import.py                 # entry point (--mode bulk / incremental)
├── api_client.py             # OAuth2 + paginated GET wrapper
├── bulk_load.py              # parquet streaming + NAF filter + UPSERT
├── incremental_sync.py       # daily API delta + UPSERT
├── column_mapping.py         # SIRENE field → bronze column mapping (stock + API)
├── naf_filter.py             # NAF whitelist + normaliser
├── README.md                 # this file
└── migrations/
    ├── 001_create_bronze_fr_companies.sql
    ├── 002_create_bronze_fr_etablissements.sql
    └── 003_create_indexes.sql      # apply AFTER bulk load completes
```

## Operational notes

- **Lambert-93 vs WGS84:** the parquet stock files store coordinates in Lambert-93
  (`coordonneeLambertAbscisseEtablissement` / `coordonneeLambertOrdonneeEtablissement`).
  We do NOT convert to WGS84 in the bulk path — fields are left NULL, and incremental
  sync (which uses the API and gets WGS84 directly) populates them. Lambert→WGS84
  reprojection of the historical stock is a silver-layer task, out of scope here.
- **NAF Rev.2 vs NAF2025:** SIRENE files currently expose Rev.2 codes only.
  The `activite_principale_naf25` column is reserved for the 2027 cutover and
  populated as NULL until then.
- **`raw_data jsonb`:** every row carries the full source payload as `raw_data`
  for downstream silver processing without re-fetching.
- **Idempotency:** ON CONFLICT updates `last_seen_at` and `updated_at`; `first_seen_at`
  is preserved. Re-running the same release is a no-op for row counts.

## Registry

Both datasets registered in `bronze_ch._registry` (centralised, FR datasets live there too):

```
dataset_code                  bronze_table                   sync_frequency
insee_sirene_companies        bronze_fr.companies            daily
insee_sirene_etablissements   bronze_fr.etablissements       daily
```

Plus one entry in `ref.data_sources`:

```
name='INSEE - API Sirene' publisher='INSEE' country_code='FR'
document_type='api_feed'  reliability_tier='institutional'
```
