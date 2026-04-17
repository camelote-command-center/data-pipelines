# SILGeneve parser

Weekly scraper for **Geneva cantonal real-estate-related laws** from [silgeneve.ch](https://silgeneve.ch/legis/).

Part of the `camelote-command-center/data-pipelines` monorepo.

## What it does

1. Loads a curated list of **~19 Geneva laws** relevant to real estate (see `laws_registry.json`).
2. Fetches each law's HTML page directly from `silgeneve.ch/legis/data/rsg_XXX.htm` (no Playwright needed — pages are static).
3. Parses out: title, adoption/entry-in-force/last-modified dates, articles with chapters and sections, and the full modifications history table.
4. Converts each law to clean markdown for RAG ingestion.
5. Upserts into `bronze_ch.silgeneve_laws` and `bronze_ch.silgeneve_articles` on **re-LLM** (`znrvddgmczdqoucmykij`).
6. Uses SHA-256 content hashing so unchanged laws skip the write.

## Why these laws

The registry is scoped to **real estate**, not the entire rsGE corpus. Domains covered:

| Domain | Laws |
|--------|------|
| Logement | LDTR, RDTR, ArRLoyers, LGL, RGL |
| Construction | LCI, RCI |
| Aménagement | LaLAT, LGZD, LExt, LRoutes |
| Énergie | LEn, REn |
| Environnement | LPE-GE |
| Patrimoine | LPMNS |
| Expropriation | LEx |
| Fiscalité | LCP, LIPP |
| Procédure | LOJ |

Priority 1 laws (LDTR, RDTR, LCI, RCI, LGL, RGL, LaLAT, LGZD, LEn, REn) are the core daily-use texts for LIA and LBI. Add/remove laws by editing `laws_registry.json`.

## Running locally

```bash
cd data-pipelines/silgeneve
pip install -r requirements.txt

# Point to the supabase registry (same pattern as other parsers)
export SUPABASE_REGISTRY_PATH="$HOME/supabase-registry/supabase-projects.json"

python parser.py
```

Expected output:
```
2026-04-17 14:23:00 [INFO] SILGeneve parser run starting. run_id=...
2026-04-17 14:23:00 [INFO] Loaded 19 laws from registry.
2026-04-17 14:23:01 [INFO] [1/19] Fetching L 5 05 (LCI)
2026-04-17 14:23:02 [INFO]   → L 5 05 CHANGED (87 articles, 42341 words, last mod: 2024-03-15)
...
```

## Deployment

1. **Apply schema to re-LLM** (one-time):
   ```sql
   -- Use Supabase apply_migration (not execute_sql — this is DDL)
   -- Copy contents of sql/01_bronze_schema.sql
   ```
   Verify:
   ```sql
   SELECT proname FROM pg_proc
   JOIN pg_namespace ON pronamespace = oid
   WHERE nspname = 'bronze_ch' AND proname LIKE 'silgeneve%';
   ```

2. **Copy files into the data-pipelines monorepo**:
   ```
   camelote-command-center/data-pipelines/
   └── silgeneve/
       ├── parser.py
       ├── laws_registry.json
       ├── requirements.txt
       ├── README.md
       └── sql/01_bronze_schema.sql
   ```

3. **Add the workflow** to the repo-level `.github/workflows/` directory (not inside `data-pipelines/silgeneve/`). The workflow file's `working-directory` assumes that path.

4. **Secrets required** in GitHub repo settings:
   - `CAMELOTE_REGISTRY_TOKEN` — for checking out the supabase-registry private repo
   - `CAMELOTE_REGISTRY_SSH_KEY` — if registry uses SSH auth

5. **First run** — trigger manually via workflow_dispatch. Expect ~1 minute total (19 laws × 1.5 s polite delay + fetch + parse).

## Schema

### `bronze_ch.silgeneve_laws`
One row per law. Primary key `law_rsge` (e.g. `'L 5 20'`). Includes `content_md` for RAG, `content_hash` for change detection, and JSONB `modifications_history`.

### `bronze_ch.silgeneve_articles`
One row per article inside a law. Keyed by `(law_rsge, article_number)`. Use for article-level RAG retrieval or direct lookup ("Art. 3 LDTR").

### `bronze_ch.silgeneve_fetch_log`
Audit trail — every run logs `run_start`, per-law `success`/`unchanged`/`error`, then `run_end`. Grouped by `run_id` UUID.

## Downstream usage (LIA + LBI)

### Option A — markdown RAG ingestion
Pull `content_md` column into your vector store. Good for natural-language queries like "Puis-je transformer ce bien sous LDTR ?"

```sql
SELECT law_rsge, short_name, content_md
FROM bronze_ch.silgeneve_laws
WHERE priority <= 2
ORDER BY priority, law_rsge;
```

### Option B — article-level lookup
When LIA references a specific article, fetch the exact article text:

```sql
SELECT content, chapter, section
FROM bronze_ch.silgeneve_articles
WHERE law_rsge = 'L 5 20' AND article_number = 'Art. 3';
```

### Option C — public RPC for frontend (Lovable)
Add to `lamap_db` after data is ready:

```sql
-- in public schema, never lamap_app
CREATE OR REPLACE FUNCTION public.get_geneva_law_article(
  p_rsge TEXT,
  p_article TEXT
) RETURNS TABLE (
  law_rsge TEXT,
  short_name TEXT,
  article_number TEXT,
  article_title TEXT,
  chapter TEXT,
  content TEXT
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, bronze_ch
AS $$
  SELECT l.law_rsge, l.short_name, a.article_number, a.article_title, a.chapter, a.content
  FROM bronze_ch.silgeneve_articles a
  JOIN bronze_ch.silgeneve_laws l USING (law_rsge)
  WHERE a.law_rsge = p_rsge AND a.article_number = p_article;
$$;
```

Note: Since `bronze_ch` lives on re-LLM and `lamap_db` is a separate DB, the SECURITY DEFINER function would need to live on re-LLM, or the data needs to be pushed via FDW to `lamap_db.public`. Decide based on whether this is read from LBI (re-LLM frontend friendly) or Lamap/LIA (lamap_db).

## Maintenance

- **Registry updates**: edit `laws_registry.json`, commit. Next run picks up new/removed laws. Removed laws do NOT delete existing rows — add a cleanup step if needed.
- **URL pattern**: SILGeneve uses `rsg_{vol}{num}.htm` with lowercase letters, no space, and `p{subnum}` for sub-documents (règlements, arrêtés).
- **Encoding**: pages are served in `windows-1252`. The parser forces this explicitly; do NOT remove the `resp.encoding = "windows-1252"` line.
- **Politeness**: `POLITE_DELAY_S = 1.5s` between requests. Raise if SILGeneve complains; lower only after asking them.
- **Failure handling**: per-law errors do not abort the run. Check `bronze_ch.silgeneve_fetch_log` after each run.

## Monitoring queries

```sql
-- Latest run summary
SELECT run_id, MIN(fetched_at) as started_at, MAX(fetched_at) as ended_at,
       COUNT(*) FILTER (WHERE status = 'success')   as changed,
       COUNT(*) FILTER (WHERE status = 'unchanged') as unchanged,
       COUNT(*) FILTER (WHERE status = 'error')     as errors
FROM bronze_ch.silgeneve_fetch_log
WHERE fetched_at > now() - interval '7 days'
GROUP BY run_id
ORDER BY started_at DESC
LIMIT 5;

-- Laws that changed in last run
SELECT law_rsge, short_name, last_modified, article_count, word_count, updated_at
FROM bronze_ch.silgeneve_laws
WHERE updated_at > now() - interval '7 days'
ORDER BY updated_at DESC;

-- Laws that errored recently
SELECT law_rsge, status, error_message, fetched_at
FROM bronze_ch.silgeneve_fetch_log
WHERE status = 'error'
  AND fetched_at > now() - interval '30 days'
ORDER BY fetched_at DESC;
```

## Future enhancements

- **Diff detection**: when `content_hash` changes, compute which articles changed and log the diff into a `silgeneve_article_changes` table. Useful for LBI alerts ("Art. 39 LDTR vient de changer").
- **Embeddings**: run `content_md` or per-article content through `pgvector` for semantic search inside LIA.
- **Fedlex cross-references**: LDTR cites the CO, CCS, LAT. Once the Fedlex pipeline is up, link Geneva articles to federal articles they reference.
- **Public RDPPF integration**: for a given parcel, return all cantonal laws that restrict it.
