# mairie-reglements (GE)

Crawls Geneva commune websites (depth-1 pages + depth-2 PDFs), extracts regulatory
text (trafilatura / pdfplumber), writes `bronze_ch.mairie_reglements_ge` →
`knowledge_ch.documents`/`chunks` on re-LLM. Classification + gold promotion run
separately (see `classify-backfill` edge function).

## Coverage: 38 / 45 GE communes

## Backlog — 7 communes not yet ingested (low long-tail signal; not built)

Each needs a separate parser capability, deliberately deferred:

- **OCR (scanned PDFs, no text layer):** Avusy, Dardagny
- **>20 MB PDF cap:** Avusy (PDCom rapport ~35 MB)
- **Deeper crawl / direct PDF URLs (content 2+ levels deep or no homepage match):**
  Bellevue, Gy, Collonge-Bellerive, Pregny-Chambésy, Cartigny

These are three independent capability additions (OCR, size-cap bump, deep/direct
crawl). Parked as backlog, not blocking.
