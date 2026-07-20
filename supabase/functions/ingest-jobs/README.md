# ingest-jobs / check-stale-urls (Tinjob portal runtime)

Deployed to **RE-LLM** (`znrvddgmczdqoucmykij`), writing to `bronze_ch.tinjob_*`.
Committed 2026-07-20 to de-strand the previously deploy-only source (Tinjob decommission).

- `ingest-jobs`: portal scraper (jobs.ch, jobup.ch). Cron `tinjob-ingest-jobs` (body `{}`).
  Deployed `--no-verify-jwt`. NOTE: contains an inert ATS phase (`body.phase="ats"`) —
  ATS is now owned by the GitHub Actions in `pipelines/tinjob-ats/`; the `tinjob-ingest-ats`
  cron is disabled. Safe to strip the ATS code on next edit.
- `check-stale-urls`: marks stale job URLs inactive. Cron `tinjob-check-stale-urls`.

Redeploy: `supabase functions deploy <name> --project-ref znrvddgmczdqoucmykij --no-verify-jwt --use-api`
