# tinjob-seo Cloudflare Worker (RE-LLM cutover)
Routes tinjob.ch SEO paths → RE-LLM edge fns. Line 7 `SUPABASE_FUNCTIONS_URL` = RE-LLM (was jobs-ch).
Zone cc3efa75ed4dcccf986a8480af0072b4, Worker `tinjob-seo`.
⚠️ NOT YET DEPLOYED (2026-07-20) — needs a Cloudflare Workers API token / `wrangler deploy`.
Deploy: `npx wrangler deploy supabase/tinjob-cloudflare-worker/worker.js --name tinjob-seo` (with CLOUDFLARE_API_TOKEN).
