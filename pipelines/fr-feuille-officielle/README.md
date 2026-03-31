# FR Feuille Officielle Pipeline

Scrapes property transactions and building permits from the Feuille officielle du canton de Fribourg (`fo.fr.ch`) and upserts them into Supabase.

## Data Sources

- **Category 15 (transactions):** Property transfers published in the official gazette, organized by district (Sarine, Singine, Gruyere, Lac, Glane, Broye, Veveyse).
- **Category 21 (building permits):** Building permit notices.

Archive URL structure: `https://fo.fr.ch/archive/{year}/{issue}/{category}/{district_id}`

## Target Tables

| Script               | Table                          |
|----------------------|--------------------------------|
| `fetch-transactions` | `bronze.transactions_national` |
| `fetch-permits`      | `bronze.sad_national`          |

## Usage

```bash
npm install
```

### Incremental run (current year)

```bash
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-transactions.ts
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-permits.ts
```

### Backfill (historical data)

```bash
# Single year
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-transactions.ts 2024

# Full backfill (2020-2026)
for year in 2020 2021 2022 2023 2024 2025 2026; do
  SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-transactions.ts $year
  SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-permits.ts $year
done
```

### Discovery only (no DB writes)

```bash
npx tsx discover-issues.ts > issues.json        # all years
npx tsx discover-issues.ts 2025 > issues.json   # single year
```

## Environment Variables

| Variable                   | Required | Description              |
|----------------------------|----------|--------------------------|
| `SUPABASE_URL`             | Yes      | Supabase project URL     |
| `SUPABASE_SERVICE_ROLE_KEY`| Yes      | Supabase service_role key|

## Districts (Category 15)

| ID  | Name                           |
|-----|--------------------------------|
| 111 | Registre foncier de la Sarine  |
| 112 | Registre foncier de la Singine |
| 113 | Registre foncier de la Gruyere |
| 114 | Registre foncier du Lac        |
| 115 | Registre foncier de la Glane   |
| 116 | Registre foncier de la Broye   |
| 117 | Registre foncier de la Veveyse |

## Notes

- Rate limited to 500ms between HTTP requests.
- District 112 (Singine) publishes bilingual text (French/German).
- Transaction prices are never disclosed in the gazette; the `price` field is always NULL.
- The parser is intentionally lenient: unparseable fields are left NULL and `raw_text` is stored in `raw_data` for later refinement.
