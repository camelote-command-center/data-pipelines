# NE SITN WFS Pipeline — Neuchatel Building Permits

Fetches building permits from the SITN (Systeme d'information du territoire neuchatelois) WFS service and upserts them into `bronze.sad_national` in Supabase.

## Data Sources

Two WFS layers are fetched from `sitn.ne.ch`:

- `at034_autorisation_construire_pendant` — permits currently in public consultation (enquete publique)
- `at034_autorisation_construire_apres` — permits after decision

The WFS service returns GML3 in EPSG:2056 (Swiss LV95). Coordinates are reprojected to EPSG:4326 (WGS84) using proj4.

## Usage

```bash
npm install
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... npx tsx fetch-permits.ts
```

## Environment Variables

| Variable                   | Required | Description                  |
|----------------------------|----------|------------------------------|
| `SUPABASE_URL`             | Yes      | Supabase project URL         |
| `SUPABASE_SERVICE_ROLE_KEY`| Yes      | Supabase service_role key    |
