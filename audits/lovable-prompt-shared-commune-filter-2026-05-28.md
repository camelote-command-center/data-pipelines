# Lovable prompt — shared canton/commune/facet filter (4 pages)

> Supersedes `lovable-prompt-subquartier-filters-2026-05-28.md`. Paste the block
> below into Lovable. It is self-contained. Backend RPCs are already live on
> Supabase (lamap_db), granted to anon/authenticated. **Do NOT create or alter any
> database objects.**
>
> Pages: `/app/registre-proprietaires`, `/app/sad`, `/app/transactions`,
> `/app/petites-annonces`. They must share ONE filter component, parameterized by
> a `dataset` string: `'registre' | 'sad' | 'transactions' | 'annonces'`.

---

## PROMPT FOR LOVABLE

Build ONE reusable location-filter component and use it on all four pages above.
It has three stacked controls — **Canton → Commune → (GIREC + Postcode)** — all
driven by three shared Supabase RPCs, parameterized by a `dataset` prop.

### The three RPCs (identical shape for every page)

**1) Cantons** — `get_cantons(p_dataset)` → rows of `{ canton_code, canton_name, n }`.

**2) Commune options** — `get_commune_options(p_dataset, p_canton)` → rows of:
```
commune_canonical_bfs : number   // the value to filter by (NOT the name)
canonical_name        : string   // display label, e.g. "Genève-Cité"
parent_bfs            : number|null
is_sub_commune        : boolean
n                     : number
```

**3) Facets** — `get_facets(p_dataset, p_commune_bfs)` → rows of:
```
facet : 'npa' | 'girec'
value : string
label : string
n     : number
```

### Controls & behaviour

**Canton selector** (new):
- Populate from `get_cantons(dataset)`. **Default to `GE` (Genève).**
- On change: refetch `get_commune_options(dataset, canton)` and **reset** the
  commune, GIREC and postcode selections below.

**Commune selector:**
- Populate from `get_commune_options(dataset, selectedCanton)`.
- Render a **flat, strictly alphabetical list by `canonical_name`**. Do NOT group
  the four Geneva sub-quartiers under a "Genève" parent, and do NOT pin or
  re-sort client-side — they sort naturally inline (Genève-Cité, Genève-Eaux-Vives,
  Genève-Petit-Saconnex, Genève-Plainpalais among the G's). *(This supersedes the
  earlier "group under Genève parent" instruction.)*
- Selected state carries `commune_canonical_bfs` (a number).

**GIREC + Postcode** (from `get_facets(dataset, commune_canonical_bfs)`, split by `facet`):
- **GIREC** (`facet='girec'`): a true hierarchical child of the commune. Render as
  single/multi-select nested under the commune. `value`=GIREC code, `label`=name.
- **Postcode / NPA** (`facet='npa'`): a **co-facet, not a strict nested child**
  (NPAs straddle boundaries). Independent multi-select that further narrows.
- On commune change: clear GIREC + NPA, show a loading state, then repopulate.
- If a facet list returns empty, disable/hide that control (e.g. **GIREC is empty
  for every non-Geneva commune and for annonces** — it's a Geneva cadastral
  concept). Disable GIREC + NPA when no commune is selected.

### Per-page wiring (dataset + result query)

- **`/app/sad`** (`dataset='sad'`): pass the selected commune's `canonical_name`
  to `get_filtered_sad_v2` (its `p_filters.commune`). Fully sub-quartier-correct.
- **`/app/registre-proprietaires`** (`dataset='registre'`):
  - Owner **count** `get_filtered_owner_count(p_communes := [canonical_name], p_npas := [...])`
    — works at sub-quartier grain.
  - ⚠️ Owner **list** `get_owners_national_v2(p_commune := text)` filters an
    entity rollup that today only knows bare "Genève", so a sub-quartier name
    returns nothing. **For a Geneva sub-quartier selection, key the owner list on
    the bare parent "Genève"** (use `parent_bfs`→"Genève") while count + facets use
    the sub-quartier. Leave a TODO at that call site (backend rollup fix pending).
- **`/app/transactions`** (`dataset='transactions'`) — **now enabled** (was fenced
  off): the commune list now shows the four Geneva sub-quartiers. Pass the selected
  `canonical_name` to the existing transactions result/list query. NOTE: until a
  back-end propagation lands (this week), sub-quartier transaction **counts/results
  will be partial** (the option count is complete but the FAO result grain is still
  catching up); they converge automatically — no FE change needed when they do.
- **`/app/petites-annonces`** (`dataset='annonces'`) — **now enabled**: commune
  options come back as **clean canonical names** (no more "Geneva"/"1532 Fétigny
  FR"). Annonces are free-text geocoded, so **Geneva appears as the single parent
  "Genève" (no sub-quartiers) and there is no GIREC facet** (NPA only). Filter the
  listings result by the selected `canonical_name` / `commune_canonical_bfs`.
  Replace any current commune-filter source that showed raw garbage strings.

### Out of scope
- Do not build client-side commune resolution or re-sorting. Trust the RPCs.

---

## Backend status (for the human, not Lovable)

| Page | dataset | options | facets | result wiring | notes |
|------|---------|---------|--------|---------------|-------|
| registre-proprietaires | `registre` | ✅ 4 sub-quartiers, canton-scoped, alpha | ✅ npa+girec | count ✅ / **list ⚠️** | owner list still bare-Genève (task #12) |
| sad | `sad` | ✅ | ✅ | ✅ canonical_name | fully correct |
| transactions | `transactions` | ✅ derived (4575/7356/7301/5769) | ⚠️ name-mapped, partial pre-propagation | canonical_name | converges after Friday job-43 |
| petites-annonces | `annonces` | ✅ clean canonical (parent grain) | npa only | canonical_name/bfs | completes after re-llm→lamap listings sync |

Shared RPCs live on lamap_db: `get_cantons(text)`, `get_commune_options(text,text)`,
`get_facets(text,int)`. Migrations: `2026-05-28b` (shared API), `2026-05-28c`
(transactions), `2026-05-28d` (re-llm listings root fix).
