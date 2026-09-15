# Bex PUM.7 outline and official parcel-reference gap

The exact PA4 measures PDF76 inset is converted into a qualified outline using red-fill/overprinted-building selection, circular closing radii 1/2/3 pixels, largest **4-connected** component, hole filling and 0.5-pixel topology-preserving simplification. Closing/hole filling infer continuity through pale parcel lines and road gaps; they do not produce an exact legal boundary. Four-connectivity avoids interpreting diagonal pixel contacts as a single valid polygon. Input SHA is enforced.

The three variants and selected radius2 overlay were visually reviewed. Repeated extraction produces identical geometry/masks. Selected area: 111,389.021 m², wholly inside Bex. Radius1/3 areas:111,365.254–111,410.560 m². The source states110,371m²: +1,018.021m² discrepancy remains unexplained. Do not adjust the fit to force agreement. Source approval remains unverified and the sheet explicitly requires further perimeter refinement.

## Parcel QA

Registered `silver_ch.cadastral_plots` has4,388 Bex rows but zero polygon intersections in this location. An official Vaud layer21 envelope query returned136 unique EGRIDs, reconciled against a separate count request. The selected outline intersects65 objects (55 private parcels,10 communal public-domain objects), all65 absent from the registered mirror. These are all land-type objects; no surface rights were returned among the intersections. This is a **mirror coverage gap**, not proof that no parcels are affected.

All three cleanup variants intersect the same65 objects. Selected overlap includes8 objects below1% of parcel area and none below1m². Illustrative buffers:

| Buffer | Intersections |
|---|---:|
| −10m | 53 |
| −5m | 54 |
| 0m | 65 |
| +5m | 72 |
| +10m | 76 |

Buffers are sensitivity scenarios, not demonstrated positional-error bounds. The raw official references and proposed overlaps are frozen as **LV95/EPSG2056 research artifacts**. No parcel/sector/receiver database rows have been added by this work.

## Integration prerequisite

The current type-refresh job derives expected commune identity solely from the registered silver mirror and would reject these65 missing EGRIDs. Before delivery, implement a scoped official-reference ingestion path with source URL, timestamp/hash, current commune checks, real PostGIS geometry and annual refresh within the Pixxels parser. Do not disguise the objects as federal-mirror records or weaken identity validation. Then replay the qualified Bex sector and references, verify Bex-only attribution, type identities and private receiver parity. No new public release is implied.

`parcel-qa.json` preserves the empty registered-mirror result. `official-parcel-qa.json` includes query/count URLs, hash/time, EGRID lists and gap verification. `check_parcels.py` reproduces all seven intersection sets from frozen geometry without mutation.
