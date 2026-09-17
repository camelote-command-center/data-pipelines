# Six-commune source-gap review — 17 September 2026

This bounded follow-up reviewed official pages for Cossonay (5477), Chavornay (5749), Vallorbe (5764), Rolle (5861), Le Chenit (5872), and La Tour-de-Peilz (5889). No full PDCom was recovered in this batch. This is **not evidence of absence** and none of the six communes is complete.

`landings.json` records exact page URLs, hashes, times, findings and relevant links. Chavornay and Vallorbe's i-web tables were decoded using the existing bounded `discovery.page_links` implementation; plain HTML anchors alone miss the table contents. There is no new parser defect or code change.

Four adjacent official PDFs (743 pages total) were downloaded and inspected for explicit PDCom references. `sources.json` preserves hashes and `selected-page-text.json` retains matching pages. These zoning/management files do not increase the PDCom corpus. The manifest's page counts describe downloads, not a complete visual review of every page.

| Commune | Verified lead / outstanding work |
|---|---|
| Cossonay | PACom dossier PDF165 references July 2000 PDCom. Recover original document/maps; office-deposit statement at PDF13. |
| Chavornay | Published table includes former Corcelles-sur-Chavornay and Essert-Pittet local plans. Keep predecessor-territory coverage explicit; full PDCom unresolved. |
| Vallorbe | PGA regulation PDF4 explicitly mentions PDCom plus visual synthesis, consultable at municipal secretariat. Obtain actual files without inferring non-existence. |
| Rolle | Management report 2019 PDF82 lists 1999/2000/2001 municipal/council/approval chronology. Exact-version document and approval not recovered. |
| Le Chenit | June–July 2026 PACom inquiry ZIP located. PACom, regional activity-zone plans and mobility studies must not be counted as comprehensive PDCom. |
| La Tour-de-Peilz | 2026 RPGA proposal PDF96 still references PDCom2000; mobility-only sources listed separately. Three old PDC filename matches are permit guides. |

The three La Tour-de-Peilz candidates in `false-positive-reviews.json` were each downloaded and read: one-page guides for works exempt from permission, minor works requiring municipal permission, and works requiring a public inquiry. Reject those exact pending candidates with retained evidence. Preserve any unrelated candidates and all prior commune evidence.

Save one manual discovery attempt per commune, update the existing queue attempt metadata, and retain manual_search_required. No no-plan classification, new PDCom document, geographic sector, receiver mutation or external contact is justified. Next work: recover original documents through additional official archives, inspect the linked Chenit dossier for precise planning references, and continue other pending communes.
