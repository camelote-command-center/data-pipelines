# Official planning-document source inventory — 13 September 2026

Observed by the parser against [geodienste NPL](https://www.geodienste.ch/services/npl_nutzungsplanung) and [Zürich ÖREB documents](https://oerebdocs.zh.ch/?themen%5Bid%5D%5B%5D=1).

111,495 reference records; 105,020 carry a URL. Source-provided canton/BFS pairs: 673. These are **not unique documents or fully covered communes**. Repeated references share downloads/text. BFS identifiers may describe historical municipalities. No numeric-rule or PDCom polygon completeness claim.

| Canton | Observed export/register status | References | With URL |
|---|---|---:|---:|
| AG | no_document_records | 0 | 0 |
| AI | no_document_records | 0 | 0 |
| AR | no_document_records | 0 | 0 |
| BE | catalogued | 71,711 | 71,711 |
| BL | catalogued | 1,274 | 1,274 |
| BS | catalogued | 768 | 768 |
| FR | catalogued | 3,493 | 0 |
| GE | no_document_records | 0 | 0 |
| GL | not_published | 0 | 0 |
| GR | no_document_records | 0 | 0 |
| JU | catalogued | 408 | 408 |
| LU | no_document_records | 0 | 0 |
| NE | catalogued | 1,234 | 0 |
| NW | restricted | 0 | 0 |
| OW | restricted | 0 | 0 |
| SG | catalogued | 6,580 | 6,580 |
| SH | catalogued | 1,405 | 1,405 |
| SO | catalogued | 11,396 | 11,396 |
| SZ | no_document_records | 0 | 0 |
| TG | catalogued | 7,619 | 7,619 |
| TI | not_published | 0 | 0 |
| UR | catalogued | 642 | 642 |
| VD | restricted | 0 | 0 |
| VS | catalogued | 1,748 | 0 |
| ZG | no_document_records | 0 | 0 |
| ZH | catalogued | 3,217 | 3,217 |

`no_document_records` means the inspected export has no document objects, not that the canton has no regulations. `not_published` means absent from this STAC collection. NW/OW/VD were not downloaded because this service requires authorization. FR/NE/VS references lack document URLs in these exports. Zürich scope is general Nutzungsplanung; detailed plans and other themes require a separate scope decision.

Existing Geneva municipal and custom PDCom feeds are retained. Next coverage work: resolve official URLs for FR/NE/VS; add source adapters for missing export content; then validate municipality-level regulation completeness and source-linked numerical rules. Current source inventory alone cannot establish Popety parity.

Discovery run `7d12daa2-d63b-45c1-9757-6ba01e3d5ce7`. Live ingestion and per-document outcomes are separate records in RE-LLM `bronze_ch.planning_document_runs` and Pixxels datasets #267/#269.
