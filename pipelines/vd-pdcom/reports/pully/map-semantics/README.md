# Pully map semantics and later Boverattes context

Continuation of PR99's source review, 16 September 2026. Source document `98ffb71a-e9e2-5643-9baf-93c42967cb30`, SHA256 `02a96fd6036e51c77e45ab188168e1c451d40cef8b71204e1271151431173d6d`.

## Two reviewed legends, not one development mask

`semantics.json` records 26 labels from urban-organisation PDF37 and 18 from Monts-de-Pully PDF53, with source-composite crop coordinates and eight thematic crosswalks to visually inspected grey objective/measure pages. The other ten map pages remain to be assessed separately.

The broad yellow urban fill combines low, medium and high residential densities. It does not identify empty land or one numerical density. Future-use review and development by special plan are separate categories. Tertiary, craft, public-facility and recreation hatches are not interchangeable housing opportunities.

Monts-de-Pully distinguishes hamlet development, transition, extensive/intensive agriculture, low-productivity land, recreation, public facilities and a motorway-junction reservation. Low agricultural productivity does not establish buildability. Grey PDF50/51 link hamlet development to studies, delimitation and implementation instruments.

These links are thematic interpretation, not proven polygon-to-objective equivalence. The PDF71 approval restriction to numbered grey pages still applies: the white map contours remain illustrative. No geographic fit or parcel capacity is asserted.

## Boverattes has later planning and material implementation

The official [special-plan listing](https://www.pully.ch/pully-pratique/urbanisme-environnement/plans-speciaux-durbanisme/) links a one-page PPA and an eleven-page regulation. Both are retained as adjacent evidence, **not counted as additional PDCom documents or covered communes**.

The signed/stamped plan gives council adoption 14 November 2007, departmental preliminary approval 19 October 2009, and entry into force 14 December 2010. Regulation PDF5/article4 names eight historical parcel numbers. They have not been resolved to current cadastral identities or used as an opportunity list.

Regulation PDF5/article2 references a separate **Plan directeur localisé Boverattes**. That exact adopted document is still missing. An indexed municipal 2007 proposal URL returns HTTP404; this failed route does not prove the PDL is unavailable elsewhere.

Regulation PDF10/article37 and the map distinguish public-utility and housing alternatives for the option areas. The source requires a municipal choice/publication; the applicable published choice is unverified. Alternative diagrams must not be added together. The PPA also contains non-buildable access and open-space areas, so its outer perimeter is not a net development footprint.

The official [municipal housing page](https://www.pully.ch/pully-pratique/affaires-sociales/logements-communaux) reports 35 adapted dwellings in the Boverattes neighbourhood opened in November 2020. This is evidence of implementation after the1996 PDCom. It does not prove that all eight historical parcels are built out or that residual capacity is zero.

`boverattes-context.json` preserves these distinctions, exact source URLs/hashes, source-page references and the failed retrieval. Next acquire the referenced PDL, verify applicable option decisions/later changes and current cadastral/building conditions before estimating residual potential. Separately, the historical hamlet-development category remains a candidate for carefully controlled image alignment.

Run `python verify.py` for artifact/source hashes, exact crop replay, legend identifiers and crosswalk references. There are no sector, parcel or receiver mutations in this milestone.
