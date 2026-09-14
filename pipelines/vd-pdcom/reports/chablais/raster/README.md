# Chablais PA4 source-pixel extraction

This is a reproducible first raster-extraction stage for strategic-volume PDF page 29, not geographic sectors. Input PDF SHA256 is enforced before parsing. The embedded 3627 × 2573 RGB raster is sampled against the printed dark-red legend swatch (`secteur mixte de densification stratégique`). The legend/title area is excluded by a fixed, reviewed map rectangle.

`extract.py` uses Euclidean RGB thresholds 20, 35 and 50, with 8-connected components. It retains the complete raw pixel masks; JSON inventories components with at least 20 matched pixels. No closing, filling or interpolation is used. Pixel coordinates originate at the top left and must never be interpreted as longitude/latitude. The image placement matrix maps the unit image square to PDF points, not image pixels to geographic coordinates.

Results:

| RGB tolerance | Matched pixels | Components ≥20 pixels |
|---|---:|---:|
| 20 | 12,713 | 32 |
| 35 | 14,798 | 29 |
| 50 | 15,672 | 30 |

Visual inspection of `review.png` confirms matches in the printed Aigle, Ollon and Bex areas and also on the Valais side. Geographic/canton assignment has not been performed. Fragment counts change with threshold: labels and roads interrupt fills, and dotted red boundaries of the distinct beyond-2036 category can match too. Therefore none of these numbers is a sector count. The masks preserve unassigned fragments for review rather than inventing complete outlines.

Pending: separate solid-fill interiors from dashed future-development boundaries; recover occluded boundaries only with explicit evidence; align using independent control points and withheld checks; verify Vaud membership and parcel sensitivity. The printed map says boundaries are approximate, at 1:45,000, dated 27 July 2021. No cadastral accuracy or approval is inferred.

Reproduction (NumPy, SciPy, PyMuPDF, Matplotlib):

```sh
python extract.py --pdf /path/to/verified-pa4-strategic.pdf --output /tmp/chablais-raster
```

PyMuPDF/NumPy versions and source/image hashes are saved in `candidates.json`. Repeated extraction was checked for identical masks and component inventories. A changed input PDF is rejected by the checksum gate. These artifacts are research evidence only, outside every public/receiver geographic table. The annual parser continues acquiring the PDF; it does not automatically promote these masks.

## Interior-depth review — 2026-09-15

`cores.py` separates components with substantial coloured interiors from thin strokes, preserving all original masks. At RGB tolerance 35 and interior depth ≥3 source pixels, 25 components / 14,183 pixels have interior support; 615 matched pixels remain in the thin/small group. Depth 2/3/4 retains 33/25/22 components. These component counts include all map regions, not just Vaud, and use no previous 20-pixel area cutoff.

Four reproduced crops were visually reviewed: printed Aigle, Ollon, Bex and a Valais dashed-boundary comparison. Most visible future-development dashes move to the thin group while substantial dark-red interiors remain. **A thin red feature along the Bex railway and small interrupted pieces also move to the thin group.** Thin does not mean semantically rejected: neither group is automatically accepted or discarded. Roads and lettering still split individual fills. This is evidence-assisted separation, not completed category validation. No new outlines, geographic coordinates, commune assignments or parcel rows result.

```sh
python cores.py --input . --output /tmp/chablais-cores
python review_cores.py --pdf /path/to/verified-pa4-strategic.pdf --input . --cores /tmp/chablais-cores
```

Next: review the thin Bex feature and occluded boundaries against detailed source maps before geographic calibration. Retain the future-development category as separate evidence; do not merge it with strategic mixed densification.

### Detailed source follow-up

The official PA4 programme of measures was subsequently acquired (423 pages), checksum `7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5`. PDF page 76, printed page 70, is the PUM.7 Bex-Gare sheet. Its detailed raster inset shows building/railway context suitable for a better calibration attempt. The sheet explicitly says the perimeter still needs refinement through SRGZA work and ongoing planning. This strengthens the reason to preserve approximate boundaries; it does not resolve the thin railway-side pixels as legally inside or outside. The exact source and statement are linked in `cores/runtime-receipt.json`. It joins the annual acquisition manifest for Aigle/Bex/Ollon; exact-version approval remains unverified.
