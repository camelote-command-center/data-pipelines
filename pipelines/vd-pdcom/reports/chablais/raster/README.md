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
