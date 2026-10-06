# Two exact historical text references

Source registry IDs/hashes:

- Blonay `2bcdca02-b3cb-5846-a712-3f538a524557`, SHA256 `ce5224d0effbabac8c62751ac25d97b03e6ac6186307a2cdd9ec809249c837d9`.
- Puidoux `896b38ec-fef9-5f88-9ccd-345d17adf737`, SHA256 `8cd5586803b24104aa1afa42e22b229ca3460fcdc01248f22c69410949c6769e`.

Both current official downloads matched the existing registered bytes on6October2026.
The independent source and page-content review records are included. Source signatures
and landing dates support historical approval only, never current applicability.
Each representation artifact inventories every physical page but contains searchable
text only for the independently selected29/15pages. Unselected text is empty;
original layer and representation hashes preserve the withheld-content boundary.
No new OCR was generated. Puidoux OCR provenance comes from existing registered
research extraction; no confidence values or perfect-recognition claims are invented.

`historical_reference.py` pins the complete review hash, which pins the artifact hash
and review evidence hashes. The build gate rechecks actual PDF bytes, page dimensions,
original text layer and extraction runtime. Delivery reconstructs the expected chunk
content, visible caveats, citations and all metadata from immutable local fixtures.
This rejects relabeling either reference as current, extra/missing/changed chunks,
changed source scope, altered provenance, or searchable content on withheld pages.

Prepared production bundles are not evidence that delivery occurred. An owned rollback
and separate parent-reviewed live operation are required; this PR executes neither.

The review limits retain the earlier candidate-stage sentence that page selection
needed review. The included `independent-content-review.json`, hash-bound by the
final contract, records acceptance of the exact44 selected page texts and supersedes
that preparation-stage gate. It does not remove OCR, source-vintage or currentness
limitations. Commit acknowledgement loss produces a stable-ID unknown-outcome receipt;
it must not be described as rolled back or automatically retried.
