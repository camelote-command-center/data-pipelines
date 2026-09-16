"""Verify the three-part source corpus without asserting geographic completeness."""
import hashlib
import json
from pathlib import Path
import uuid

import pymupdf

P = Path(__file__).resolve().parent
root = P.parents[2]
sources = json.loads((P / 'sources.json').read_text())
manifest = json.loads((root / 'reviewed_sources.json').read_text())
reviews = json.loads((root / 'document_reviews.json').read_text())
assert [s['page_count'] for s in sources] == [44, 21, 21]
for s in sources:
    raw = (P / f"part-{s['part']}.pdf").read_bytes()
    assert hashlib.sha256(raw).hexdigest() == s['sha256']
    assert s['document_id'] == str(uuid.uuid5(uuid.NAMESPACE_URL, s['source_url']+'#'+s['sha256']))
    matches = [r for r in manifest if r['pdf_url'] == s['source_url']]
    assert len(matches) == 1 and matches[0]['commune_bfs'] == 5606
    assert matches[0]['scope'] == 'communal' and matches[0]['plan_status'] == 'approved'
    inspection = json.loads((P / f"part-{s['part']}-inspection.json").read_text())
    with pymupdf.open(stream=raw, filetype='pdf') as pdf:
        assert len(pdf) == s['page_count'] == len(inspection)
        for i, page in enumerate(pdf):
            assert not page.get_text().strip() and not page.get_drawings()
            assert inspection[i]['page_number'] == i+1
            assert not inspection[i]['embedded_georef']
review = next(r for r in reviews if r['document_id'] == sources[0]['document_id'])
assert review['approval_evidence_page'] == 3 and review['approval_date'] == '2000-02-28'
assert review['receiver_release'] == 'not_ready'
for row in json.loads((P / 'ocr/manifest.json').read_text())['pages']:
    assert 1 <= row['page'] <= sources[row['part']-1]['page_count']
    assert len((P / 'ocr' / f"part-{row['part']}-page-{row['page']}.txt").read_text()) == row['chars']
for name, digest in json.loads((P / 'frozen-hashes.json').read_text()).items():
    assert hashlib.sha256((P / name).read_bytes()).hexdigest() == digest, name
print('Verified three official parts / 86 scanned pages, approval evidence and selected raw OCR.')
