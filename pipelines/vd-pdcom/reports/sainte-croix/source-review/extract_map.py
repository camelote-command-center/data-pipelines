"""Reproduce original image extraction from the adjacent official report.

Usage: python extract_map.py /path/to/reserved-report.pdf /output/directory
No geographic transformation or parcel interpretation is performed.
"""
import hashlib
import json
import pathlib
import sys

import pymupdf

source, target = map(pathlib.Path, sys.argv[1:])
expected = 'bb29086f272cd70e43fcf7c4dc9d59be5cbda29aab24c7e4d675e3d2200fc90d'
assert hashlib.sha256(source.read_bytes()).hexdigest() == expected
reference = json.loads(pathlib.Path(__file__).with_name('embedded-map.json').read_text())
target.mkdir(parents=True, exist_ok=True)
with pymupdf.open(source) as doc:
    page = doc[5]
    assert len(page.get_drawings()) == reference['native_paths'] == 0
    assert {x[0] for x in page.get_images(full=True)} == {x['xref'] for x in reference['images']}
    for item in reference['images']:
        image = doc.extract_image(item['xref'])
        assert hashlib.sha256(image['image']).hexdigest() == item['sha256']
        assert [image['width'], image['height']] == [item['width'], item['height']]
        assert [list(r) for r in page.get_image_rects(item['xref'])] == item['placement_pdf_points']
        (target / item['file']).write_bytes(image['image'])
print('Verified three original images, dimensions, hashes and placement; no geography.')
