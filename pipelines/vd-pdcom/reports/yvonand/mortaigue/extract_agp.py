"""Recover the exact AGP paint contour in the 1:5000 overview, not the 1:1000 inset."""
import argparse, hashlib, json
from pathlib import Path
import pymupdf
from shapely.geometry import Polygon, mapping

def extract(pdf):
    assert hashlib.sha256(pdf.read_bytes()).hexdigest() == '4de692dfdbfa3e3ba280d97950d0d770ec744756ca33b853346f47b745cf3f1a'
    doc = pymupdf.open(pdf)
    page = doc[0]
    path = page.get_drawings()[98]
    assert len(path['items']) == 12 and path['even_odd'] and path['type'] == 'fs'
    assert all(item[0] == 'l' for item in path['items'])
    points = [list(path['items'][0][1])]
    for _, start, end in path['items']:
        assert list(start) == points[-1]
        points.append(list(end))
    assert points[0] == points[-1]
    polygon = Polygon(points)
    assert polygon.is_valid and polygon.area > 0
    result = {
        'source_sha256': hashlib.sha256(pdf.read_bytes()).hexdigest(),
        'page': 1, 'drawing_index': 98, 'seqno': path['seqno'],
        'coordinate_space': 'unrotated PDF page points, origin top-left; not geographic',
        'page_size_points': list(page.rect), 'page_rotation': page.rotation,
        'geometry': mapping(polygon), 'area_square_points': polygon.area,
        'map_panel': 'Plan d’ensemble', 'printed_scale': '1:5000',
        'separate_detail_scale': '1:1000; does not contain this Mordagne contour',
        'meaning': 'proposed zone agricole protégée 16 LAT (AGP), western parcel 326',
        'source_table_partial_parcel_area_m2': 10411,
        'limitations': ['source table area is not a derived geographic measurement',
                        'no ground transform or independent alignment validation',
                        'inquiry version; later final approval not established',
                        'not a development opportunity or effective parcel entitlement'],
        'runtime_release': 'withheld',
    }
    return result

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pdf', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    args.out.write_text(json.dumps(extract(args.pdf), ensure_ascii=False, indent=2) + '\n')
