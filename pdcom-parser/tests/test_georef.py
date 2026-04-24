from pdcom_parser.georef import detect_commune_boundary


def test_commune_boundary_detectable(page42):
    poly = detect_commune_boundary(page42)
    assert poly is not None
    assert poly.is_valid or poly.buffer(0).is_valid
