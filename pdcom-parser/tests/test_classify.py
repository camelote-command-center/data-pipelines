from pdcom_parser.classify import classify_page


def test_p42_is_map(page42):
    info = classify_page(page42)
    assert info["type"] == "map"
    assert info["drawing_count"] >= 500


def test_p53_is_map(page53):
    info = classify_page(page53)
    assert info["type"] == "map"


def test_p64_is_map(page64):
    info = classify_page(page64)
    assert info["type"] == "map"
