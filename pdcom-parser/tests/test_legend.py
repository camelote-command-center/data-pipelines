from pdcom_parser.legend import detect_legend


def test_p42_legend_has_multiple_entries(page42):
    legend = detect_legend(page42)
    # Spec §6 baseline: 10 fill colors on p42
    assert len(legend["entries"]) >= 4, f"got {len(legend['entries'])} entries: {[e['label'] for e in legend['entries']]}"
    assert legend["title"], "title should not be empty"
    assert "espace public" in legend["title"].lower()


def test_p53_legend_title_elements_naturels(page53):
    legend = detect_legend(page53)
    assert len(legend["entries"]) >= 4
    title = legend["title"].lower()
    assert "éléments" in title or "elements" in title or "naturels" in title or "semi" in title, title


def test_p64_legend_title_patrimoine(page64):
    legend = detect_legend(page64)
    title = legend["title"].lower()
    assert "patrimoine" in title, title
