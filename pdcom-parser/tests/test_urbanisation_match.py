"""Unit tests for the v0.5.2 urbanisation v2 keyword matcher (promoteur-focused)."""
from pdcom_parser.urbanisation_extractor import (
    extract_year_from_filename,
    match_label_to_category,
    normalize_for_match,
)


# ─── normalize_for_match ───────────────────────────────────────────────────────

def test_normalize_strips_accents_and_lowercases():
    assert normalize_for_match("Périmètre de Densification Accrue") == "perimetre de densification accrue"


def test_normalize_preserves_parens():
    out = normalize_for_match("Zone 5 (résidentielle)")
    assert "(" in out and ")" in out
    assert "residentielle" in out


def test_normalize_drops_pdcn_boilerplate():
    out = normalize_for_match("MZ court-moyen, selon fiche A03 du PDCn")
    assert "fiche" not in out
    assert "pdcn" not in out


# ─── Direct category hits ──────────────────────────────────────────────────────

def test_zone_5_match():
    m = match_label_to_category("Zone 5 résidentielle")
    assert m is not None
    assert m.category_key == "zone_5"


def test_z5_short_match():
    m = match_label_to_category("Z5")
    assert m is not None
    assert m.category_key == "zone_5"


def test_zone_5_ferroviaire_excluded():
    """zone 5 ferroviaire is something else — must not match zone_5."""
    m = match_label_to_category("Zone 5 ferroviaire")
    # Either no match, or a different category — must NOT be zone_5
    if m is not None:
        assert m.category_key != "zone_5"


def test_densification_accrue_match():
    m = match_label_to_category("Périmètre de densification accrue")
    assert m is not None
    assert m.category_key == "densification_accrue"


def test_densification_anieres_variant():
    """Anières-style 'densification de type 1/2/3'."""
    m = match_label_to_category("Densification de type 2")
    assert m is not None
    assert m.category_key == "densification_accrue"


def test_densification_bernex_variant():
    m = match_label_to_category("Fort potentiel de densification accrue")
    assert m is not None
    assert m.category_key == "densification_accrue"


def test_plq_a_etablir_match():
    m = match_label_to_category("PLQ à établir")
    assert m is not None
    assert m.category_key == "plq_a_etablir"


def test_plq_realise_match():
    m = match_label_to_category("PLQ en force")
    assert m is not None
    assert m.category_key == "plq_realise"


def test_a_proteger_match():
    m = match_label_to_category("Secteur à protéger")
    assert m is not None
    assert m.category_key == "a_proteger"


def test_a_menager_match():
    m = match_label_to_category("Quartier en zone 5, à ménager")
    # Both 'zone 5' and 'à ménager' — a_proteger wins via priority
    assert m is not None
    assert m.category_key == "a_proteger"
    assert "zone_5" in m.alternate_categories


def test_zone_agricole_match():
    m = match_label_to_category("Zone agricole — couloir à faune")
    assert m is not None
    assert m.category_key == "a_proteger"


def test_rives_du_lac_match():
    m = match_label_to_category("Rives du Lac")
    assert m is not None
    assert m.category_key == "a_proteger"


# ─── Priority resolution & alternates ──────────────────────────────────────────

def test_densification_beats_zone_5():
    """A label with both 'zone 5' and 'densification accrue' must resolve to
    densification_accrue (more specific)."""
    m = match_label_to_category("Zone 5 — périmètre de densification accrue")
    assert m is not None
    assert m.category_key == "densification_accrue"
    assert "zone_5" in m.alternate_categories


def test_alternate_categories_populated():
    """When multiple match, alternates list everything except resolved."""
    m = match_label_to_category("Zone 5 à protéger")
    assert m is not None
    assert m.category_key == "a_proteger"
    assert "zone_5" in m.alternate_categories


# ─── Excludes ──────────────────────────────────────────────────────────────────

def test_plq_a_etablir_excludes_realise():
    """'PLQ réalisé' should not match plq_a_etablir despite 'plq' substring,
    because 'réalisé' is in keywords_excludes."""
    m = match_label_to_category("PLQ réalisé en zone 5")
    assert m is not None
    # plq_a_etablir is excluded, plq_realise wins
    assert m.category_key == "plq_realise"
    assert "plq_a_etablir" not in m.alternate_categories  # truly excluded


def test_exploitable_excludes_non():
    m = match_label_to_category("Non constructible")
    # 'constructible' substring would match exploitable, but 'non constructible'
    # is in keywords_excludes — must not match exploitable
    if m is not None:
        assert m.category_key != "exploitable"


def test_inconstructible_does_not_match_exploitable():
    """'Surface inconstructible' must NOT match exploitable just because
    'constructible' is a substring inside 'inconstructible'. Word-boundary check."""
    m = match_label_to_category("Surface inconstructible")
    assert m is not None
    assert m.category_key == "a_proteger"  # inconstructible → a_proteger


def test_distance_inconstructible_matches_a_proteger():
    m = match_label_to_category("distance inconstructible")
    assert m is not None
    assert m.category_key == "a_proteger"


def test_constructible_alone_matches_exploitable():
    """'Constructible' as a standalone word must still match exploitable."""
    m = match_label_to_category("zone constructible")
    assert m is not None
    assert m.category_key == "exploitable"


# ─── Drop ──────────────────────────────────────────────────────────────────────

def test_off_topic_drops():
    """Basemap clutter / heritage / mobility — none of the 6 categories."""
    assert match_label_to_category("Bâtiment patrimoine bâti recensé") is None
    assert match_label_to_category("Arrêt TPG") is None
    assert match_label_to_category("Cheminement piéton") is None
    assert match_label_to_category("") is None
    assert match_label_to_category(None) is None  # type: ignore[arg-type]


# ─── Year extraction ───────────────────────────────────────────────────────────

def test_year_extraction():
    assert extract_year_from_filename("pdcom_lancy_2020-carte-de-synthese.pdf") == 2020
    assert extract_year_from_filename("pdcom_perly-certoux_carte-de-synthese-mai-2018.pdf") == 2018
    assert extract_year_from_filename("pdcom_anieres_carte-densification-v2.pdf") is None
    # Multiple years — pick max
    assert extract_year_from_filename("plan_2018_revised_2024.pdf") == 2024
