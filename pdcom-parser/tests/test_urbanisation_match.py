"""Unit tests for the v0.5.2 urbanisation v2 keyword matcher (promoteur-focused)."""
from pdcom_parser.urbanisation_extractor import (
    extract_year_from_filename,
    is_meta_label,
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


# ─── Meta-label rejection (v2.2) ───────────────────────────────────────────────

def test_meta_filename_jumble_rejected():
    """Filename leakage: digit-prefixed alphanumeric jumble."""
    assert is_meta_label("19181_Stratz5-biodiv9000_200929-awy") is True
    assert match_label_to_category("19181_Stratz5-biodiv9000_200929-awy") is None


def test_meta_fiche_reference_rejected():
    """Fiche XX: section reference, not a zone label."""
    assert is_meta_label("Fiche 07a: Stratégie d'évolution de la zone 5 (ouest)") is True
    assert is_meta_label("FICHE 12: Modification de zone") is True
    assert is_meta_label("Fiche 07b. Stratégie d'évolution") is True
    assert match_label_to_category("Fiche 07a: Stratégie d'évolution de la zone 5") is None


def test_meta_strategie_title_rejected():
    """Stratégie d… document title prefix."""
    assert is_meta_label("Stratégie d'évolution de la zone 5") is True
    assert is_meta_label("STRATÉGIE D'ÉVOLUTION DE LA ZONE 5") is True


def test_meta_all_caps_header_rejected():
    """Long all-caps document headers."""
    assert is_meta_label("PLAN DIRECTEUR COMMUNAL D'ANIERES") is True
    assert is_meta_label("CARTE DE SYNTHÈSE COMMUNALE") is True


def test_meta_short_uppercase_not_rejected():
    """'Z5' / 'PLQ' alone should NOT be rejected as meta — too short."""
    assert is_meta_label("Z5") is False
    assert is_meta_label("PLQ") is False
    assert is_meta_label("MZ") is False


def test_meta_real_legend_label_passes():
    """Real legend labels with mixed case should pass meta-rejection."""
    assert is_meta_label("Zone 5") is False
    assert is_meta_label("Périmètre de densification accrue") is False
    assert is_meta_label("Zone agricole") is False
    assert is_meta_label("PLQ à établir") is False


def test_meta_strategie_uppercase_in_keyword_match_path():
    """Even the actual Vandoeuvres-style 'Stratégie d'évolution de la zone 5'
    must NOT contribute features to zone_5 — it's a title, not a legend."""
    # Confirm it's caught by meta-rejection (the matcher returns None)
    assert match_label_to_category("Stratégie d'évolution de la zone 5") is None
    assert match_label_to_category("STRATÉGIE D'ÉVOLUTION DE LA ZONE 5") is None


def test_meta_does_not_clobber_real_a_proteger():
    """A real label with 'Stratégie' as a non-prefix must still be checked
    for keywords. Currently meta-rejection only fires on prefix patterns
    so 'Renforcer la stratégie de protection' should NOT be rejected."""
    assert is_meta_label("Renforcer la stratégie de protection") is False


# ─── Year extraction ───────────────────────────────────────────────────────────

def test_year_extraction():
    assert extract_year_from_filename("pdcom_lancy_2020-carte-de-synthese.pdf") == 2020
    assert extract_year_from_filename("pdcom_perly-certoux_carte-de-synthese-mai-2018.pdf") == 2018
    assert extract_year_from_filename("pdcom_anieres_carte-densification-v2.pdf") is None
    # Multiple years — pick max
    assert extract_year_from_filename("plan_2018_revised_2024.pdf") == 2024
