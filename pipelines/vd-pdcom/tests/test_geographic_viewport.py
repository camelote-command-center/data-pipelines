import pymupdf as fitz
from acquire_sources import has_geographic_viewport


def test_relative_length_viewport_is_not_geographic():
    with fitz.open() as doc:
        page = doc.new_page()
        doc.xref_set_key(page.xref, 'VP', '[<</Type/Viewport/Measure<</Type/Measure/Subtype/RL/R(1 mm = 1 m)>>>>]')
        assert not has_geographic_viewport(doc, page)


def test_indirect_geo_and_absent_viewports():
    with fitz.open() as doc:
        page = doc.new_page()
        assert not has_geographic_viewport(doc, page)
        measure = doc.get_new_xref()
        doc.update_object(measure, '<</Type/Measure/Subtype/GEO/GPTS[46 6 47 6 47 7 46 7]>>')
        viewport = doc.get_new_xref()
        doc.update_object(viewport, f'<</Type/Viewport/Measure {measure} 0 R>>')
        doc.xref_set_key(page.xref, 'VP', f'[{viewport} 0 R]')
        assert has_geographic_viewport(doc, page)


def test_cyclic_viewport_references_terminate():
    with fitz.open() as doc:
        page = doc.new_page()
        ref = doc.get_new_xref()
        doc.update_object(ref, f'<</Type/Viewport/Measure {ref} 0 R>>')
        doc.xref_set_key(page.xref, 'VP', f'[{ref} 0 R]')
        assert not has_geographic_viewport(doc, page)
