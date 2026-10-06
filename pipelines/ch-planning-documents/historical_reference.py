"""Exact Blonay/Puidoux historical text references; no registry status promotion.

Both source identities, complete review contracts and frozen representations are
pinned. This does not broaden the legacy Saint-Légier contract or approve plans.
"""
import hashlib
import json
from pathlib import Path

SOURCE = 'vd_pdcom_historical_reference'
ROOT = Path(__file__).parent / 'reports' / 'blonay-puidoux-historical-text'
# Full contracts are pinned after independent source and page-selection review.
PINS = {'2bcdca02-b3cb-5846-a712-3f538a524557': {'review_sha256': 'b456495fc3a28d898615f0f862d89a2a484ce9c8fad029bcc3c9a5b118566506', 'source_sha256': 'ce5224d0effbabac8c62751ac25d97b03e6ac6186307a2cdd9ec809249c837d9', 'page_count': 145, 'artifact': 'blonay-representations.json', 'tracking_bfs': 5892, 'selected_pages': [3, 4, 6, 8, 15, 17, 19, 21, 25, 37, 38, 39, 40, 41, 44, 52, 56, 57, 58, 59, 60, 61, 64, 65, 66, 67, 68, 97, 98], 'representation': 'embedded_pdf_text'}, '896b38ec-fef9-5f88-9ccd-345d17adf737': {'review_sha256': '7f98fd1a13e1f9b3107ba8c7d0c2f9af8325497541379b6b5dbb12d86ff64019', 'source_sha256': '8cd5586803b24104aa1afa42e22b229ca3460fcdc01248f22c69410949c6769e', 'page_count': 74, 'artifact': 'puidoux-representations.json', 'tracking_bfs': 5607, 'selected_pages': [4, 5, 6, 8, 9, 14, 16, 17, 18, 19, 23, 24, 25, 26, 27], 'representation': 'existing_uncorrected_research_ocr'}}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def validate_review(r):
    pin = PINS.get(r.get('vd_document_id'))
    require(pin is not None, 'historical_reference_source_not_reviewed')
    require(digest(r) == pin['review_sha256'], 'historical_reference_review_changed')
    require(r.get('plan_status') == 'unverified' and r.get('source_role') == 'historical_source_reference', 'historical_reference_registry_status_required')
    require(r.get('historical_reference', {}).get('current_applicability') == 'unverified', 'historical_reference_currentness_required')
    require(not any(r.get(k) for k in ('historical_scope', 'native_page_selection', 'text_extraction', 'mixed_representations')), 'historical_reference_exclusive_mode_required')
    require(r['source_sha256'] == pin['source_sha256'] and r['page_count'] == pin['page_count'], 'historical_reference_identity_changed')
    return pin


def evidence(r):
    pin = validate_review(r)
    for name, key in [('independent-source-review.json', 'independent_source_review_sha256'),
                      ('independent-content-review.json', 'independent_content_review_sha256')]:
        require(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == r['historical_reference'][key], 'historical_reference_independent_review_changed')
    raw = (ROOT / pin['artifact']).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == r['historical_reference']['artifact_sha256'], 'historical_reference_artifact_changed')
    e = json.loads(raw)
    require(e['document_id'] == r['vd_document_id'] and e['source_sha256'] == r['source_sha256'], 'historical_reference_artifact_identity_changed')
    require([p['page_number'] for p in e['pages']] == list(range(1, r['page_count'] + 1)), 'historical_reference_page_inventory_changed')
    require([p['page_number'] for p in e['pages'] if p['include_in_search']] == pin['selected_pages'], 'historical_reference_selection_changed')
    for p in e['pages']:
        require(type(p['include_in_search']) is bool and bool(p['selection_reason']), 'historical_reference_selection_required')
        require(hashlib.sha256(p['text'].encode()).hexdigest() == p['text_sha256'], 'historical_reference_text_changed')
        require(bool(p['text'].strip()) if p['include_in_search'] else p['text'] == '', 'historical_reference_withheld_content')
        require(p['representation'] == pin['representation'], 'historical_reference_representation_changed')
        require(p['confidence'] is None and p['confidence_status'] == 'not_recorded', 'historical_reference_no_invented_confidence')
    return e


def frozen_pages(r):
    e = evidence(r)
    return [dict(p, status='historical_reference_search_text' if p['include_in_search'] else 'historical_reference_withheld') for p in e['pages']]


def load_pages(pdf, r):
    import fitz
    e = evidence(r)
    require(fitz.VersionBind == e['pymupdf_version'], 'historical_reference_runtime_changed')
    require(len(pdf) == len(e['pages']), 'historical_reference_pdf_page_count_changed')
    for original, p in zip(pdf, e['pages']):
        text = original.get_text('text')
        require(hashlib.sha256(text.encode()).hexdigest() == p['original_pdf_text_sha256'] and len(text) == p['original_pdf_char_count'], 'historical_reference_original_layer_changed')
        require(original.rect.width == p['width'] and original.rect.height == p['height'], 'historical_reference_page_dimensions_changed')
        if p['representation'] == 'embedded_pdf_text':
            require(not p['include_in_search'] or text == p['text'], 'historical_reference_embedded_text_changed')
        else:
            require(not text.strip(), 'historical_reference_ocr_requires_empty_original')
    return frozen_pages(r)


def assemble(r, pages, byte_count):
    from vd_pdcom_text import uid
    require(pages == frozen_pages(r), 'historical_reference_pages_not_exact_frozen_evidence')
    url, sha = r['source_url'], r['source_sha256']
    doc = uid('knowledge:' + url + ':' + sha)
    provenance = {
        'parser': SOURCE, 'vd_document_id': r['vd_document_id'], 'content_hash': sha,
        'source_url': url, 'commune_bfs': r['commune_bfs'], 'scope': r['scope'],
        'approval_scope': r['signed_approval'], 'plan_status': 'unverified',
        'source_role': r['source_role'], 'historical_reference': r['historical_reference'],
        'diagnostic_vintage_limit': r['diagnostic_vintage_limit'], 'limits': r['limits'],
        'all_prose_is_binding': False, 'spatial_qualification': False,
        'commune_complete': False, 'review_sha256': digest(r),
    }
    chunks = []
    for p in pages:
        for start in range(0, len(p['text']), 2000):
            content = p['text'][start:start + 2000].strip()
            if content:
                n = len(chunks)
                chunks.append({
                    'id': uid(doc + ':' + str(n)), 'document_id': doc, 'chunk_index': n,
                    'page_number': p['page_number'], 'content': r['historical_reference']['human_readable_caveat'] + '\n\n' + content,
                    'metadata': dict(provenance, citation=url + '#page=' + str(p['page_number']),
                                     extraction_status=p['status'], representation=p['representation'],
                                     page_provenance={k: v for k, v in p.items() if k != 'text'}),
                })
    require(bool(chunks), 'historical_reference_no_searchable_text')
    metadata = dict(provenance, review_contract=r, physical_page_count=r['page_count'],
                    page_manifest=[{k: v for k, v in p.items() if k != 'text'} for p in pages],
                    extraction_status='partial_historical_reference_text', map_pages=None,
                    fully_certified_pages=0, full_visual_map_extraction=False)
    return {'review': r, 'pages': pages, 'byte_count': byte_count, 'chunks': chunks,
            'source_id': uid(SOURCE + ':VD:' + r['vd_document_id']),
            'version_id': uid('version:' + url + ':' + sha), 'document_id': doc, 'metadata': metadata}


def validate_registered_mapping(cursor, r):
    pin = validate_review(r)
    cursor.execute('SELECT commune_bfs,coverage_extent FROM bronze_ch.vd_pdcom_document_communes WHERE document_id=%s ORDER BY commune_bfs', (r['vd_document_id'],))
    require(cursor.fetchall() == [(pin['tracking_bfs'], 'unverified')], 'historical_reference_registry_mapping_changed')


def validate_delivery(document, chunks):
    r = document.get('review_contract', {})
    validate_review(r)
    expected = assemble(r, frozen_pages(r), 0)
    require(document == expected['metadata'], 'historical_reference_document_metadata_changed')
    require(sorted(chunks, key=lambda x: x['chunk_index']) == expected['chunks'], 'historical_reference_chunk_content_or_provenance_changed')
