"""Pinned, explicitly partial research OCR; never overwrites original PDF text."""
import hashlib
import json
from pathlib import Path

FIELDS = ('ocr_quality', 'requires_source_image_verification', 'corpus_limits', 'supporting_documents')

def require(value, reason):
    if not value:
        raise ValueError(reason)

def validate_review(review):
    require(review.get('scope','municipal')=='municipal' and type(review.get('commune_bfs')) is int, 'ocr_municipal_scope_required')
    require(review.get('ocr_quality') == 'uncorrected_machine_ocr', 'ocr_quality_required')
    require(review.get('requires_source_image_verification') is True, 'ocr_image_verification_required')
    require(bool(review.get('corpus_limits')), 'ocr_corpus_limits_required')
    config = review['text_extraction']
    require(config.get('mode') == 'reviewed_ocr', 'reviewed_ocr_mode_required')
    name = Path(config['artifact'])
    require(not name.is_absolute() and '..' not in name.parts and name.suffix == '.json', 'ocr_artifact_path_invalid')
    require(len(config.get('sha256', '')) == 64, 'ocr_artifact_hash_required')
    require(bool(review.get('supporting_documents')), 'ocr_supporting_documents_required')
    support={d['document_id']:d for d in review['supporting_documents']}
    require(len(support)==len(review['supporting_documents']), 'ocr_duplicate_supporting_document')
    references=[dict(review['signed_approval'], evidence_pdf_pages=[review['signed_approval'].get('evidence_pdf_page')])] + review.get('reservations',[])
    for ref in references:
        evidence=support.get(ref.get('evidence_document_id'))
        require(evidence and evidence['sha256']==ref.get('evidence_source_sha256') and all(type(n) is int and 1<=n<=evidence['page_count'] for n in ref.get('evidence_pdf_pages',[])), 'ocr_cross_document_evidence_mismatch')

def load_pages(pdf, review):
    validate_review(review)
    config = review['text_extraction']
    path = Path(__file__).parent / 'reports' / config['artifact']
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == config['sha256'], 'ocr_artifact_hash_mismatch')
    evidence = json.loads(data)
    require(evidence['document_id'] == review['vd_document_id'] and evidence['source_sha256'] == review['source_sha256'], 'ocr_source_identity_mismatch')
    require([p['page_number'] for p in evidence['pages']] == list(range(1, len(pdf)+1)), 'ocr_page_coverage_mismatch')
    pages = []
    for original, e in zip(pdf, evidence['pages']):
        require(not original.get_text('text').strip(), 'reviewed_ocr_requires_empty_native_page')
        text = e['raw_ocr_text']
        require(hashlib.sha256(text.encode()).hexdigest() == e['text_sha256'], 'ocr_text_hash_mismatch')
        require(type(e['include_in_search']) is bool and bool(e['selection_reason']), 'ocr_page_selection_required')
        require(e['confidence'] is None and e['confidence_status'] == 'not_recorded', 'ocr_confidence_not_invented')
        status = 'reviewed_ocr_search_text' if e['include_in_search'] else 'ocr_withheld'
        require(not e['include_in_search'] or bool(text.strip()), 'empty_ocr_search_page')
        pages.append({'page_number': e['page_number'], 'text': text if e['include_in_search'] else '',
                      'method': 'tesseract_fra_frozen_evidence', 'status': status,
                      'width': original.rect.width, 'height': original.rect.height,
                      'ocr_provenance': {k: e[k] for k in ('text_sha256','confidence','confidence_status','selection_reason','include_in_search')},
                      'ocr_artifact_sha256': config['sha256']})
    return pages

def validate_delivery(document, chunks):
    require(document.get('scope','municipal')=='municipal' and type(document.get('commune_bfs')) is int, 'ocr_municipal_scope_required')
    require(document.get('ocr_quality') == 'uncorrected_machine_ocr' and document.get('requires_source_image_verification') is True, 'ocr_delivery_caveat_required')
    require(bool(document.get('corpus_limits')) and bool(chunks), 'ocr_delivery_scope_required')
    for metadata in chunks:
        require(all(metadata.get(k) == document.get(k) for k in FIELDS + ('reservations','diagnostic_vintage_limit','text_extraction')), 'ocr_chunk_caveat_mismatch')
        require(metadata.get('ocr_artifact_sha256')==document['text_extraction']['sha256'], 'ocr_chunk_artifact_mismatch')
        proof = metadata.get('ocr_provenance', {})
        require(proof.get('include_in_search') is True and proof.get('confidence_status') == 'not_recorded' and proof.get('confidence') is None and len(proof.get('text_sha256','')) == 64, 'ocr_chunk_provenance_required')
