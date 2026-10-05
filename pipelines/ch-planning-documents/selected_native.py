"""Reviewed page selection from the unchanged existing PDF text layer."""
import hashlib

def require(ok, reason):
    if not ok:
        raise ValueError(reason)

def validate_review(review):
    require(not review.get('text_extraction'), 'native_selection_not_ocr')
    require(review.get('scope','municipal')=='municipal' and type(review.get('commune_bfs')) is int, 'native_selection_municipal_scope_required')
    selection=review['native_page_selection']
    require(selection.get('mode')=='reviewed_existing_pdf_text' and selection.get('partial') is True, 'native_selection_mode_required')
    require(bool(selection.get('pymupdf_version')) and selection.get('extraction_call')=="page.get_text('text')", 'native_selection_runtime_required')
    require(bool(selection.get('currentness_caveat')) and bool(selection.get('coverage_limit')), 'native_selection_limits_required')
    pages=selection.get('pages',[])
    require([p.get('page_number') for p in pages]==list(range(1,review['page_count']+1)), 'native_selection_complete_page_inventory_required')
    for p in pages:
        require(type(p.get('include_in_search')) is bool and bool(p.get('reason')) and bool(p.get('role')), 'native_selection_reason_required')
        require(len(p.get('original_text_sha256',''))==64 and type(p.get('original_char_count')) is int and p['original_char_count']>=0, 'native_selection_original_identity_required')

def load_pages(pdf,review):
    validate_review(review)
    import fitz
    require(fitz.VersionBind==review['native_page_selection']['pymupdf_version'], 'native_selection_runtime_changed')
    pages=[]
    for original,e in zip(pdf,review['native_page_selection']['pages']):
        text=original.get_text('text')
        require(hashlib.sha256(text.encode()).hexdigest()==e['original_text_sha256'] and len(text)==e['original_char_count'], 'native_selection_original_text_changed')
        require(not e['include_in_search'] or bool(text.strip()), 'native_selection_empty_search_page')
        pages.append({'page_number':e['page_number'],'text':text if e['include_in_search'] else '',
                      'method':'pymupdf_existing_pdf_text_layer','status':'native_text_extracted' if e['include_in_search'] else 'native_text_withheld',
                      'width':original.rect.width,'height':original.rect.height,'native_selection_provenance':e})
    return pages

def validate_delivery(document,chunks):
    selection=document['native_page_selection']
    require(selection.get('partial') is True and selection.get('mode')=='reviewed_existing_pdf_text' and bool(selection.get('currentness_caveat')) and bool(selection.get('coverage_limit')), 'native_delivery_limits_required')
    approved={p['page_number']:p for p in selection['pages'] if p['include_in_search']}
    require(bool(chunks), 'native_delivery_chunks_required')
    for m in chunks:
        require(m.get('native_page_selection')==selection and m.get('diagnostic_vintage_limit')==document.get('diagnostic_vintage_limit') and m.get('limits')==document.get('limits'), 'native_delivery_caveat_mismatch')
        p=m.get('native_selection_provenance',{})
        require(p==approved.get(p.get('page_number')) and p.get('include_in_search') is True and m.get('extraction_status')=='native_text_extracted', 'native_delivery_unreviewed_page')


def validate_pages(review,pages):
    validate_review(review)
    ledger=review['native_page_selection']['pages']
    require(len(pages)==len(ledger), 'native_selection_pages_missing')
    for p,e in zip(pages,ledger):
        require(p['page_number']==e['page_number'] and p.get('native_selection_provenance')==e, 'native_selection_page_provenance_changed')
        require(p.get('method')=='pymupdf_existing_pdf_text_layer', 'native_selection_method_changed')
        if e['include_in_search']:
            require(p['status']=='native_text_extracted' and len(p['text'])==e['original_char_count'] and hashlib.sha256(p['text'].encode()).hexdigest()==e['original_text_sha256'], 'native_selection_search_text_changed')
        else:
            require(p['status']=='native_text_withheld' and p['text']=='', 'native_selection_withheld_content')
