"""Pinned OCR and separately authored image transcriptions; no whole-page certification."""
import hashlib
import json
from pathlib import Path

SPAN_METHOD='raw_ocr_selected_span'
METHODS=('raw_fullpage_tesseract_ocr','raw_tesseract_localized_recovery','separately_labeled_assistant_image_grounded_transcription')

def require(ok,reason):
    if not ok:raise ValueError(reason)

def validate_review(r):
    require(r.get('source_role')=='approved_main' and r.get('plan_status')=='approved', 'mixed_only_approved_main_supported')
    require(not r.get('text_extraction') and not r.get('native_page_selection'),'mixed_mode_exclusive')
    require(r.get('scope','municipal')=='municipal' and type(r.get('commune_bfs')) is int,'mixed_municipal_scope_required')
    m=r['mixed_representations'];name=Path(m['artifact'])
    require(m.get('mode')=='reviewed_mixed_representations' and m.get('fully_certified_pages')==0,'mixed_partial_scope_required')
    require(not name.is_absolute() and '..' not in name.parts and name.suffix=='.json' and len(m.get('sha256',''))==64,'mixed_artifact_identity_required')
    for key in ('approval_scope','transport_qualification','currentness_caveat','ocr_quality_limit','representation_limit'):
        require(bool(m.get(key)),'mixed_'+key+'_required')

def evidence(r):
    validate_review(r);m=r['mixed_representations'];raw=(Path(__file__).parent/'reports'/m['artifact']).read_bytes()
    require(hashlib.sha256(raw).hexdigest()==m['sha256'],'mixed_artifact_hash_changed')
    e=json.loads(raw);require(e['document_id']==r['vd_document_id'] and e['source_sha256']==r['source_sha256'],'mixed_source_changed')
    require([p['page_number'] for p in e['pages']]==list(range(1,r['page_count']+1)),'mixed_page_inventory_required')
    if m.get('amendment_map'):
        actual=[{'number':x.get('amendment_number'),'source_physical_page':x.get('physical_page'),'target_printed_page':x.get('target_printed_page'),'target_physical_page':x.get('target_physical_page'),'case':x.get('case')} for x in e['localized_representations']]
        require(actual==m['amendment_map'],'mixed_amendment_association_changed')
    return e

def representations(e):
    result=[]
    for p in e['pages']:
        require(hashlib.sha256(p['raw_ocr_text'].encode()).hexdigest()==p['raw_ocr_sha256'],'mixed_raw_ocr_hash_changed')
        require(type(p['include_raw_ocr']) is bool and bool(p['selection_reason']) and p['approval_class'] in ('numbered_grey_approval_scope','white_nonapproved_context','historical_approved_source_context'),'mixed_page_role_required')
        if p['include_raw_ocr']:
            result.append({'representation_id':'raw-page-'+str(p['page_number']),'page_number':p['page_number'],'text':p['raw_ocr_text'],'text_sha256':p['raw_ocr_sha256'],'method':METHODS[0],'approval_class':p['approval_class'],'coverage':'partial_raw_ocr_not_fullpage_certified','confidence':None,'confidence_status':'not_recorded','source_provenance':e['raw_ocr_provenance']})
        last_end=0
        for j,span in enumerate(p.get('raw_ocr_spans',[])):
            start,end=span.get('start'),span.get('end')
            require(not p['include_raw_ocr'] and type(start) is int and type(end) is int and 0<=start<end<=len(p['raw_ocr_text']), 'mixed_span_bounds_invalid')
            require(start>=last_end,'mixed_span_overlap_or_reorder')
            last_end=end
            text=p['raw_ocr_text'][start:end]
            require('text' not in span or span['text']==text,'mixed_span_text_changed')
            require(hashlib.sha256(text.encode()).hexdigest()==span.get('text_sha256') and span.get('method')==SPAN_METHOD and bool(span.get('scope')), 'mixed_span_identity_changed')
            result.append({'representation_id':'raw-span-'+str(p['page_number'])+'-'+str(j),'page_number':p['page_number'],'text':text,'text_sha256':span['text_sha256'],'method':SPAN_METHOD,'approval_class':p['approval_class'],'coverage':'selected_raw_ocr_span_only_not_fullpage_certified','confidence':None,'confidence_status':'not_recorded','source_provenance':dict(e['raw_ocr_provenance'],original_raw_ocr_sha256=p['raw_ocr_sha256'],exact_start=start,exact_end=end,span_scope=span['scope'])})
    transcripts={x['sha256']:x['text'] for x in e.get('full_transcripts',[])}
    require(len(transcripts)==len(e.get('full_transcripts',[])),'mixed_duplicate_full_transcript')
    transcript_intervals={sha:[] for sha in transcripts}
    amendment_numbers=set()
    for sha,text in transcripts.items():
        require(hashlib.sha256(text.encode()).hexdigest()==sha,'mixed_full_transcript_hash_changed')
    for i,p in enumerate(e['localized_representations']):
        if 'full_transcript_sha256' in p:
            full=transcripts.get(p['full_transcript_sha256']);start=p.get('transcript_start');end=p.get('transcript_end')
            require(full is not None and type(start) is int and type(end) is int and 0<=start<end<=len(full),'mixed_transcript_span_bounds_invalid')
            require(p['text']==full[start:end],'mixed_transcript_span_text_changed')
            require(all(end<=a or start>=b for a,b in transcript_intervals[p['full_transcript_sha256']]),'mixed_transcript_overlap_or_duplicate')
            transcript_intervals[p['full_transcript_sha256']].append((start,end))
            number=p.get('amendment_number')
            require(type(number) is int and number not in amendment_numbers,'mixed_amendment_number_duplicate_or_missing')
            amendment_numbers.add(number)
        require(hashlib.sha256(p['text'].encode()).hexdigest()==p['text_sha256'],'mixed_passage_hash_changed')
        require(p['representation'] in METHODS[1:],'mixed_passage_method_required')
        if p['decision']=='accepted_exact_visible_words':
            result.append({'representation_id':'localized-'+str(i),'page_number':p['physical_page'],'text':p['text'],'text_sha256':p['text_sha256'],'method':p['representation'],'approval_class':e['pages'][p['physical_page']-1]['approval_class'],'coverage':'independently_verified_localized_passage_only','confidence':None,'confidence_status':'not_calibrated_or_invented','source_provenance':{k:v for k,v in p.items() if k!='text'}})
        else:require(p['decision']=='held_residual_recognition_errors','mixed_unknown_passage_decision')
    for sha,intervals in transcript_intervals.items():
        ordered=sorted(intervals)
        require(bool(ordered) and ordered[0][0]==0 and ordered[-1][1]==len(transcripts[sha]) and all(a[1]==b[0] for a,b in zip(ordered,ordered[1:])),'mixed_transcript_coverage_incomplete')
    require(len({x['representation_id'] for x in result})==len(result),'mixed_duplicate_representation')
    return result

def expected_pages(r):
    e=evidence(r);reps=representations(e);pages=[]
    for p in e['pages']:
        selected=[x for x in reps if x['page_number']==p['page_number']]
        pages.append({'page_number':p['page_number'],'text':'','method':'reviewed_mixed_representations','status':'partial_mixed_search' if selected else 'mixed_withheld','width':p['width'],'height':p['height'],'representations':selected,'original_pdf_text_sha256':p['original_pdf_text_sha256'],'original_pdf_char_count':p['original_pdf_char_count'],'raw_ocr_sha256':p['raw_ocr_sha256'],'raw_ocr_char_count':len(p['raw_ocr_text']),'approval_class':p['approval_class'],'selection_reason':p['selection_reason']})
    return pages

def load_pages(pdf,r):
    pages=expected_pages(r)
    for original,p in zip(pdf,pages):
        t=original.get_text('text');require(hashlib.sha256(t.encode()).hexdigest()==p['original_pdf_text_sha256'] and len(t)==p['original_pdf_char_count'] and original.rect.width==p['width'] and original.rect.height==p['height'],'mixed_original_pdf_layer_changed')
    return pages

def assemble(r,pages,byte_count):
    from vd_pdcom_text import validate_review,uid,digest,source_kind
    validate_review(r);require(pages==expected_pages(r),'mixed_page_or_representation_changed')
    url=r['source_url'];sha=r['source_sha256'];doc=uid('knowledge:'+url+':'+sha)
    provenance={'parser':source_kind(r),'vd_document_id':r['vd_document_id'],'content_hash':sha,'commune_bfs':r['commune_bfs'],'scope':'municipal','approval_scope':r['signed_approval'],'all_prose_is_binding':False,'diagnostic_vintage_limit':r['diagnostic_vintage_limit'],'limits':r['limits'],'spatial_qualification':False,'commune_complete':False,'source_role':r['source_role'],'plan_status':r['plan_status'],'review_sha256':digest(r),'mixed_representations':r['mixed_representations']}
    chunks=[]
    for p in pages:
        for rep in p['representations']:
            proof={k:v for k,v in rep.items() if k!='text'}
            for start in range(0,len(rep['text']),2000):
                content=rep['text'][start:start+2000].strip()
                if content:
                    i=len(chunks);chunks.append({'id':uid(doc+':'+str(i)),'document_id':doc,'chunk_index':i,'content':content,'page_number':p['page_number'],'metadata':dict(provenance,source_url=url,citation=url+'#page='+str(p['page_number']),extraction_status='partial_mixed_representation',representation=proof)})
    require(bool(chunks),'mixed_no_searchable_text')
    metadata=dict(provenance,source_url=url,physical_page_count=len(pages),page_manifest=[{k:v for k,v in p.items() if k not in ('text','representations')} for p in pages],extraction_status='partial_mixed_representations',map_pages=r['map_pages'],full_visual_map_extraction=False)
    return {'review':r,'pages':pages,'byte_count':byte_count,'chunks':chunks,'source_id':uid(source_kind(r)+':VD:'+r['vd_document_id']),'version_id':uid('version:'+url+':'+sha),'document_id':doc,'metadata':metadata}

def validate_delivery(document,chunks):
    r=dict(document,page_count=document['physical_page_count'],source_sha256=document['content_hash']);validate_review(r)
    reps={x['representation_id']:x for x in representations(evidence(r))}
    require(bool(chunks),'mixed_delivery_chunks_required')
    from vd_pdcom_text import uid
    doc=uid('knowledge:'+document['source_url']+':'+document['content_hash'])
    expected=[]
    for rep in sorted(reps.values(),key=lambda x:x['page_number']):
        for n in range(0,len(rep['text']),2000):
            t=rep['text'][n:n+2000].strip()
            if t:
                i=len(expected);expected.append((uid(doc+':'+str(i)),i,rep['page_number'],t,rep['representation_id']))
    actual=[(x['id'],x['chunk_index'],x['page_number'],x['content'],x['metadata'].get('representation',{}).get('representation_id')) for x in sorted(chunks,key=lambda x:x['chunk_index'])]
    require(actual==expected,'mixed_delivery_sequence_incomplete_or_changed')
    for chunk in chunks:
        m=chunk['metadata'];proof=m.get('representation',{});rep=reps.get(proof.get('representation_id'))
        require(rep is not None and proof=={k:v for k,v in rep.items() if k!='text'},'mixed_delivery_representation_changed')
        require(chunk['page_number']==rep['page_number'] and chunk['content'] in [rep['text'][n:n+2000].strip() for n in range(0,len(rep['text']),2000)],'mixed_delivery_text_changed')
        require(all(m.get(k)==document.get(k) for k in ('mixed_representations','approval_scope','diagnostic_vintage_limit','limits')),'mixed_delivery_caveat_changed')
