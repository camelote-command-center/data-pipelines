"""Exact Saint-Légier historical scope; never promotes registry approval or territory."""
import hashlib
import json
SEMANTIC_SHA='b32c72e9d1281c44fb5f118fd2727bc12e93c5cac4cbe2198df5510dffc22f4c'
DOCUMENT='ecaca53c-511b-5af4-94b2-7925852fc740'
SHA='86b3c0352167167623ab93ebc0aeafd2957e14c0797867a3e982910e65b227eb'
SOURCE='vd_pdcom_historical_former_commune'
LABEL='Référence historique — ancienne commune de Saint-Légier-La Chiésaz (OFS5888); validité actuelle et intégration des amendements non établies.'

def require(ok,reason):
    if not ok:raise ValueError(reason)

def validate(r):
    if 'content_hash' in r:
        require(r.get('parser')==SOURCE,'historical_parser_required')
    require(r.get('vd_document_id')==DOCUMENT and r.get('source_sha256',r.get('content_hash'))==SHA and r.get('page_count',r.get('physical_page_count'))==75,'historical_exact_source_required')
    require(r.get('scope')=='historical_former_commune' and r.get('commune_bfs') is None,'historical_no_active_commune_required')
    require(r.get('plan_status')=='unverified' and r.get('source_role')=='historical_former_commune_reference','historical_registry_status_must_remain_unverified')
    h=r.get('historical_scope',{})
    require(h.get('former_commune_bfs')==5888 and type(h.get('former_commune_bfs')) is int and h.get('registry_tracking_bfs')==5892 and type(h.get('registry_tracking_bfs')) is int,'historical_territory_identity_required')
    require(h.get('registry_coverage_extent')=='unverified' and h.get('tracking_is_not_coverage') is True and h.get('current_municipal_reference') is False and h.get('current_merged_commune_coverage') is False,'historical_no_current_scope_promotion')
    require(h.get('source_status_promotion') is False and h.get('spatial_currentness_block_retained') is True,'historical_no_source_or_spatial_promotion')
    require(h.get('amendment_incorporation')=='unknown_missing_separate_list' and h.get('separate_annex_cahier_adopted') is False,'historical_missing_context_required')
    require(h.get('human_readable_caveat')==LABEL,'historical_readable_caveat_required')
    require(bool(h.get('page_specific_limits')) and bool(r.get('diagnostic_vintage_limit')) and bool(r.get('limits')),'historical_page_limits_required')

    semantic={'historical_scope':h,'signed_approval':r.get('signed_approval',r.get('approval_scope')),'diagnostic_vintage_limit':r.get('diagnostic_vintage_limit'),'limits':r.get('limits')}
    require(hashlib.sha256(json.dumps(semantic,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()==SEMANTIC_SHA,'historical_reviewed_semantics_changed')

def validate_registered_mapping(cursor):
    cursor.execute('SELECT commune_bfs,coverage_extent FROM bronze_ch.vd_pdcom_document_communes WHERE document_id=%s ORDER BY commune_bfs',(DOCUMENT,))
    require(cursor.fetchall()==[(5892,'unverified')],'historical_tracking_relation_changed')

def validate_chunk(document,metadata):
    validate(document)
    validate(dict(metadata,physical_page_count=document['physical_page_count']))
    require(all(metadata.get(k)==document.get(k) for k in ('parser','historical_scope','scope','commune_bfs','source_role','plan_status','diagnostic_vintage_limit','limits')),'historical_chunk_scope_or_caveat_changed')
