"""Reconcile acquired-source summaries with verified partial private extraction.

Caller owns the transaction. This never advances delivery or qualification.
Each source hash has one bounded reconciliation receipt with frozen sector IDs;
this is not a cumulative projection. Later private batches must not silently
expand or overwrite that receipt.
"""
import json
from psycopg2.extras import Json,RealDictCursor


def projection(current,document_id,sha,sector_ids,approval_review):
    if current['delivery_status']!='not_ready':raise ValueError('progress_delivery_not_ready_required')
    if current['extraction_status'] not in ('downloaded','candidate_vectors'):raise ValueError('progress_existing_extraction_not_reconcilable')
    if not isinstance(approval_review,dict) or approval_review.get('source_sha256')!=sha or approval_review.get('document_id')!=document_id or approval_review.get('status') not in ('approved','approved_with_reservation') or not approval_review.get('evidence_uri') or not approval_review.get('scope') or approval_review.get('currentness_and_completeness_resolved') is not False:raise ValueError('progress_approval_review_required')
    if not sector_ids or len(sector_ids)!=len(set(sector_ids)):raise ValueError('progress_unique_private_sectors_required')
    evidence=current['evidence']
    if not isinstance(evidence,list) or not any(x.get('document_id')==document_id for x in evidence if isinstance(x,dict)):raise ValueError('progress_acquired_source_evidence_required')
    event={'kind':'partial_private_extraction_reconciliation','document_id':document_id,'source_sha256':sha,'private_sector_ids':sorted(sector_ids),'exact_version_approval_review':approval_review,'publication_authorized':False,'commune_complete':False}
    prior=[x for x in evidence if isinstance(x,dict) and x.get('kind')==event['kind'] and x.get('document_id')==document_id and x.get('source_sha256')==sha]
    if prior and prior!=[event]:raise ValueError('progress_existing_reconciliation_conflict')
    if prior:
        if current['extraction_status']!='candidate_vectors':raise ValueError('progress_state_evidence_conflict')
        return None
    note='Verified partial private cartographic extraction; exact signed version approval reviewed separately. Thematic precision, currentness reconciliation, remaining map inventory and parcel release remain unresolved. No commune completion.'
    return {'extraction_status':'candidate_vectors','evidence':evidence+[event],'blocker':current.get('blocker') or note,'review_note':(current.get('review_note')+'\n' if current.get('review_note') else '')+note}


def reconcile(conn,bfs,document_id,sha,sector_ids,approval_review):
    with conn.cursor(cursor_factory=RealDictCursor) as c:
        c.execute('SELECT * FROM bronze_ch.vd_pdcom_communes WHERE commune_bfs=%s FOR UPDATE',(bfs,));current=c.fetchone()
        if not current or not current['is_current']:raise ValueError('progress_current_commune_required')
        c.execute('''SELECT d.sha256,d.plan_status FROM bronze_ch.vd_pdcom_documents d JOIN bronze_ch.vd_pdcom_document_communes dc ON dc.document_id=d.id WHERE d.id=%s AND dc.commune_bfs=%s FOR SHARE OF d''',(document_id,bfs));source=c.fetchone()
        if not source or source['sha256']!=sha or source['plan_status']!=approval_review.get('status'):raise ValueError('progress_live_approved_source_mismatch')
        c.execute('''SELECT id::text,review_status,source_precision_m,validated_by FROM bronze_ch.vd_pdcom_sectors WHERE document_id=%s AND id=ANY(%s::uuid[]) FOR SHARE''',(document_id,sector_ids));rows=c.fetchall()
        if {x['id']for x in rows}!=set(sector_ids) or any(x['review_status']!='review_required' or x['source_precision_m'] is not None or x['validated_by'] is not None for x in rows):raise ValueError('progress_verified_private_sector_scope_required')
        c.execute('''SELECT count(*) FROM gold_ch.v_vd_pdcom_review_sectors s LEFT JOIN lamap_db_foreign.vd_pdcom_review_sectors r ON r.id=s.id WHERE s.id=ANY(%s::uuid[]) AND to_jsonb(s)=to_jsonb(r)''',(sector_ids,))
        if c.fetchone()['count']!=len(sector_ids):raise ValueError('progress_private_receiver_parity_required')
        patch=projection(current,document_id,sha,sector_ids,approval_review)
        if patch:
            c.execute('''UPDATE bronze_ch.vd_pdcom_communes SET extraction_status=%s,evidence=%s,blocker=%s,review_note=%s,last_checked_at=now() WHERE commune_bfs=%s''',(patch['extraction_status'],Json(patch['evidence']),patch['blocker'],patch['review_note'],bfs))
        c.execute('SELECT extraction_status,delivery_status,resolved FROM bronze_ch.vd_pdcom_coverage WHERE commune_bfs=%s',(bfs,));result=dict(c.fetchone())
        if result!={'extraction_status':'candidate_vectors','delivery_status':'not_ready','resolved':False}:raise ValueError('progress_unexpected_promotion')
        return dict(result,changed=patch is not None)
