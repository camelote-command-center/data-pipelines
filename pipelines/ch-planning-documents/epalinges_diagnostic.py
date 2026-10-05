"""Exact reviewed status transition for the Épalinges diagnostic original.

No commit; caller owns the source review and text delivery transaction together.
Only plan_status may change. Source chronology and extracted evidence are retained.
"""
import json
from pathlib import Path
from psycopg2.extras import Json
from vd_pdcom_text import require, validate_review, digest

ROOT=Path(__file__).parent/'reports/epalinges-diagnostic-text'
DOCUMENT='6967798a-aaf8-5d5b-a897-b40132d28d53'
SHA='4a6640222c279c112853745003f9ab692e804b4713f989cbe4313c0855fd43cc'

def promote_source(conn, review):
    validate_review(review)
    require(review['vd_document_id']==DOCUMENT and review['source_sha256']==SHA and review['page_count']==144 and review['plan_status']=='approved_with_reservation', 'diagnostic_review_identity_changed')
    with conn.cursor() as q:
        q.execute('SELECT to_jsonb(d) FROM bronze_ch.vd_pdcom_documents d WHERE id=%s FOR UPDATE', (DOCUMENT,))
        row=q.fetchone()
        require(row is not None, 'diagnostic_source_missing')
        before=row[0]
        require((before['sha256'],before['page_count'],before['source_url'])==(SHA,144,review['source_url']), 'diagnostic_registered_identity_changed')
        require(before['plan_status'] in ('unverified','approved_with_reservation'), 'diagnostic_unexpected_prior_status')
        after=dict(before,plan_status='approved_with_reservation')
        if before!=after:
            q.execute("UPDATE bronze_ch.vd_pdcom_documents SET plan_status='approved_with_reservation' WHERE id=%s AND to_jsonb(vd_pdcom_documents)=%s",(DOCUMENT,Json(before)))
            require(q.rowcount==1, 'diagnostic_status_compare_update_failed')
        q.execute('SELECT to_jsonb(d) FROM bronze_ch.vd_pdcom_documents d WHERE id=%s',(DOCUMENT,))
        require(q.fetchone()[0]==after, 'diagnostic_source_other_fields_changed')
    return {'document_id':DOCUMENT,'before_sha256':digest(before),'after_sha256':digest(after),'from_status':before['plan_status'],'to_status':after['plan_status'],'changed':before!=after,'only_plan_status_changed':True}
