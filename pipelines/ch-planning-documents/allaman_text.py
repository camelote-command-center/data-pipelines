"""Exact reviewed status transition for the Allaman municipal original.

No commit; caller owns the source review and text delivery transaction together.
Only plan_status may change. Source chronology and extracted evidence are retained.
"""
import json
from pathlib import Path
from psycopg2.extras import Json
from vd_pdcom_text import require, validate_review, digest

ROOT=Path(__file__).parent/'reports/allaman-text'
DOCUMENT='904b343d-c730-5541-adbd-96150c95a4a2'
SHA='f8c89a783271df54831de5fb3800c5bb51bd06610203d10be1c4aec2be463261'

def promote_source(conn, review):
    validate_review(review)
    require(review['vd_document_id']==DOCUMENT and review['source_sha256']==SHA and review['page_count']==99 and review['plan_status']=='approved', 'allaman_review_identity_changed')
    with conn.cursor() as q:
        q.execute('SELECT to_jsonb(d) FROM bronze_ch.vd_pdcom_documents d WHERE id=%s FOR UPDATE', (DOCUMENT,))
        row=q.fetchone()
        require(row is not None, 'allaman_source_missing')
        before=row[0]
        require((before['sha256'],before['page_count'],before['source_url'])==(SHA,99,review['source_url']), 'allaman_registered_identity_changed')
        require(before['plan_status'] in ('unverified','approved'), 'allaman_unexpected_prior_status')
        after=dict(before,plan_status='approved')
        if before!=after:
            q.execute("UPDATE bronze_ch.vd_pdcom_documents SET plan_status='approved' WHERE id=%s AND to_jsonb(vd_pdcom_documents)=%s",(DOCUMENT,Json(before)))
            require(q.rowcount==1, 'allaman_status_compare_update_failed')
        q.execute('SELECT to_jsonb(d) FROM bronze_ch.vd_pdcom_documents d WHERE id=%s',(DOCUMENT,))
        require(q.fetchone()[0]==after, 'allaman_source_other_fields_changed')
    return {'document_id':DOCUMENT,'before_sha256':digest(before),'after_sha256':digest(after),'from_status':before['plan_status'],'to_status':after['plan_status'],'changed':before!=after,'only_plan_status_changed':True}
