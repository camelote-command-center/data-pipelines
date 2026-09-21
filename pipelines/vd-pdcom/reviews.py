"""Apply byte-specific human/model source reviews without releasing geography."""
import json
from pathlib import Path
from psycopg2.extras import Json


def matching_review(document_id, sha):
    reviews=json.loads(Path(__file__).with_name('document_reviews.json').read_text())
    return next((r for r in reviews if r['document_id']==document_id and r['sha256']==sha),None)


def apply_review(conn,document_id,sha):
    review=matching_review(document_id,sha)
    if review is None:return False
    with conn,conn.cursor() as c:
        c.execute('SELECT inspection,page_count FROM bronze_ch.vd_pdcom_documents WHERE id=%s AND sha256=%s FOR UPDATE',(document_id,sha))
        row=c.fetchone()
        if row is None:raise ValueError('review_document_missing')
        inspection,page_count=row
        required=[review['approval_evidence_page']]+[p['page_number'] for p in review['map_pages']]
        if any(p<1 or p>page_count for p in required):raise ValueError('review_page_out_of_range')
        for page in inspection:
            if page.get('page_number')==review['approval_evidence_page']:page['source_review']=review
            for mapped in review['map_pages']:
                if page.get('page_number')==mapped['page_number']:page['reviewed_map']=mapped
        c.execute('UPDATE bronze_ch.vd_pdcom_documents SET plan_status=%s,inspection=%s WHERE id=%s AND sha256=%s',
                  (review['plan_status'],Json(inspection),document_id,sha))
        if c.rowcount!=1:raise ValueError('review_target_changed')
    return True
