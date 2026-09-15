"""Exercise the real view using rolled-back synthetic sectors; no receiver writes."""
import json,os,pathlib,uuid,psycopg2,sys
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[2]))
from review_delivery import validate_attribution
ROOT=pathlib.Path(__file__).resolve().parents[4]
def check(conn):
    results=[]
    try:
        with conn.cursor() as c:
            c.execute("SET LOCAL statement_timeout='30s'; SET LOCAL lock_timeout='5s'")
            validate_attribution(c)
            doc='e9288af6-8ecb-5be2-a07e-0581b0a3b068'
            cases=[('bex_only',[5402],[5402]),('ollon_only',[5409],[5409]),('shared_bex_ollon',[5402,5409],[5402,5409]),('aigle_touching_neighbors',[5401],[5401]),('outside_document_scope',[5725],[])]
            for name,areas,expected in cases:
                sid=str(uuid.uuid4())
                # Commune polygons provide exact boundary-touch and multi-part cases.
                c.execute("""INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
                SELECT %s,%s,76,%s,ST_Transform(ST_UnaryUnion(ST_Collect(geometry)),4326),0,'{"test_only":true}'::jsonb,'review_required'
                FROM bronze_ch.swiss_communes_geo WHERE bfs_nummer=ANY(%s)""",(sid,doc,'ROLLBACK attribution fixture '+name,areas))
                c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,));actual=c.fetchone()[0]
                if actual!=expected:raise AssertionError((name,actual,expected))
                results.append({'case':name,'actual':actual})
            try:validate_attribution(c)
            except ValueError as exc:assert str(exc)=='review_sector_commune_unresolved'
            else:raise AssertionError('unresolved fixture did not block delivery')
            return results
    finally:conn.rollback()
if __name__=='__main__':
    with psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15) as co:print(json.dumps(check(co)))
