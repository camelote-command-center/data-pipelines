"""Opt-in PostgreSQL regression checks; only session-local temporary tables."""
import json,os,sys,unittest,uuid
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from parser import persist_pilot_source

@unittest.skipUnless(os.getenv('VD_PDCOM_TEST_DB_URL'),'requires PostgreSQL integration route')
class PilotRefreshPersistence(unittest.TestCase):
    def test_existing_reviews_survive_and_changed_bytes_stay_separate(self):
        conn=psycopg2.connect(os.environ['VD_PDCOM_TEST_DB_URL'],connect_timeout=15)
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT 1');self.assertEqual(cursor.fetchone(),(1,))
                cursor.execute('CREATE TEMP TABLE vd_pdcom_documents (id uuid PRIMARY KEY,source_url text,landing_url text,sha256 text,title text,plan_status text,scope text,status_evidence_url text,page_count integer,inspection jsonb,UNIQUE(source_url,sha256)) ON COMMIT DROP')
                cursor.execute('CREATE TEMP TABLE vd_pdcom_document_communes(document_id uuid,commune_bfs integer,evidence_url text,PRIMARY KEY(document_id,commune_bfs)) ON COMMIT DROP')
                cursor.execute('CREATE TEMP TABLE vd_pdcom_communes(commune_bfs integer PRIMARY KEY,discovery_status text,extraction_status text,delivery_status text,last_checked_at timestamptz,evidence jsonb,blocker text) ON COMMIT DROP')
                class TempCursor:
                    def execute(self,sql,params=None):
                        # Run the production statements against isolated tables only.
                        for table in ['vd_pdcom_documents','vd_pdcom_document_communes','vd_pdcom_communes']:
                            sql=sql.replace('bronze_ch.'+table,'pg_temp.'+table)
                        cursor.execute(sql,params)
                c=TempCursor();source={'commune_bfs':1,'pdf_url':'https://example.org/plan.pdf','landing_url':'https://example.org','title':'Plan','plan_status':'approved','scope':'communal','status_evidence_url':'https://example.org'}
                old=str(uuid.uuid4());new=str(uuid.uuid4());raw=[{'page_number':1,'text':'raw'}]
                evidence=[{'document_id':old},{'operation_id':'review','finding':'unresolved reservation'}]
                reviewed=[{'page_number':1,'text':'raw','ocr_research':{'text':'reviewed OCR'},'source_review':{'reservation':'retain'}}]
                cursor.execute("INSERT INTO pg_temp.vd_pdcom_communes VALUES(1,'sources_found','validated','verified',now(),%s,'specific review limitation')",(Json(evidence),))
                persist_pilot_source(c,source,old,'oldsha',reviewed,{'paths':[1]})
                for _ in range(2):persist_pilot_source(c,source,old,'oldsha',raw,{'paths':[]})
                cursor.execute('SELECT inspection FROM pg_temp.vd_pdcom_documents WHERE id=%s',(old,));self.assertEqual(cursor.fetchone()[0],reviewed)
                cursor.execute('SELECT evidence,extraction_status,delivery_status,blocker FROM pg_temp.vd_pdcom_communes');self.assertEqual(cursor.fetchone(),(evidence,'validated','verified','specific review limitation'))
                persist_pilot_source(c,source,new,'newsha',raw,{'paths':[]})
                cursor.execute('SELECT inspection FROM pg_temp.vd_pdcom_documents WHERE id=%s',(old,));self.assertEqual(cursor.fetchone()[0],reviewed)
                cursor.execute('SELECT inspection FROM pg_temp.vd_pdcom_documents WHERE id=%s',(new,));self.assertEqual(cursor.fetchone()[0],raw)
                cursor.execute('SELECT evidence,extraction_status,delivery_status FROM pg_temp.vd_pdcom_communes');got=cursor.fetchone();self.assertEqual(got[0][:2],evidence);self.assertEqual(got[0][2]['document_id'],new);self.assertEqual(got[1:],('downloaded','not_ready'))
                persist_pilot_source(c,source,new,'newsha',raw,{'paths':[]})
                cursor.execute('SELECT jsonb_array_length(evidence) FROM pg_temp.vd_pdcom_communes');self.assertEqual(cursor.fetchone()[0],3)
                source['commune_bfs']=2;cursor.execute("INSERT INTO pg_temp.vd_pdcom_communes VALUES(2,'not_searched','pending','not_ready',now(),NULL,NULL)")
                persist_pilot_source(c,source,new,'newsha',raw,{'paths':[1]})
                cursor.execute('SELECT extraction_status,delivery_status,jsonb_array_length(evidence) FROM pg_temp.vd_pdcom_communes WHERE commune_bfs=2');self.assertEqual(cursor.fetchone(),('candidate_vectors','not_ready',1))
        finally:
            conn.rollback();conn.close()
