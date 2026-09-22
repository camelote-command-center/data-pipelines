"""Opt-in real PostGIS test; temporary fixtures and real view DDL are rolled back."""
import uuid
import os,json,psycopg2
from pathlib import Path
sql=(Path(__file__).resolve().parents[1]/'sql/rellm_native_review_attribution.sql').read_text()
c=psycopg2.connect(os.environ['RE_LLM_DB_URL'],connect_timeout=15)
try:
 with c.cursor() as q:
  q.execute("SET LOCAL statement_timeout='60s'")
  for table in ['vd_pdcom_sectors','vd_pdcom_documents','vd_pdcom_document_communes','vd_pdcom_communes','swiss_communes_geo']:
   q.execute(f'CREATE TEMP TABLE {table} AS SELECT * FROM bronze_ch.{table} LIMIT 0')
  q.execute("INSERT INTO pg_temp.vd_pdcom_communes(commune_bfs,is_current) VALUES(1,true),(2,true)")
  q.execute("INSERT INTO pg_temp.swiss_communes_geo(bfs_nummer,geometry) VALUES(1,ST_Multi(ST_MakeEnvelope(2530000,1150000,2531000,1151000,2056))),(2,ST_Multi(ST_MakeEnvelope(2531000,1150000,2532000,1151000,2056)))")
  doc=str(uuid.uuid4());q.execute("INSERT INTO pg_temp.vd_pdcom_documents(id,sha256,source_url) VALUES(%s,%s,'https://example.invalid/test')",(doc,'a'*64));q.execute('INSERT INTO pg_temp.vd_pdcom_document_communes(document_id,commune_bfs) VALUES(%s,1)',(doc,))
  # Use actual transformed shared vertices/edges, avoiding coordinate-roundtrip artifacts.
  q.execute("CREATE TEMP TABLE shapes AS SELECT ST_GeometryN(ST_Transform(geometry,4326),1) g FROM pg_temp.swiss_communes_geo WHERE bfs_nummer=1")
  q.execute("CREATE TEMP TABLE cases(name text,geom geometry,kind text,expected integer[],boundary boolean)")
  q.execute("""INSERT INTO cases SELECT 'interior_point',ST_PointOnSurface(g),'source_native_point',ARRAY[1],false FROM shapes
   UNION ALL SELECT 'boundary_vertex',ST_PointN(ST_ExteriorRing(g),3),'source_native_point',ARRAY[1,2],true FROM shapes
   UNION ALL SELECT 'boundary_line',ST_MakeLine(ST_PointN(ST_ExteriorRing(g),3),ST_PointN(ST_ExteriorRing(g),4)),'source_native_line',ARRAY[1,2],true FROM shapes
   UNION ALL SELECT 'interior_line',ST_MakeLine(ST_PointOnSurface(g),ST_LineInterpolatePoint(ST_MakeLine(ST_PointOnSurface(g),ST_PointN(ST_ExteriorRing(g),1)),.2)),'source_native_line',ARRAY[1],false FROM shapes
   UNION ALL SELECT 'wrong_kind',ST_PointOnSurface(g),'source_native_line',ARRAY[]::integer[],false FROM shapes""")
  # Determine shared boundary explicitly (ring orientation can vary).
  q.execute("UPDATE cases SET geom=(SELECT ST_Intersection(ST_Transform(a.geometry,4326),ST_Transform(b.geometry,4326)) FROM pg_temp.swiss_communes_geo a JOIN pg_temp.swiss_communes_geo b ON a.bfs_nummer=1 AND b.bfs_nummer=2) WHERE name='boundary_line'")
  q.execute("UPDATE cases SET geom=(SELECT ST_StartPoint(geom) FROM cases WHERE name='boundary_line') WHERE name='boundary_vertex'")
  q.execute("INSERT INTO cases SELECT 'crossing_line',ST_MakeLine(ST_PointOnSurface(ST_Transform(a.geometry,4326)),ST_PointOnSurface(ST_Transform(b.geometry,4326))),'source_native_line',ARRAY[1,2],true FROM pg_temp.swiss_communes_geo a JOIN pg_temp.swiss_communes_geo b ON a.bfs_nummer=1 AND b.bfs_nummer=2")
  q.execute("INSERT INTO pg_temp.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status) SELECT gen_random_uuid(),%s,1,name,geom,1,jsonb_build_object('feature_kind',kind),'review_required' FROM cases",(doc,))
  testsql=sql.replace('gold_ch.v_vd_pdcom_review_sectors','pg_temp.native_review').replace('bronze_ch.','pg_temp.').replace('CREATE OR REPLACE VIEW','CREATE TEMP VIEW')
  q.execute(testsql)
  q.execute("SELECT v.label,v.commune_bfs,k.expected,(v.validation_evidence->'native_attribution'->>'boundary_ambiguity')::boolean,k.boundary FROM pg_temp.native_review v JOIN cases k ON k.name=v.label")
  rows=q.fetchall();assert all(a==b and c==d for _,a,b,c,d in rows),rows
  # Real existing polygon rows must remain byte-identical under actual view SQL.
  q.execute('SELECT to_jsonb(s) FROM gold_ch.v_vd_pdcom_review_sectors s');before=q.fetchall()
  q.execute(sql)
  q.execute('SELECT to_jsonb(s) FROM gold_ch.v_vd_pdcom_review_sectors s');after=q.fetchall();norm=lambda rs:sorted(json.dumps(x[0],sort_keys=True) for x in rs);assert norm(before)==norm(after)
 c.rollback();result={'native_cases':rows,'existing_polygon_rows_unchanged':len(before),'all_changes_rolled_back':True};print(json.dumps(result))
finally:c.rollback();c.close()
