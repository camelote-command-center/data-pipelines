"""Replay reviewed source selections as spatial CANDIDATES, never a release."""
import json
from pathlib import Path
import uuid
from psycopg2.extras import Json
# Keep the exact full digest, checked against the committed calibration.
SHA='bbd1d3d0474d5f491ac4cc17b05d7bf1c7912861d9f93f9347c719bf7522fedc'
def persist(conn,document_id,sha):
    if sha!=SHA or str(document_id)!='dd172ee9-e928-5c2f-99d6-3a24eb28cd68':return None
    root=Path(__file__).parent/'reports'/'aigle'/'alignment'
    a=json.loads((root/'alignment.json').read_text())
    features=json.loads((root/'sectors-lv95.geojson').read_text())['features']
    evidence={**a,'calibration_date':'2026-09-14','validation_status':'review_required',
              'visual_review':'North/centre/south RCB location-point overlays reviewed; alignment broadly consistent. Fourteen holdouts exceed10m and remain unresolved. Not surveyed accuracy.',
              'semantic_limit':'Nine Densifier and two Préparer la densification source polygons. Indicative actions, not numeric density assignments or parcel rights. Other categories and inset excluded.',
              'approval_limit':'Source final council/canton approval fields blank; consultation version only.',
              'source_precision':'unknown','boundary_buffer':'10m review heuristic, not proven error bound'}
    evidence['followup_qa']=json.loads((root.parent/'qa'/'qa.json').read_text())
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        sector_ids=[str(uuid.uuid5(uuid.NAMESPACE_URL,str(document_id)+'#page19-extdrawing='+str(f['properties']['extended_drawing_index'])+'#calibration=2026-09-14')) for f in features]
        for f in features:
            sid=str(uuid.uuid5(uuid.NAMESPACE_URL,str(document_id)+'#page19-extdrawing='+str(f['properties']['extended_drawing_index'])+'#calibration=2026-09-14'))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,19,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT(id) DO NOTHING''',
            (sid,document_id,f['properties']['label'],json.dumps(f['geometry']),a['holdout_rmse_m'],Json(evidence)))
        c.execute('''WITH p AS MATERIALIZED (
        SELECT egrid,ST_Transform(ST_MakeValid(geometry),2056) g FROM silver_ch.cadastral_plots WHERE commune_bfs=5401 AND geometry IS NOT NULL),
        s AS MATERIALIZED (SELECT id,ST_Transform(geom,2056) g FROM bronze_ch.vd_pdcom_sectors WHERE id=ANY(%s::uuid[])),
        pairs AS (SELECT s.id,p.egrid,ST_Intersection(s.g,p.g) overlap,ST_Area(p.g) area,ST_DWithin(p.g,ST_Boundary(s.g),10) edge FROM s JOIN p ON ST_Intersects(s.g,p.g))
        INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom)
        SELECT id,egrid,ST_Area(overlap),area,LEAST(1,ST_Area(overlap)/area),edge,10,ST_Transform(overlap,4326) FROM pairs
        WHERE ST_Area(overlap)>0 AND area>0 AND egrid IS NOT NULL ON CONFLICT(sector_id,egrid) DO NOTHING''',(sector_ids,))
        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Partial indicative action categories; holdout outliers and parcel QA pending' WHERE commune_bfs=5401 AND extraction_status IN ('pending','downloaded','needs_ocr')")
        c.execute('''SELECT count(*),count(distinct p.egrid) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN bronze_ch.vd_pdcom_sectors s ON s.id=p.sector_id WHERE s.id=ANY(%s::uuid[])''',(sector_ids,));pairs,parcels=c.fetchone()
    return {'sectors':11,'parcel_pairs':pairs,'distinct_parcels':parcels,'status':'review_required'}
