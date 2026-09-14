"""Replay reviewed source selections as spatial CANDIDATES, never a release."""
import json
from pathlib import Path
import uuid
from psycopg2.extras import Json
# Keep the exact full digest, checked against the committed calibration.
SHA='bd095d80d2284073a93ef7e108f89b083d32fb361c97b689809522b7b8f64c86'
def persist(conn,document_id,sha):
    if sha!=SHA or str(document_id)!='8f42da46-9b59-5b72-aa5b-13522e418682':return None
    root=Path(__file__).parent/'reports'/'epalinges'/'alignment'
    a=json.loads((root/'alignment.json').read_text())
    features=json.loads((root/'medium-density-lv95.geojson').read_text())['features']
    evidence={**a,'calibration_date':'2026-09-14','validation_status':'review_required',
              'visual_review':'North/centre/south RCB building-location overlays reviewed; broad alignment consistent. Not independently surveyed accuracy.',
              'semantic_limit':'Six medium-density indicative source fills only. Other categories and overprinted measures are not extracted.',
              'reservation':'Cantonal reservation on residential/mixed-zone resizing principles, chapter2.2/pages21-22.',
              'source_precision':'unknown','boundary_buffer':'10m review heuristic, not proven error bound'}
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        for f in features:
            sid=str(uuid.uuid5(uuid.NAMESPACE_URL,str(document_id)+'#page99-extdrawing='+str(f['properties']['extended_drawing_index'])+'#calibration=2026-09-14'))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,99,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT(id) DO NOTHING''',
            (sid,document_id,f['properties']['label'],json.dumps(f['geometry']),a['withheld_boundary_rmse_m'],Json(evidence)))
        c.execute('''WITH p AS MATERIALIZED (
        SELECT egrid,ST_Transform(ST_MakeValid(geometry),2056) g FROM silver_ch.cadastral_plots WHERE commune_bfs=5584 AND geometry IS NOT NULL),
        s AS MATERIALIZED (SELECT id,ST_Transform(geom,2056) g FROM bronze_ch.vd_pdcom_sectors WHERE document_id=%s),
        pairs AS (SELECT s.id,p.egrid,ST_Intersection(s.g,p.g) overlap,ST_Area(p.g) area,ST_DWithin(p.g,ST_Boundary(s.g),10) edge FROM s JOIN p ON ST_Intersects(s.g,p.g))
        INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom)
        SELECT id,egrid,ST_Area(overlap),area,LEAST(1,ST_Area(overlap)/area),edge,10,ST_Transform(overlap,4326) FROM pairs
        WHERE ST_Area(overlap)>0 AND area>0 AND egrid IS NOT NULL ON CONFLICT(sector_id,egrid) DO NOTHING''',(document_id,))
        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Partial indicative density category; independent spatial QA and remaining categories pending' WHERE commune_bfs=5584 AND extraction_status IN ('pending','downloaded','needs_ocr')")
        c.execute('''SELECT count(*),count(distinct p.egrid) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN bronze_ch.vd_pdcom_sectors s ON s.id=p.sector_id WHERE s.document_id=%s''',(document_id,));pairs,parcels=c.fetchone()
    return {'sectors':6,'parcel_pairs':pairs,'distinct_parcels':parcels,'status':'review_required'}
