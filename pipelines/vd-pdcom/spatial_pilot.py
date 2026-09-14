"""Replay the hash-bound Prangins pilot through the Pixxels acquisition workflow.

All outputs remain review_required. The frozen 2026-09-13 calibration is not
relabelled as a new validation when the acquisition workflow runs again.
"""
import json
from pathlib import Path
import uuid
from psycopg2.extras import Json


def persist(conn,document_id,sha):
    if sha!='a4079a88201b716ede0793150102306f8b1faddb55c391a7c2c1696244a96d02':
        return None
    root=Path(__file__).parent/'reports'/'prangins'
    alignment=json.loads((root/'alignment.json').read_text())
    alignment.update(release_status='review_required',calibration_date='2026-09-13',
                     source_precision='unknown',visual_review='Three area overlays plus seven individual holdout overlays reviewed; largest outlier explained by Nyon/reference coverage, six location-point/shape differences remain unresolved')
    features=json.loads((root/'sectors-lv95.geojson').read_text())['features']
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        for f in features:
            sid=str(uuid.uuid5(uuid.NAMESPACE_URL,str(document_id)+'#drawing='+str(f['properties']['drawing_index'])))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors
            (id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,1,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required')
            ON CONFLICT(id) DO NOTHING''',(sid,document_id,f['properties']['label'],json.dumps(f['geometry']),alignment['holdout_rmse_m'],Json(alignment)))
        c.execute('''WITH parcels AS MATERIALIZED (
          SELECT egrid,ST_Transform(ST_MakeValid(geometry),2056) AS g
          FROM silver_ch.cadastral_plots WHERE commune_bfs=5725 AND geometry IS NOT NULL
        ), sectors AS MATERIALIZED (
          SELECT id,ST_Transform(geom,2056) AS g FROM bronze_ch.vd_pdcom_sectors WHERE document_id=%s
        ), pairs AS (
          SELECT s.id,p.egrid,ST_Intersection(s.g,p.g) AS overlap,
                 ST_Area(p.g) AS parcel_area,ST_DWithin(p.g,ST_Boundary(s.g),10) AS boundary_review
          FROM sectors s JOIN parcels p ON ST_Intersects(s.g,p.g)
        ) INSERT INTO bronze_ch.vd_pdcom_parcel_candidates
        (sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom)
        SELECT id,egrid,ST_Area(overlap),parcel_area,LEAST(1,ST_Area(overlap)/parcel_area),boundary_review,10,ST_Transform(overlap,4326)
        FROM pairs WHERE ST_Area(overlap)>0 AND parcel_area>0 AND egrid IS NOT NULL
        ON CONFLICT(sector_id,egrid) DO NOTHING''',(document_id,))
        c.execute('''SELECT count(*),count(DISTINCT p.egrid) FROM bronze_ch.vd_pdcom_parcel_candidates p
                     JOIN bronze_ch.vd_pdcom_sectors s ON s.id=p.sector_id WHERE s.document_id=%s''',(document_id,))
        pairs,parcels=c.fetchone()
    return {'commune_bfs':5725,'sectors':len(features),'sector_parcel_pairs':pairs,
            'unique_parcels':parcels,'review_status':'review_required','calibration_date':'2026-09-13',
            'holdout_median_m':alignment['holdout_median_m'],'holdout_untrimmed_rmse_m':alignment['holdout_rmse_m']}
