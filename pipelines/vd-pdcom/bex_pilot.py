"""Bex PUM.7 private candidate with source-scoped official parcel references."""
import json,uuid
from pathlib import Path
from psycopg2.extras import Json
from official_references import refresh
from cadastral_types import KINDS
SHA='7d2fa9b86cb8bb49757eb935bd0735ff3a1476aebc8de39aed4cd39d905f42d5'
DOC='e9288af6-8ecb-5be2-a07e-0581b0a3b068'
SID=str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#page76-inset809x979#radius2#alignment2026-09-15'))
def persist(conn,document_id,sha):
    if sha!=SHA or str(document_id)!=DOC:return None
    root=Path(__file__).parent/'reports/chablais';v=json.loads((root/'bex-outline/variants.json').read_text());a=json.loads((root/'bex-alignment/alignment.json').read_text());f=v['features'][1]
    if v['source_sha256']!=SHA or f['properties']['closing_radius_pixels']!=2:raise ValueError('bex_artifact_mismatch')
    geometry=json.dumps(f['geometry'])
    with conn,conn.cursor() as c:
        c.execute('''WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) g)
        SELECT ST_XMin(Box2D(g))-15,ST_YMin(Box2D(g))-15,ST_XMax(Box2D(g))+15,ST_YMax(Box2D(g))+15 FROM g''',(geometry,));bounds=list(c.fetchone())
    reference=refresh(conn,DOC,5402,bounds)
    evidence={'alignment':a,'source_outline':{k:v[k] for k in ('source_sha256','selection','limitations')},'source_stated_area_m2':110371,'candidate_area_m2':f['properties']['area_m2'],
      'source_precision':'unknown','source_area_discrepancy':'unresolved; no calibration adjustment to force stated area',
      'approval_limit':'Exact-version approval unverified; PA4 candidate needs comparison with newer PA5. PUM.7 perimeter requires refinement.',
      'source_currency':json.loads((root/'pa5-currency/evidence.json').read_text()),
      'parcel_reference':reference,'parcel_qa':json.loads((root/'bex-outline/official-parcel-qa.json').read_text())['variants'],
      'buffer_limit':'10m review heuristic, not demonstrated positional bound','validation_status':'review_required'}
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='60s'")
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
        VALUES(%s,%s,76,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(SID,DOC,f['properties']['label'],geometry,a['holdout_rmse_m'],Json(evidence)))
        c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(SID,))
        if c.fetchone()[0]!=[5402]:raise ValueError('bex_geographic_attribution_mismatch')
        c.execute('SELECT ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(geometry,SID))
        if not c.fetchone()[0]:raise ValueError('bex_frozen_outline_changed_requires_review')
        c.execute('''CREATE TEMP TABLE bex_current_pairs ON COMMIT DROP AS
        WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) g)
        SELECT r.egrid,ST_Intersection(g.g,r.geom) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.g),10) edge,r.official_attributes
        FROM bronze_ch.vd_pdcom_parcel_references r,g WHERE r.snapshot_id=%s AND ST_Intersects(g.g,r.geom)
        AND ST_Area(ST_Intersection(g.g,r.geom))>0''',(geometry,reference['snapshot_id']))
        c.execute('SELECT count(*) FROM bex_current_pairs');count=c.fetchone()[0]
        if count==0:raise ValueError('empty_bex_parcel_intersections')
        c.execute('SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(SID,));existing=c.fetchone()[0]
        if existing:
            # Annual fresh reference snapshots may not silently rewrite reviewed geometry.
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p
            FULL JOIN bex_current_pairs n ON p.egrid=n.egrid AND p.sector_id=%s
            LEFT JOIN bronze_ch.vd_pdcom_candidate_parcel_references link ON link.sector_id=p.sector_id AND link.egrid=p.egrid
            LEFT JOIN bronze_ch.vd_pdcom_parcel_references old ON old.snapshot_id=link.snapshot_id AND old.egrid=p.egrid
            LEFT JOIN bronze_ch.vd_pdcom_parcel_references fresh ON fresh.snapshot_id=%s AND fresh.egrid=n.egrid
            WHERE (p.sector_id=%s OR p.sector_id IS NULL) AND (p.egrid IS NULL OR n.egrid IS NULL OR old.geom IS NULL OR NOT ST_Equals(old.geom,fresh.geom))''',(SID,reference['snapshot_id'],SID))
            if c.fetchone()[0]:raise ValueError('bex_reference_geometry_changed_requires_review')
        c.execute('SELECT egrid,official_attributes FROM bex_current_pairs')
        attrs=dict(c.fetchall())
        for egrid,properties in attrs.items():
            proof={'source_url':reference['query_url'],'response_sha256':reference['response_sha256'],'reference_snapshot_ids':[reference['snapshot_id']],'official_attributes':properties,'status':'matched' if properties.get('GENRE_TXT') in KINDS else 'unrecognized_kind'}
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence)
            SELECT %s,egrid,ST_Area(overlap),area,LEAST(1,ST_Area(overlap)/area),edge,10,ST_Transform(overlap,4326),%s,%s FROM bex_current_pairs WHERE egrid=%s
            ON CONFLICT(sector_id,egrid) DO NOTHING''',(SID,KINDS.get(properties.get('GENRE_TXT'),'unknown'),Json(proof),egrid))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id) VALUES(%s,%s,%s) ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(SID,egrid,reference['snapshot_id']))
        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Bex PA4 PUM.7 private candidate; newer PA5 comparison, exact-version approval, area discrepancy and parcel boundary review pending' WHERE commune_bfs=5402")
    return {'sectors':1,'parcel_pairs':count,'status':'review_required','reference_snapshot_id':reference['snapshot_id']}
