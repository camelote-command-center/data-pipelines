"""Private Orbe priority-territory candidate from bounded official-reference tiles."""
import json,uuid,math
from pathlib import Path
from psycopg2.extras import Json
from official_references import refresh
from cadastral_types import KINDS
SHA='8b78d42eb83210b6b63404dd8b4270fedd15d1b7e3d65d2a403d6edcee654a3c'
DOC='464c3687-bdd2-55b4-9e35-f8ece05099cb'
SID=str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#page1-main-priority-outer-band#alignment2026-09-15'))
def tiles(bounds):
    if len(bounds)!=4 or not all(math.isfinite(x) for x in bounds):raise ValueError('invalid_tile_bounds')
    x0,y0,x1,y1=bounds
    if not (0<x1-x0<=4000 and 0<y1-y0<=4000):raise ValueError('orbe_extent_requires_review')
    nx=math.ceil((x1-x0)/2000);ny=math.ceil((y1-y0)/2000)
    return [[x0+(x1-x0)*i/nx,y0+(y1-y0)*j/ny,x0+(x1-x0)*(i+1)/nx,y0+(y1-y0)*(j+1)/ny] for i in range(nx) for j in range(ny)]
def persist(conn,document_id,sha):
    if sha!=SHA or str(document_id)!=DOC:return None
    root=Path(__file__).parent/'reports/orbe';qa=json.loads((root/'parcel-qa/parcel-review.json').read_text());a=json.loads((root/'alignment/alignment.json').read_text());features=json.loads((root/'alignment/priority-band-lv95.geojson').read_text())['features'];gs={f['properties']['boundary_variant']:f['geometry'] for f in features}
    if qa['source_sha256']!=SHA or qa['source_document_id']!=DOC:raise ValueError('orbe_artifact_mismatch')
    references=[refresh(conn,DOC,5757,b) for b in tiles(qa['bounds_lv95'])];sids=[r['snapshot_id'] for r in references]
    evidence={'alignment':a,'boundary_choice':'Outer edge of one cartographic band, conservative inclusion for private review; not exact legal boundary. Inner edge sensitivity retained.','outer_only_egrids':qa['outer_only_egrids'],'parcel_scenarios':{k:{a:b for a,b in v.items() if a!='pairs'} for k,v in qa['variants'].items()},'official_references':references,'approval_limit':'Unsigned2021 PDF linked by municipality under2022approval; exact-version approval unverified.','footprint_limit':'14holdout outliers,4centroids outside current footprints and other shape differences unresolved.','source_precision':'unknown','buffer_limit':'10m plus cartographic band review heuristic, not demonstrated error bound','validation_status':'review_required'}
    frozen=json.loads((root/'parcel-qa/official-parcels.geojson').read_text())['features']
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'")
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_references a JOIN bronze_ch.vd_pdcom_parcel_references b ON a.egrid=b.egrid AND a.snapshot_id<b.snapshot_id
        WHERE a.snapshot_id=ANY(%s::uuid[]) AND b.snapshot_id=ANY(%s::uuid[]) AND (NOT ST_Equals(a.geom,b.geom) OR a.official_attributes<>b.official_attributes)''',(sids,sids))
        if c.fetchone()[0]:raise ValueError('orbe_tile_overlap_conflict')
        c.execute('''CREATE TEMP TABLE orbe_current_pairs ON COMMIT DROP AS
        WITH refs AS (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) ORDER BY egrid,snapshot_id),
        g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) outer_g,ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) band)
        SELECT r.*,ST_Intersection(r.geom,g.outer_g) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,g.band,10) edge
        FROM refs r,g WHERE ST_Intersects(r.geom,g.outer_g) AND ST_Area(ST_Intersection(r.geom,g.outer_g))>0''',(sids,json.dumps(gs['outer']),json.dumps(gs['band'])))
        c.execute('SELECT egrid FROM orbe_current_pairs');actual={r[0] for r in c.fetchall()};expected={r['egrid'] for r in qa['variants']['outer']['pairs']}
        if actual!=expected:raise ValueError('orbe_reference_membership_changed_requires_review')
        c.execute('''WITH frozen AS (SELECT x->'properties'->>'EGRID' egrid,ST_SetSRID(ST_GeomFromGeoJSON((x->'geometry')::text),2056) geom FROM jsonb_array_elements(%s::jsonb) x)
        SELECT count(*) FROM orbe_current_pairs n LEFT JOIN frozen f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom)''',(Json(frozen),))
        if c.fetchone()[0]:raise ValueError('orbe_reference_geometry_changed_requires_review')
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
        VALUES(%s,%s,1,'Territoires à densifier prioritairement — bord extérieur indicatif',ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(SID,DOC,json.dumps(gs['outer']),a['holdout_rmse_m'],Json(evidence)))
        c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(SID,))
        if c.fetchone()[0]!=[5757]:raise ValueError('orbe_geographic_attribution_mismatch')
        c.execute('SELECT ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(json.dumps(gs['outer']),SID))
        if not c.fetchone()[0]:raise ValueError('orbe_existing_sector_changed_requires_review')
        c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(SID,));old={r[0] for r in c.fetchall()}
        if old and old!=actual:raise ValueError('orbe_existing_candidate_membership_conflict')
        c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN orbe_current_pairs n USING(egrid)
        WHERE p.sector_id=%s AND NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326))''',(SID,))
        if c.fetchone()[0]:raise ValueError('orbe_existing_candidate_geometry_conflict')
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence)
        SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),
        COALESCE(%s::jsonb->>(n.official_attributes->>'GENRE_TXT'),'unknown'),
        jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched')
        FROM orbe_current_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(SID,Json(KINDS)))
        c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id)
        SELECT %s,egrid,snapshot_id FROM orbe_current_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(SID,))
        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Orbe priority territory private review only; cartographic band, other categories, footprint differences and exact-version approval pending' WHERE commune_bfs=5757")
    return {'sector_id':SID,'sectors':1,'parcel_pairs':len(actual),'reference_snapshots':references,'status':'review_required'}
