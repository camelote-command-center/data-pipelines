"""Private Morges base-hatch candidates; source overprints/currentness stay explicit."""
import json,uuid
from pathlib import Path
from psycopg2.extras import Json
from official_references import refresh
from cadastral_types import KINDS
SHA='9ac3058bc9f837696f69886815f54e65dab848bd67e62df90deb476cebd85300'
DOC='b4deac7f-9349-5463-b2f5-0afdd2bf78e6'
PATHS={5558,5761,7935,8039,8192,8238}
def sector_id(index):
    if index not in PATHS:raise ValueError('morges_unreviewed_path')
    return str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#page1-project-base-'+str(index)+'#alignment2026-09-15'))
def load_artifacts():
    root=Path(__file__).parent/'reports/morges'
    qa=json.loads((root/'parcel-qa/parcel-review.json').read_text());a=json.loads((root/'alignment/alignment.json').read_text());features=json.loads((root/'alignment/full-hatch-lv95.geojson').read_text())['features'];h=json.loads((root/'hatching/semantic-review.json').read_text())
    if qa['source_sha256']!=SHA or qa['source_document_id']!=DOC or len(features)!=6 or {f['properties']['drawing_index'] for f in features}!=PATHS or set(h['full_base_hatch_paths'])!=PATHS:raise ValueError('morges_artifact_mismatch')
    frozen=json.loads((root/'parcel-qa/official-parcels.geojson').read_text())['features']
    return qa,a,features,frozen

def persist(conn,document_id,sha):
    if sha!=SHA or str(document_id)!=DOC:return None
    qa,a,features,frozen=load_artifacts()
    references=[refresh(conn,DOC,5642,qa['bounds_lv95'])];sids=[r['snapshot_id'] for r in references];results=[]
    with conn,conn.cursor() as c:
        c.execute("SET LOCAL statement_timeout='90s'")
        for feature in features:
            name=str(feature['properties']['drawing_index']);sid=sector_id(int(name));geometry=feature['geometry']
            evidence={'alignment':a,'source_geometry_kind':'Underlying full project-hatch base outline; not a complete visible net development perimeter or legal parcel boundary.','source_drawing_index':int(name),'source_precision':'unknown','official_references':references,'parcel_scenarios':{k:{a:b for a,b in v.items() if a!='pairs'} for k,v in qa['sectors'][name].items()},'approval_limit':'Exact map signed10October2012; later land-use currentness unresolved. Mobility chapter replaced2023; no new land-use approval inferred.','footprint_limit':'7holdout differences; two source outlines without current footprint overlap, others partial. Nearest RCB fit is not independent surveyed accuracy.','overprint_limit':'Source overprints/grouping remain part of review. Path8192gray/purple/transport overlays and0.0834m2commune-edge discrepancy retained, not clipped away. Partial hatch paths8261/8641withheld.','outside_commune_m2':feature['properties']['outside_morges_m2'],'buffer_limit':'10m boundary proximity heuristic; ±5/10m scenarios are not accuracy bounds.','validation_status':'review_required','artifact':'pipelines/vd-pdcom/reports/morges/parcel-qa'}
            c.execute('DROP TABLE IF EXISTS pg_temp.morges_current_pairs')
            c.execute('''CREATE TEMP TABLE morges_current_pairs ON COMMIT DROP AS
            WITH refs AS (SELECT DISTINCT ON(egrid) * FROM bronze_ch.vd_pdcom_parcel_references WHERE snapshot_id=ANY(%s::uuid[]) ORDER BY egrid,snapshot_id),
            g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) outer_g)
            SELECT r.*,ST_Intersection(r.geom,g.outer_g) overlap,ST_Area(r.geom) area,ST_DWithin(r.geom,ST_Boundary(g.outer_g),10) edge
            FROM refs r,g WHERE ST_Intersects(r.geom,g.outer_g) AND ST_Area(ST_Intersection(r.geom,g.outer_g))>0''',(sids,json.dumps(geometry)))
            c.execute('SELECT egrid FROM morges_current_pairs');actual={r[0] for r in c.fetchall()};expected={r['egrid'] for r in qa['sectors'][name]['0']['pairs']}
            if actual!=expected:raise ValueError('morges_reference_membership_changed_requires_review')
            c.execute('''WITH frozen AS (SELECT x->'properties'->>'EGRID' egrid,x->'properties'->>'GENRE_TXT' kind,ST_SetSRID(ST_GeomFromGeoJSON((x->'geometry')::text),2056) geom FROM jsonb_array_elements(%s::jsonb) x)
            SELECT count(*) FROM morges_current_pairs n LEFT JOIN frozen f USING(egrid) WHERE f.egrid IS NULL OR NOT ST_Equals(n.geom,f.geom) OR (n.official_attributes->>'GENRE_TXT') IS DISTINCT FROM f.kind''',(Json(frozen),))
            if c.fetchone()[0]:raise ValueError('morges_reference_geometry_changed_requires_review')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_sectors(id,document_id,page_number,label,geom,alignment_rmse_m,validation_evidence,review_status)
            VALUES(%s,%s,1,%s,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326),%s,%s,'review_required') ON CONFLICT DO NOTHING''',(sid,DOC,'Habitat moyenne densité à restructurer / urbaniser — contour indicatif '+name,json.dumps(geometry),a['holdout_rmse_m'],Json(evidence)))
            c.execute('SELECT commune_bfs FROM gold_ch.v_vd_pdcom_review_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!=[5642]:raise ValueError('morges_geographic_attribution_mismatch')
            c.execute('SELECT review_status FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(sid,))
            if c.fetchone()[0]!='review_required':raise ValueError('morges_existing_review_status_requires_review')
            c.execute('SELECT ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)) FROM bronze_ch.vd_pdcom_sectors WHERE id=%s',(json.dumps(geometry),sid))
            if not c.fetchone()[0]:raise ValueError('morges_existing_sector_changed_requires_review')
            c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,));old={r[0] for r in c.fetchall()}
            if old and old!=actual:raise ValueError('morges_existing_candidate_membership_conflict')
            c.execute('''SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN morges_current_pairs n USING(egrid)
            WHERE p.sector_id=%s AND NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326))''',(sid,))
            if c.fetchone()[0]:raise ValueError('morges_existing_candidate_geometry_conflict')
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_parcel_candidates(sector_id,egrid,overlap_m2,parcel_area_m2,overlap_fraction,boundary_review_required,uncertainty_buffer_m,geom,cadastral_object_kind,cadastral_type_evidence)
            SELECT %s,n.egrid,ST_Area(n.overlap),n.area,LEAST(1,ST_Area(n.overlap)/n.area),n.edge,10,ST_Transform(n.overlap,4326),
            COALESCE(%s::jsonb->>(n.official_attributes->>'GENRE_TXT'),'unknown'),
            jsonb_build_object('source_url',s.query_url,'response_sha256',s.response_sha256,'reference_snapshot_ids',jsonb_build_array(n.snapshot_id),'official_attributes',n.official_attributes,'status','matched')
            FROM morges_current_pairs n JOIN bronze_ch.vd_pdcom_parcel_reference_snapshots s ON s.id=n.snapshot_id ON CONFLICT DO NOTHING''',(sid,Json(KINDS)))
            c.execute('''INSERT INTO bronze_ch.vd_pdcom_candidate_parcel_references(sector_id,egrid,snapshot_id)
            SELECT %s,egrid,snapshot_id FROM morges_current_pairs ON CONFLICT(sector_id,egrid) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id''',(sid,))
            c.execute("UPDATE bronze_ch.vd_pdcom_sectors SET validation_evidence=%s WHERE id=%s AND review_status='review_required'",(Json(evidence),sid))
            results.append({'sector_id':sid,'drawing_index':int(name),'parcel_pairs':len(actual)})
        c.execute("UPDATE bronze_ch.vd_pdcom_communes SET extraction_status='candidate_vectors',blocker='Morges private base-hatch review only; overprints, partial paths, footprint differences and later currentness pending' WHERE commune_bfs=5642")
    return {'sectors':len(results),'parcel_pairs':sum(x['parcel_pairs'] for x in results),'candidates':results,'reference_snapshots':references,'status':'review_required'}
