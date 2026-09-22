"""Exact reviewed opacity correction; only the historical cordon collection changes."""
import hashlib,json,uuid
from pathlib import Path
from psycopg2.extras import Json
DOC='ad153736-58f1-533f-8cca-e41524f9fe12'
OLD_SHA='1770673f8bb8e50367de24f69478a2b986806cf2710eb2cae639e8b15480aa73'
REMOVED='CH714595838194'
def load_old():
    raw=(Path(__file__).parent/'reports/prangins-correction/old-feature.json').read_bytes()
    if hashlib.sha256(raw).hexdigest()!=OLD_SHA:raise ValueError('prangins_correction_changed_preimage')
    b=json.loads(raw)
    if b['path_ids']!=[143,144,147] or len(b['expected_pairs'])!=22:raise ValueError('prangins_correction_preimage_scope')
    return b
def evidence():
    return dict(reason='Exact source parent-group opacity separates Renforcement de la structure existante (0.5) from Cordons boisés à créer (1.0).',removed_source_path=147,retained_source_paths=[143,144],replacement_category='reinforcement_existing_structure',old_feature_sha256=OLD_SHA,source_currentness='unresolved; historical source category correction only')
def correct(cursor,sid,new_geom):
    """Run inside caller transaction after new-reference and geometry gates passed."""
    c=cursor
    if str(sid)!=str(uuid.uuid5(uuid.NAMESPACE_URL,DOC+'#indicative-category-2026-09-22#wooded_cordons_create')):raise ValueError('prangins_correction_wrong_sector')
    old=load_old();og=json.dumps(old['geometry_lv95'])
    c.execute('''SELECT document_id::text,review_status,source_precision_m,validated_by,
      ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326)),
      ST_Equals(geom,ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326))
      FROM bronze_ch.vd_pdcom_sectors WHERE id=%s FOR UPDATE''',(new_geom,og,sid))
    row=c.fetchone()
    if row is None:return False
    if row[:4]!=(DOC,'review_required',None,None):raise ValueError('prangins_correction_sector_state')
    if row[4]:return False
    if not row[5]:raise ValueError('prangins_correction_unknown_geometry')
    frozen=[dict(egrid=r['egrid'],geometry=r['geometry'],kind=r['attributes']['GENRE_TXT']) for r in old['expected_pairs']]
    c.execute('SELECT egrid FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
    if {r[0] for r in c.fetchall()}!={r['egrid'] for r in frozen}:raise ValueError('prangins_correction_old_membership')
    c.execute('''WITH g AS (SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s),2056) geom),r AS
      (SELECT x->>'egrid' egrid,x->>'kind' kind,ST_SetSRID(ST_GeomFromGeoJSON(x->'geometry'),2056) geom FROM jsonb_array_elements(%s::jsonb)x),
      n AS (SELECT r.*,ST_Intersection(r.geom,g.geom) overlap,ST_DWithin(r.geom,ST_Boundary(g.geom),10) edge FROM r,g)
      SELECT count(*) FROM bronze_ch.vd_pdcom_parcel_candidates p JOIN n USING(egrid) WHERE p.sector_id=%s AND
      (NOT ST_Equals(p.geom,ST_Transform(n.overlap,4326)) OR p.overlap_m2 IS DISTINCT FROM ST_Area(n.overlap)
       OR p.parcel_area_m2 IS DISTINCT FROM ST_Area(n.geom) OR p.overlap_fraction IS DISTINCT FROM LEAST(1,ST_Area(n.overlap)/ST_Area(n.geom))
       OR p.boundary_review_required IS DISTINCT FROM n.edge OR p.uncertainty_buffer_m IS DISTINCT FROM 10
       OR p.cadastral_object_kind IS DISTINCT FROM CASE n.kind WHEN 'DDP superficie' THEN 'ddp_superficie' WHEN 'DDP source' THEN 'ddp_source' ELSE 'bien_fonds' END)''',(og,Json(frozen),sid))
    if c.fetchone()[0]:raise ValueError('prangins_correction_old_values')
    c.execute('''SELECT count(*) FROM gold_ch.v_vd_pdcom_review_parcels s FULL JOIN lamap_db_foreign.vd_pdcom_review_parcels t USING(sector_id,egrid)
      WHERE COALESCE(s.sector_id,t.sector_id)=%s AND (s.sector_id IS NULL OR t.sector_id IS NULL OR to_jsonb(s) IS DISTINCT FROM to_jsonb(t))''',(sid,))
    if c.fetchone()[0]:raise ValueError('prangins_correction_receiver_preimage_mismatch')
    c.execute('SELECT egrid FROM prangins_pairs')
    if {r[0] for r in c.fetchall()}!={r['egrid'] for r in frozen}-{REMOVED}:raise ValueError('prangins_correction_new_membership')
    # Only one disappearing pair. Surviving receiver rows are canonical-upserted later.
    c.execute('DELETE FROM lamap_db_foreign.vd_pdcom_review_parcels WHERE sector_id=%s AND egrid=%s',(sid,REMOVED))
    if c.rowcount!=1:raise ValueError('prangins_correction_receiver_delete_count')
    c.execute('DELETE FROM bronze_ch.vd_pdcom_candidate_parcel_references WHERE sector_id=%s',(sid,))
    c.execute('DELETE FROM bronze_ch.vd_pdcom_parcel_candidates WHERE sector_id=%s',(sid,))
    if c.rowcount!=22:raise ValueError('prangins_correction_source_delete_count')
    c.execute('UPDATE bronze_ch.vd_pdcom_sectors SET geom=ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(%s),2056),4326) WHERE id=%s',(new_geom,sid))
    return True
