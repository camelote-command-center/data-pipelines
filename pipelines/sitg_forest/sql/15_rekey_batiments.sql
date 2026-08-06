-- ============================================================================
-- Re-key the two batiments syncs onto the business key (177de4c5, Step 1)
-- ============================================================================
-- Both procedures joined on `id`, an ingest-assigned surrogate misaligned
-- across the two databases: 82'423 of 83'018 buildings resolved to a different
-- egid on each side, and only 595 were actually aligned.
--
-- Two things are fixed here, not one:
--
-- 1. The UPDATE branch is re-keyed to (no_comm, no_batiment) and its timestamp
--    gate replaced by a full-column comparison, every value cast to text.
--
-- 2. The INSERT branch is re-keyed too. `WHERE NOT EXISTS (... t.id = s.id)` is
--    what MANUFACTURED duplicate buildings: when a re-ingest renumbered a
--    building, the destination had no row with the new id and a second copy of
--    an already-present building was inserted. Four such duplicates existed on
--    lamap_db (ge_cad_batiments 22356/66643/63815, souterrains 4778, all
--    backed up to backup.*_dupes_20260806 before deletion). Re-keying only the
--    UPDATE would have left that generator running.
--
-- `id` is also dropped from the SET list. Copying the source surrogate onto the
-- destination surrogate is what made the two id spaces look interchangeable;
-- the destination now keeps its own id and the business key identifies the row.
--
-- A UNIQUE INDEX on (no_comm, no_batiment) was added on both destination tables
-- so the database enforces what the sync now assumes.
-- ============================================================================
CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_cad_batiments t SET objectid=s.objectid, commune=s.commune, no_comm=s.no_comm, no_batiment=s.no_batiment, ident=s.ident, egid=s.egid, nombat=s.nombat, mutnum=s.mutnum, datedt=s.datedt, destination=s.destination, nomenclature=s.nomenclature, nomen_classe=s.nomen_classe, provenance=s.provenance, no_autor=s.no_autor, epoque_construction=s.epoque_construction, annee_construction=s.annee_construction, annee_transfornation=s.annee_transfornation, niveaux_horsol=s.niveaux_horsol, niveaux_ssol=s.niveaux_ssol, hauteur=s.hauteur, surface=s.surface, egrid_liste=s.egrid_liste, egrid_centroide=s.egrid_centroide, shape_area=s.shape_area, shape_len=s.shape_len, iteration=s.iteration, created_at=s.created_at, updated_at=s.updated_at, geometry=s.geometry FROM bronze_ch.ge_cad_batiments s WHERE t.no_comm=s.no_comm AND t.no_batiment=s.no_batiment AND ((t."objectid"::text, t."commune"::text, t."no_comm"::text, t."no_batiment"::text, t."ident"::text, t."egid"::text, t."nombat"::text, t."mutnum"::text, t."datedt"::text, t."destination"::text, t."nomenclature"::text, t."nomen_classe"::text, t."provenance"::text, t."no_autor"::text, t."epoque_construction"::text, t."annee_construction"::text, t."annee_transfornation"::text, t."niveaux_horsol"::text, t."niveaux_ssol"::text, t."hauteur"::text, t."surface"::text, t."egrid_liste"::text, t."egrid_centroide"::text, t."shape_area"::text, t."shape_len"::text, t."iteration"::text, t."created_at"::text, t."geometry"::text) IS DISTINCT FROM (s."objectid"::text, s."commune"::text, s."no_comm"::text, s."no_batiment"::text, s."ident"::text, s."egid"::text, s."nombat"::text, s."mutnum"::text, s."datedt"::text, s."destination"::text, s."nomenclature"::text, s."nomen_classe"::text, s."provenance"::text, s."no_autor"::text, s."epoque_construction"::text, s."annee_construction"::text, s."annee_transfornation"::text, s."niveaux_horsol"::text, s."niveaux_ssol"::text, s."hauteur"::text, s."surface"::text, s."egrid_liste"::text, s."egrid_centroide"::text, s."shape_area"::text, s."shape_len"::text, s."iteration"::text, s."created_at"::text, s."geometry"::text));
  INSERT INTO lamap_db_foreign.ge_cad_batiments (id, objectid, commune, no_comm, no_batiment, ident, egid, nombat, mutnum, datedt, destination, nomenclature, nomen_classe, provenance, no_autor, epoque_construction, annee_construction, annee_transfornation, niveaux_horsol, niveaux_ssol, hauteur, surface, egrid_liste, egrid_centroide, shape_area, shape_len, iteration, created_at, updated_at, geometry) SELECT (SELECT coalesce(max(id), 0) FROM lamap_db_foreign.ge_cad_batiments) + row_number() OVER (ORDER BY s.no_comm, s.no_batiment), s.objectid, s.commune, s.no_comm, s.no_batiment, s.ident, s.egid, s.nombat, s.mutnum, s.datedt, s.destination, s.nomenclature, s.nomen_classe, s.provenance, s.no_autor, s.epoque_construction, s.annee_construction, s.annee_transfornation, s.niveaux_horsol, s.niveaux_ssol, s.hauteur, s.surface, s.egrid_liste, s.egrid_centroide, s.shape_area, s.shape_len, s.iteration, s.created_at, s.updated_at, s.geometry FROM bronze_ch.ge_cad_batiments s WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_cad_batiments t WHERE t.no_comm=s.no_comm AND t.no_batiment=s.no_batiment);
END;$procedure$

;

CREATE OR REPLACE PROCEDURE gold_ch.sync_ge_cad_batiments_souterrains()
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'gold_ch', 'bronze_ch', 'public'
 SET statement_timeout TO '600s'
AS $procedure$
BEGIN
  UPDATE lamap_db_foreign.ge_cad_batiments_souterrains t SET objectid=s.objectid, iteration=s.iteration, created_at=s.created_at, updated_at=s.updated_at, commune=s.commune, no_comm=s.no_comm, no_batiment=s.no_batiment, ident=s.ident, egid=s.egid, nombat=s.nombat, mutnum=s.mutnum, datedt=s.datedt, destination=s.destination, nomenclature=s.nomenclature, nomen_classe=s.nomen_classe, provenance=s.provenance, no_autor=s.no_autor, epoque_construction=s.epoque_construction, annee_construction=s.annee_construction, annee_transfornation=s.annee_transfornation, niveaux_horsol=s.niveaux_horsol, niveaux_ssol=s.niveaux_ssol, hauteur=s.hauteur, surface=s.surface, egrid_liste=s.egrid_liste, egrid_centroide=s.egrid_centroide, shape_area=s.shape_area, shape_len=s.shape_len, geometry=s.geometry FROM bronze_ch.ge_cad_batiments_souterrains s WHERE t.no_comm=s.no_comm AND t.no_batiment=s.no_batiment AND ((t."objectid"::text, t."iteration"::text, t."created_at"::text, t."commune"::text, t."no_comm"::text, t."no_batiment"::text, t."ident"::text, t."egid"::text, t."nombat"::text, t."mutnum"::text, t."datedt"::text, t."destination"::text, t."nomenclature"::text, t."nomen_classe"::text, t."provenance"::text, t."no_autor"::text, t."epoque_construction"::text, t."annee_construction"::text, t."annee_transfornation"::text, t."niveaux_horsol"::text, t."niveaux_ssol"::text, t."hauteur"::text, t."surface"::text, t."egrid_liste"::text, t."egrid_centroide"::text, t."shape_area"::text, t."shape_len"::text, t."geometry"::text) IS DISTINCT FROM (s."objectid"::text, s."iteration"::text, s."created_at"::text, s."commune"::text, s."no_comm"::text, s."no_batiment"::text, s."ident"::text, s."egid"::text, s."nombat"::text, s."mutnum"::text, s."datedt"::text, s."destination"::text, s."nomenclature"::text, s."nomen_classe"::text, s."provenance"::text, s."no_autor"::text, s."epoque_construction"::text, s."annee_construction"::text, s."annee_transfornation"::text, s."niveaux_horsol"::text, s."niveaux_ssol"::text, s."hauteur"::text, s."surface"::text, s."egrid_liste"::text, s."egrid_centroide"::text, s."shape_area"::text, s."shape_len"::text, s."geometry"::text));
  INSERT INTO lamap_db_foreign.ge_cad_batiments_souterrains (id, objectid, iteration, created_at, updated_at, commune, no_comm, no_batiment, ident, egid, nombat, mutnum, datedt, destination, nomenclature, nomen_classe, provenance, no_autor, epoque_construction, annee_construction, annee_transfornation, niveaux_horsol, niveaux_ssol, hauteur, surface, egrid_liste, egrid_centroide, shape_area, shape_len, geometry) SELECT (SELECT coalesce(max(id), 0) FROM lamap_db_foreign.ge_cad_batiments_souterrains) + row_number() OVER (ORDER BY s.no_comm, s.no_batiment), s.objectid, s.iteration, s.created_at, s.updated_at, s.commune, s.no_comm, s.no_batiment, s.ident, s.egid, s.nombat, s.mutnum, s.datedt, s.destination, s.nomenclature, s.nomen_classe, s.provenance, s.no_autor, s.epoque_construction, s.annee_construction, s.annee_transfornation, s.niveaux_horsol, s.niveaux_ssol, s.hauteur, s.surface, s.egrid_liste, s.egrid_centroide, s.shape_area, s.shape_len, s.geometry FROM bronze_ch.ge_cad_batiments_souterrains s WHERE NOT EXISTS (SELECT 1 FROM lamap_db_foreign.ge_cad_batiments_souterrains t WHERE t.no_comm=s.no_comm AND t.no_batiment=s.no_batiment);
END;$procedure$

;

