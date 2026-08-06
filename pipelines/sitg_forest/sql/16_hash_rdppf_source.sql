-- ---- bronze_ch.ge_rdppf_synthese : 54 columns hashed ----
ALTER TABLE bronze_ch.ge_rdppf_synthese ADD COLUMN IF NOT EXISTS content_hash text;

CREATE OR REPLACE FUNCTION bronze_ch.ge_rdppf_synthese_content_hash() RETURNS trigger
LANGUAGE plpgsql
IMMUTABLE
AS $fn$
BEGIN
  NEW.content_hash := md5(concat_ws('|',
           coalesce(to_char(NEW."created_at" AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), '\x00NULL'),
           coalesce(NEW."egrid"::text, '\x00NULL'),
           coalesce(NEW."extrait_rdppf_pdf"::text, '\x00NULL'),
           coalesce(NEW."genre"::text, '\x00NULL'),
           coalesce(NEW."geometry"::text, '\x00NULL'),
           coalesce(NEW."globalid"::text, '\x00NULL'),
           coalesce(NEW."id103_ch_zr_instal_aero"::text, '\x00NULL'),
           coalesce(NEW."id104_ch_align_instal_aero"::text, '\x00NULL'),
           coalesce(NEW."id108_ch_plan_zone_securite"::text, '\x00NULL'),
           coalesce(NEW."id116_cadastre_site_pollues"::text, '\x00NULL'),
           coalesce(NEW."id117_ch_cad_site_pollues_mili"::text, '\x00NULL'),
           coalesce(NEW."id118_ch_cad_site_pollues_aero"::text, '\x00NULL'),
           coalesce(NEW."id119_ch_cad_site_pollues_tp"::text, '\x00NULL'),
           coalesce(NEW."id131_zones_protec_eaux_sout"::text, '\x00NULL'),
           coalesce(NEW."id132_perim_protec_eaux_sout"::text, '\x00NULL'),
           coalesce(NEW."id145_dsopb"::text, '\x00NULL'),
           coalesce(NEW."id157_limites_foret_statiques"::text, '\x00NULL'),
           coalesce(NEW."id159_distances_foret"::text, '\x00NULL'),
           coalesce(NEW."id160_reserves_forestieres"::text, '\x00NULL'),
           coalesce(NEW."id190_ere"::text, '\x00NULL'),
           coalesce(NEW."id217_ch_zr_ltn220"::text, '\x00NULL'),
           coalesce(NEW."id218_ch_align_instal_elec"::text, '\x00NULL'),
           coalesce(NEW."id73_extract_grav"::text, '\x00NULL'),
           coalesce(NEW."id73_pdzam"::text, '\x00NULL'),
           coalesce(NEW."id73_pdzi"::text, '\x00NULL'),
           coalesce(NEW."id73_pla"::text, '\x00NULL'),
           coalesce(NEW."id73_plcp"::text, '\x00NULL'),
           coalesce(NEW."id73_plq"::text, '\x00NULL'),
           coalesce(NEW."id73_pnp"::text, '\x00NULL'),
           coalesce(NEW."id73_ps"::text, '\x00NULL'),
           coalesce(NEW."id73_pus"::text, '\x00NULL'),
           coalesce(NEW."id73_rs"::text, '\x00NULL'),
           coalesce(NEW."id73_si"::text, '\x00NULL'),
           coalesce(NEW."id73_zones_affect_prim"::text, '\x00NULL'),
           coalesce(NEW."id73_zones_affect_superp"::text, '\x00NULL'),
           coalesce(NEW."id73_zones_affect_synt"::text, '\x00NULL'),
           coalesce(NEW."id73_zp"::text, '\x00NULL'),
           coalesce(NEW."id76_zr"::text, '\x00NULL'),
           coalesce(NEW."id87_zr_rn"::text, '\x00NULL'),
           coalesce(NEW."id88_ch_align_rn"::text, '\x00NULL'),
           coalesce(NEW."id96_ch_zr_instal_ferrov"::text, '\x00NULL'),
           coalesce(NEW."id97_ch_align_instal_ferrov"::text, '\x00NULL'),
           coalesce(NEW."idge_1079_cadastre_foret"::text, '\x00NULL'),
           coalesce(NEW."idge_1175_ouvrages_souterrains"::text, '\x00NULL'),
           coalesce(NEW."idge_xxxx_align_const_lroutes"::text, '\x00NULL'),
           coalesce(NEW."idxxge_cadastre_foret"::text, '\x00NULL'),
           coalesce(NEW."idxxge_lim_const_lroutes"::text, '\x00NULL'),
           coalesce(NEW."iteration"::text, '\x00NULL'),
           coalesce(NEW."lien_www"::text, '\x00NULL'),
           coalesce(NEW."numero"::text, '\x00NULL'),
           coalesce(NEW."objectid"::text, '\x00NULL'),
           coalesce(NEW."shape__area"::text, '\x00NULL'),
           coalesce(NEW."shape__length"::text, '\x00NULL'),
           coalesce(NEW."surface"::text, '\x00NULL')));
  RETURN NEW;
END $fn$;

DROP TRIGGER IF EXISTS ge_rdppf_synthese_content_hash_trg ON bronze_ch.ge_rdppf_synthese;
CREATE TRIGGER ge_rdppf_synthese_content_hash_trg
  BEFORE INSERT OR UPDATE ON bronze_ch.ge_rdppf_synthese
  FOR EACH ROW EXECUTE FUNCTION bronze_ch.ge_rdppf_synthese_content_hash();

-- Backfill every existing row through the trigger.
UPDATE bronze_ch.ge_rdppf_synthese SET content_hash = NULL;

CREATE INDEX IF NOT EXISTS ge_rdppf_synthese_content_hash_idx ON bronze_ch.ge_rdppf_synthese (content_hash);
