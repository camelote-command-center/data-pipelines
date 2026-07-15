-- ============================================================================
-- 2026-07-15 — A2: FEDERAL sites pollués sub-registers (VBS / BAZL / BAV) — BRONZE
-- ============================================================================
-- GE has 4 sites-pollués variants (id116 base + id117 militaire + id118 aéroports
-- + id119 transports publics). VD had only the base. These are the federal three.
--
-- ── SOURCE: geo.admin.ch — FEDERAL, therefore UNGATED ──────────────────────
-- Unlike the cantonal agsgc.map.vd.ch (VPS-only) and geodienste (VD Freigabe-gated,
-- 401), these are published by the Confederation and answer from a laptop:
--   militaire          ch.vbs.kataster-belasteter-standorte-militaer
--   aeroports          ch.bazl.kataster-belasteter-standorte-zivilflugplaetze
--   transports_publics ch.bav.kataster-belasteter-standorte-oev
--
-- ⚠️ api3 `identify` SILENTLY CAPS at ~201 results — limit=500 and limit=2000 both
--    return 201, HTTP 200, no warning. It is a CAP, not a count. Fetched with
--    RECURSIVE QUADTREE TILING (split any tile at/over the cap). Militaire:
--    201 single-call → **323** tiled. A naive fetch would have silently lost 122 sites.
--    (Same silent-truncation family as the geodienste WFS returning 0 for VD.)
--
-- ⚠️ GEOMETRY IS MIXED — hence geometry(Geometry, 2056), not MultiPolygon:
--      militaire           140 Point + 183 MultiPolygon
--      aeroports            14 MultiPoint + 19 MultiPolygon
--      transports_publics  201 MultiPolygon
--    Typing MultiPolygon would silently drop 154 point objects, and the
--    ST_CollectionExtract(…,3) guard returns EMPTY for points — it would destroy them.
--
-- ⚠️ NATIONAL SCOPE, NOT canton-stamped. These are federal registers fetched over the
--    VD *envelope*, which overlaps FR/GE/VS/NE. There is deliberately NO
--    canton_code='VD' CHECK (that would stamp VD onto neighbouring-canton sites).
--    VD scoping happens in the link, by intersecting VD plots — which is the only
--    honest filter available (re-LLM has no VD boundary polygon: ref.cantons has no
--    geometry, and ref.communes.ofs_number is not the BFS).
--    Follows the existing `federal_bav_transit` precedent for federal data.
--
-- ROLLBACK at the bottom.
-- ============================================================================

BEGIN;
SET LOCAL lock_timeout = '60s';

CREATE TABLE IF NOT EXISTS bronze_ch.federal_kbs_sites (
  raw_data              jsonb,
  registre              text NOT NULL
                        CHECK (registre IN ('militaire','aeroports','transports_publics')),
  feature_id            text NOT NULL,
  katasternummer        text,
  statut                text,      -- statusaltlv_fr  (the KbS legal status)
  standorttyp           text,      -- standorttyp_fr
  untersuchungsmassnahmen text,    -- untersuchungsmassnahmen_fr
  url_fiche             text,      -- url_fr
  geometry              geometry(Geometry, 2056),   -- MIXED point + polygon (see header)
  source_layer          text NOT NULL,
  first_seen_at         timestamptz NOT NULL DEFAULT now(),
  last_seen_at          timestamptz NOT NULL DEFAULT now(),
  deleted_at            timestamptz,
  PRIMARY KEY (registre, feature_id)
);
COMMENT ON TABLE bronze_ch.federal_kbs_sites IS
  'FEDERAL sites pollués sub-registers (VBS militaire / BAZL aéroports / BAV transports publics) from '
  'geo.admin.ch — federal, UNGATED, reachable without the VPS (unlike cantonal agsgc) and without a '
  'geodienste Freigabe. Mirrors GE id117/118/119. NATIONAL scope: fetched over the VD envelope, which '
  'overlaps neighbouring cantons ⇒ deliberately NOT canton-stamped; VD scoping happens in the link by '
  'intersecting VD plots. ⚠️ api3 identify silently caps at ~201 — fetched with recursive quadtree '
  'tiling (militaire 201 single-call → 323 tiled). ⚠️ geometry is MIXED point+polygon.';
COMMENT ON COLUMN bronze_ch.federal_kbs_sites.geometry IS
  'MIXED: Point/MultiPoint AND MultiPolygon — the registers store some sites as locations, some as '
  'perimeters. Never apply ST_CollectionExtract(…,3) to this column: it returns EMPTY for points.';

CREATE INDEX IF NOT EXISTS federal_kbs_sites_geom_idx
  ON bronze_ch.federal_kbs_sites USING GIST (geometry) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS federal_kbs_sites_registre_idx
  ON bronze_ch.federal_kbs_sites (registre) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS federal_kbs_sites_lastseen_idx
  ON bronze_ch.federal_kbs_sites (last_seen_at);

COMMIT;

-- ============================================================================
-- ROLLBACK
-- ============================================================================
-- BEGIN;
--   DROP TABLE IF EXISTS bronze_ch.federal_kbs_sites RESTRICT;
--   DELETE FROM supabase_migrations.schema_migrations WHERE version = '20260715000006';
-- COMMIT;
-- ============================================================================
