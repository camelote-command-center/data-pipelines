-- ═══════════════════════════════════════════════════════════════════════════
-- SITG tree cadastre — bronze landing tables on re-LLM
--
-- Two layers, both licensed "A — Accès libre" with no restriction (verified on
-- the SITG catalogue 2026-08-07):
--   SIPV_ICA_ARBRE_ISOLE      239'167 pts  inventaire cantonal des arbres isolés
--   FFP_ARBRES_REMARQUABLES       594 pts  recensement des arbres remarquables
--
-- A third layer, SIPV_ICA_ABATTAGE_SEV_PTS (felling authorisations), was
-- DELIBERATELY NOT INGESTED. Its licence is "Accès libre, usage privé /
-- A* non commercial" plus "Uniquement pour l'affichage dans la carte
-- interactive dédiée". Both clauses independently exclude our use: Lamap and
-- LBI are commercial, and the dedicated-map clause forbids re-display anywhere
-- else. Do not add it later without a written licence variation from the
-- Ville de Genève.
--
-- CONFLICT KEY IS globalid, NOT id_arbre.
-- id_arbre looks like the natural key and is not one. On the remarquables
-- layer, 262 of 594 rows carry id_arbre = 0 as a null sentinel, so keying on it
-- would collapse 262 rows into 1 on every run. That is precisely the defect
-- logged as bug ab259069 on another pipeline, found the same week. globalid is
-- an ArcGIS-guaranteed unique GUID. id_arbre is carried as an indexed
-- attribute and its uniqueness is asserted in SQL after load, not assumed.
-- ═══════════════════════════════════════════════════════════════════════════

-- COLUMN TYPES COME FROM THE LAYER'S DECLARED esri FIELD TYPES, not from
-- eyeballing a sample row. Every numeric field on SIPV_ICA_ARBRE_ISOLE is
-- esriFieldTypeDouble, and four of them really are fractional in the live data
-- (rayon_couronne on 62'079 rows, hauteur_tronc 30'696, hauteur_totale 11'363,
-- diametre_couronne 2'916). Typing them integer from a sample whose values
-- happened to be whole failed the first load with 22P02 on "2.5".
-- FFP_ARBRES_REMARQUABLES declares genuine esriFieldTypeInteger, so it differs.

CREATE SCHEMA IF NOT EXISTS bronze_ch;

-- ── Inventaire cantonal des arbres isolés ────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_ch.ge_sipv_arbre_isole (
  globalid                 text PRIMARY KEY,
  objectid                 bigint,
  id_arbre                 bigint,
  no_inventaire            text,
  nom_complet              text,          -- latin species incl. variety
  classe                   text,          -- Feuillus / Conifères / ...
  remarquable              text,          -- free text reason; NULL = not flagged
  situation                text,
  type_plantation          text,
  nombre_troncs            text,          -- arrives as a string, kept verbatim
  circonference_1m         numeric,       -- cm, MEASURED at 1 m. Drives the
                                          -- felling-authorisation flag.
  diametre_1m              numeric,       -- cm, sparse
  hauteur_tronc            numeric,
  hauteur_totale           numeric,       -- m
  diametre_couronne        numeric,       -- m
  rayon_couronne           numeric,       -- m
  forme                    text,
  stade_developpement      text,
  vitalite                 text,
  conduite                 text,
  type_sol                 text,
  type_surface             text,
  esperance_vie            numeric,
  souche                   text,          -- 'Oui' = stump, 67 rows
  statut                   text,          -- POSITIONAL status, see comment below
  id_acteur                text,
  date_plantation          timestamptz,
  date_plantation_estimee  numeric,
  date_observation         timestamptz,
  geom                     geometry(Point, 2056),
  ingested_at              timestamptz NOT NULL DEFAULT now(),
  updated_at               timestamptz NOT NULL DEFAULT now(),
  deleted_at               timestamptz
);

COMMENT ON COLUMN bronze_ch.ge_sipv_arbre_isole.statut IS
  'Positional provenance, NOT a legal status. SITG documents an accuracy for '
  'only two of the four live values: Historique (1976 paper survey, ~25 m) and '
  'Relevé (recent field survey, ~1 m). Positionné (6165 rows) and Nommé (2320) '
  'have NO documented accuracy and must not be assigned one. Carried verbatim '
  'so the consumer can caveat rather than guess.';

COMMENT ON COLUMN bronze_ch.ge_sipv_arbre_isole.circonference_1m IS
  'Circumference in cm measured at 1 m trunk height. NULL on 101462 of 239167 '
  'rows (42%) and literal 0 on a further 2411. Both mean "not measured", never '
  '"small". Any downstream threshold test must stay NULL for these rather than '
  'resolving to false.';

CREATE INDEX IF NOT EXISTS ge_sipv_arbre_isole_geom_gix
  ON bronze_ch.ge_sipv_arbre_isole USING GIST (geom);
CREATE INDEX IF NOT EXISTS ge_sipv_arbre_isole_id_arbre_ix
  ON bronze_ch.ge_sipv_arbre_isole (id_arbre);
CREATE INDEX IF NOT EXISTS ge_sipv_arbre_isole_live_ix
  ON bronze_ch.ge_sipv_arbre_isole (deleted_at) WHERE deleted_at IS NULL;

-- ── Recensement des arbres remarquables ──────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_ch.ge_ffp_arbres_remarquables (
  globalid         text PRIMARY KEY,
  objectid         bigint,
  id_arbre         bigint,        -- 0 on 262 of 594 rows: null sentinel
  espece           text,
  diametre_tronc   integer,       -- cm
  interet_1        text,
  interet_2        text,
  interet_3        text,
  etat             text,          -- VALIDATION status, see comment
  remarque         text,
  geom             geometry(Point, 2056),
  ingested_at      timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  deleted_at       timestamptz
);

COMMENT ON TABLE bronze_ch.ge_ffp_arbres_remarquables IS
  'Presence in this layer does NOT mean a tree is remarkable. The layer is the '
  'assessment register, and it carries rejected and pending candidates too. '
  'Of 594 rows: 208 Approuvé remarquable, 221 En cours de vérification, 165 '
  'Non remarquable. Only the 208 are legally remarkable, which matches the ~206 '
  'figure OCAN publishes. Filter on etat.';

COMMENT ON COLUMN bronze_ch.ge_ffp_arbres_remarquables.etat IS
  'Assessment outcome: "Approuvé remarquable" | "En cours de vérification" | '
  '"Non remarquable". Remarkability is a scored designation (OCAN/CJB/HEPIA '
  'weight six criteria, >=12 of 20 points), NOT a size threshold, so it can '
  'only be carried from source and never derived from circumference.';

COMMENT ON COLUMN bronze_ch.ge_ffp_arbres_remarquables.id_arbre IS
  '0 is a null sentinel on 262 of 594 rows, not a tree id. Never use as a '
  'conflict key. Cross-reference to ge_sipv_arbre_isole is by id_arbre only '
  'where non-zero, otherwise by spatial proximity.';

CREATE INDEX IF NOT EXISTS ge_ffp_arbres_remarquables_geom_gix
  ON bronze_ch.ge_ffp_arbres_remarquables USING GIST (geom);
CREATE INDEX IF NOT EXISTS ge_ffp_arbres_remarquables_id_arbre_ix
  ON bronze_ch.ge_ffp_arbres_remarquables (id_arbre) WHERE id_arbre <> 0;
