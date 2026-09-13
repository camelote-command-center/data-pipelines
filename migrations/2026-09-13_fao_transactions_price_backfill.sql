-- ============================================================================
-- 2026-09-13 — FAO transactions: backfill prices that are visible in the stored
-- gazette text but NULL in bronze_ch.fao_transactions.price.
-- ============================================================================
-- ROOT CAUSE (verified against live data, not assumed):
--   * 84 rows carry "Prix total de l'affaire: X" and were parsed 2026-03-09..04-11,
--     BEFORE price.ts (regex-primary extraction) shipped on 2026-05-13. The LLM was
--     then the only price path; it dropped these. The May fix was forward-only —
--     the tail was never reparsed. 0 such rows since 2026-05-13.
--   * The regex only knew "Prix total de l'affaire". Multi-lot entries publish one
--     "Prix: X.-" per lot; those never matched, and when the LLM also missed them the
--     price was written NULL with exit 0. STILL LIVE: 8 rows since 2026-05-13.
--
-- VALUES were computed by the parser's OWN validateParsedPrice() (pipelines/
-- transactions-fao/sql/2026-09-13_compute_price_backfill.ts), with the LLM input
-- passed as null — so each value is byte-identical to what the fixed parser writes,
-- and no LLM is invoked. Multi-lot rule = SUM of parts: the established semantics,
-- 381 of 435 already-priced multi-lot rows (88%) store the sum.
--
-- KEYED BY PRIMARY KEY id. affaire_number is NOT a safe key: 31,155 of 106,815
-- rows have it NULL. 14 of the 117 rows below are such legacy rows.
--
-- NO DELETIONS. UPDATE only, guarded by coalesce(price,'')='' so an existing price
-- is never overwritten; re-running is a no-op. Pre-image in backup table.
-- Ambiguous/malformed amounts (97 rows) are NOT guessed — they are quarantined.
-- ============================================================================

BEGIN;
SET LOCAL statement_timeout = '300s';

CREATE SCHEMA IF NOT EXISTS backup;
CREATE TABLE IF NOT EXISTS backup.fao_transactions_price_backfill_20260913 AS
SELECT id, affaire_number, price, type_clean_list, updated_at, now() AS backed_up_at
FROM bronze_ch.fao_transactions WHERE false;

CREATE TEMP TABLE _bf(id int PRIMARY KEY, affaire_number text, new_price text) ON COMMIT DROP;
INSERT INTO _bf VALUES
    (211, '2025/8185/0', '30350000'),
    (391, '2025/8809/0', '10148250'),
    (951, '2025/12041/0', '22300000'),
    (1151, '2025/12913/0', '335000'),
    (1516, '2025/15113/0', '27000'),
    (1695, '2026/1150/0', '15600000'),
    (1701, '2026/1123/0', '4700000'),
    (1707, '2026/1090/0', '1250000'),
    (1712, '2026/1068/0', '1100000'),
    (1714, '2026/1065/0', '1200000'),
    (1715, '2026/1063/0', '1300000'),
    (1726, '2026/1006/0', '1650000'),
    (1750, '2026/883/0', '969422'),
    (1763, '2026/655/0', '841620'),
    (1765, '2026/651/0', '667746'),
    (1767, '2026/619/0', '1370000'),
    (1768, '2026/617/0', '570000'),
    (2065, '2025/9347/0', '14367163'),
    (2246, '2024/8608/0', '848400'),
    (2264, '2024/6879/0', '2950000'),
    (2273, '2024/6323/0', '100'),
    (2286, '2024/8385/0', '1085000'),
    (2289, '2024/8373/0', '324100'),
    (2291, '2024/8375/0', '1158700'),
    (2293, '2024/8380/0', '1616900'),
    (2296, '2024/9117/0', '1154000'),
    (2312, '2024/8354/0', '14000000'),
    (2321, '2024/8334/0', '700000'),
    (2326, '2024/8322/0', '1394000'),
    (2337, '2024/8279/0', '1031400'),
    (2348, '2024/5761/0', '850000'),
    (2350, '2024/5750/0', '760000'),
    (2351, '2024/5748/0', '860000'),
    (2379, '2024/4796/0', '1775000'),
    (2384, '2024/6085/0', '1089700'),
    (2449, '2026/2171/0', '2600000'),
    (2463, '2026/2131/0', '2900000'),
    (2480, '2026/1794/0', '1080840'),
    (2482, '2026/1789/0', '682000'),
    (2501, '2026/1722/0', '660516.10'),
    (2502, '2026/1721/0', '680516.10'),
    (2534, '2026/1608/0', '320000'),
    (2535, '2026/1605/0', '310000'),
    (2536, '2026/1603/0', '270000'),
    (32214, '2014/134/0', '1050000.00'),
    (42759, '2017/13047/0', '652500'),
    (47918, '2018/7072/0', '100'),
    (47985, '2018/7320/0', '350000'),
    (48172, '2018/8050/0', '5900000'),
    (53548, '2020/13705/0', '2450000'),
    (72663, '2025/10923/0', '1800000'),
    (73447, '2025/13421/0', '10500'),
    (73448, '2025/13423/0', '376250'),
    (73467, '2025/13492/0', '7500000'),
    (73468, '2025/13502/0', '661400'),
    (73469, '2025/13505/0', '1265000'),
    (73470, '2025/13508/0', '855500'),
    (73471, '2025/13512/0', '2050000'),
    (73481, '2025/13536/0', '2200000'),
    (73482, '2025/13538/0', '1950000'),
    (73483, '2025/13540/0', '1250000'),
    (73484, '2025/13543/0', '1290000'),
    (73485, '2025/13546/0', '925000'),
    (73486, '2025/13549/0', '90000'),
    (73488, '2025/13551/0', '1710000'),
    (73490, '2025/13557/0', '1640000'),
    (73491, '2025/13560/0', '840000'),
    (73492, '2025/13564/0', '2719200'),
    (73494, '2025/13568/0', '1850000'),
    (73496, '2025/13578/0', '877456'),
    (73497, '2025/13584/0', '1340000'),
    (73498, '2025/13586/0', '1366666'),
    (73583, '2025/13917/0', '750000'),
    (73705, '2025/14334/0', '601344'),
    (73706, '2025/14336/0', '607728'),
    (73707, '2025/14340/0', '1160000'),
    (73708, '2025/14342/0', '1780000'),
    (73709, '2025/14344/0', '4290000'),
    (73712, '2025/14353/0', '1000000'),
    (73713, '2025/14356/0', '2441598'),
    (73714, '2025/14358/0', '832104'),
    (73715, '2025/14360/0', '940000'),
    (73716, '2025/14362/0', '805000'),
    (73717, '2025/14365/0', '2000000'),
    (73718, '2025/14370/0', '1500000'),
    (73719, '2025/14373/0', '575000'),
    (73720, '2025/14375/0', '2500000'),
    (74348, '2025/2991/0', '9337317'),
    (74602, '2025/390/0', '0'),
    (74817, '2025/4655/0', '47800000'),
    (76015, '2025/8760/0', '13507000'),
    (77912, NULL, '505000'),
    (78706, NULL, '113500'),
    (84931, NULL, '490000'),
    (88896, NULL, '77000'),
    (89121, NULL, '482291'),
    (90472, NULL, '401460'),
    (91563, NULL, '115000'),
    (91683, NULL, '0'),
    (95260, NULL, '580000'),
    (99463, NULL, '0'),
    (99646, NULL, '0'),
    (100644, NULL, '397965'),
    (100933, NULL, '93000'),
    (102212, NULL, '0'),
    (108963, '2026/3020/0', '100'),
    (108973, '2026/2975/0', '1800000'),
    (109030, '2026/2928/0', '769000'),
    (109053, '2025/13969/0', '1820000'),
    (110180, '2025/13265/0', '11800'),
    (112277, '2026/4918/0', '22300000'),
    (112515, '2026/5866/0', '1550000'),
    (112591, '2026/5275/0', '5961724'),
    (113499, '2026/7033/0', '28925000'),
    (114020, '2026/1026/0', '41975000'),
    (114023, '2026/2908/0', '792712.35'),
    (114054, '2026/9084/0', '59927500');

CREATE TEMP TABLE _qt(id int PRIMARY KEY, affaire_number text, reason text, fragment text) ON COMMIT DROP;
INSERT INTO _qt VALUES
    (76935, 'VA 15171', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76938, 'VA 15178', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76952, 'VA 15192', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76964, 'VA 15204', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76974, 'VA 15214', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76975, 'VA 15215', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (76980, 'VA 15220', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77008, 'VA 15248', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77010, 'VA 15250', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77013, 'VA 15253', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77034, 'VA 15274', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77035, 'VA 15275', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77039, 'VA 15279', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77040, 'VA 15280', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77059, 'VA 15300', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77060, 'VA 15301', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77061, 'VA 15302', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77067, 'VA 15309', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77068, 'VA 15310', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77070, 'VA 15312', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77091, 'VA 15333', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77092, 'VA 15334', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77093, 'VA 15335', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77096, 'VA 15338', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77098, 'VA 15340', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77149, 'VA 15393', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77156, 'VA 15401', 'price_in_text_not_extracted', 'Prix de vente: Frs 1'),
    (77178, 'VA 15424', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77187, 'VA 15435', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77192, 'VA 15440', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77193, 'VA 15441', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77208, 'VA 15456', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77210, 'VA 15458', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77232, 'VA 15480', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77233, 'VA 15481', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77234, 'VA 15482', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77248, 'VA 15497', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77255, 'VA 15504', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77257, 'VA 15506', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77261, 'VA 15510', 'price_in_text_not_extracted', 'Prix de vente: frs 6'),
    (77273, 'VA 15523', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77294, 'VA 15545', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77299, 'VA 15550', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77302, 'VA 15553', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77316, 'VA 15569', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77319, 'VA 15573', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77328, 'VA 15582', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77330, 'VA 15584', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77332, 'VA 15587', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77335, 'VA 15590', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77339, 'VA 15594', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77351, 'VA 15606', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77352, 'VA 15607', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77353, 'VA 15608', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77359, 'VA 15614', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77360, 'VA 15615', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77361, 'VA 15616', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77362, 'VA 15617', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77363, 'VA 15618', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77365, 'VA 15620', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77375, 'VA 15630', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77379, 'VA 15634', 'price_in_text_not_extracted', 'Prix de vente: frs 6'),
    (77380, 'VA 15635', 'price_in_text_not_extracted', 'Prix de vente: frs 6'),
    (77422, 'VA 15679', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77424, 'VA 15681', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77429, 'VA 15686', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77436, 'VA 15694', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77463, 'VA 15723', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77472, 'VA 15732', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77473, 'VA 15733', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77474, 'VA 15734', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77475, 'VA 15735', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77478, 'VA 15738', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77481, 'VA 15741', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77486, 'VA 15746', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77507, 'VA 15767', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77508, 'VA 15768', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77515, 'VA 15775', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77517, 'VA 15777', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77518, 'VA 15778', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77526, 'VA 15787', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77527, 'VA 15788', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77535, 'VA 15799', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77539, 'VA 15803', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77570, 'VA 15836', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77581, 'VA 15848', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77585, 'VA 15852', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77586, 'VA 15853', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77588, 'VA 15855', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77589, 'VA 15856', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77599, 'VA 15866', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77600, 'VA 15867', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77604, 'VA 15871', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77605, 'VA 15872', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77610, 'VA 15877', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (77666, 'VA 15934', 'price_in_text_not_extracted', 'Prix de vente: Frs 0'),
    (114094, '2026/9282/0', 'price_malformed_in_source', '100''00.');

-- pre-image of every row this migration touches (idempotent)
INSERT INTO backup.fao_transactions_price_backfill_20260913 (id, affaire_number, price, type_clean_list, updated_at, backed_up_at)
SELECT f.id, f.affaire_number, f.price, f.type_clean_list, f.updated_at, now()
FROM bronze_ch.fao_transactions f
WHERE f.id IN (SELECT id FROM _bf UNION SELECT id FROM _qt)
  AND NOT EXISTS (SELECT 1 FROM backup.fao_transactions_price_backfill_20260913 b WHERE b.id = f.id);

-- the backfill
UPDATE bronze_ch.fao_transactions f
SET price = b.new_price, updated_at = now()
FROM _bf b
WHERE f.id = b.id AND coalesce(f.price, '') = '';

-- quarantine the ambiguous ones (never guessed); traceable to the bronze row
INSERT INTO bronze_ch.fao_transactions_parse_errors (affaire_number, reason, parsed_price, raw_regex_price, raw_text, warnings)
SELECT q.affaire_number, q.reason, NULL, q.fragment, f.transaction,
       ARRAY['bronze_id=' || q.id, 'source=backfill_2026-09-13']
FROM _qt q JOIN bronze_ch.fao_transactions f ON f.id = q.id
WHERE NOT EXISTS (SELECT 1 FROM bronze_ch.fao_transactions_parse_errors e
                  WHERE e.warnings @> ARRAY['bronze_id=' || q.id]);

COMMIT;

-- ROLLBACK (restores the pre-image price; parse_errors rows are additive records):
-- BEGIN;
--   UPDATE bronze_ch.fao_transactions f SET price = b.price, updated_at = b.updated_at
--   FROM backup.fao_transactions_price_backfill_20260913 b WHERE f.id = b.id;
--   UPDATE bronze_ch.fao_transactions_parse_errors SET reason = reason || ':rolled_back'
--   WHERE warnings @> ARRAY['source=backfill_2026-09-13'];
-- COMMIT;
