CREATE MATERIALIZED VIEW gold_ch.core_buildings AS
SELECT b.egid,
       b.egrid,
       b.canton_code,
       b.canton_name,
       b.commune_bfs,
       b.commune_name,
       b.category_code,
       b.category_label,
       b.class_code,
       b.class_label,
       b.status_code,
       b.status_label,
       b.construction_year,
       -- NEW: period-derived estimate (always populated when period code exists)
       
  CASE bb.gbaup
    WHEN $1 THEN $2  -- avant 1919 (open-ended; conservative midpoint)
    WHEN $3 THEN $4  -- 1919-1945 midpoint
    WHEN $5 THEN $6  -- 1946-1960
    WHEN $7 THEN $8  -- 1961-1970
    WHEN $9 THEN $10  -- 1971-1980
    WHEN $11 THEN $12  -- 1981-1985
    WHEN $13 THEN $14  -- 1986-1990
    WHEN $15 THEN $16  -- 1991-1995
    WHEN $17 THEN $18  -- 1996-2000
    WHEN $19 THEN $20  -- 2001-2005
    WHEN $21 THEN $22  -- 2006-2010
    WHEN $23 THEN $24  -- 2011-2015
    WHEN $25 THEN $26  -- 2016+ (open-ended; conservative)
    ELSE $27::integer
  END
 AS construction_year_estimated,
       
  CASE bb.gbaup
    WHEN $28 THEN $29
    WHEN $30 THEN $31
    WHEN $32 THEN $33
    WHEN $34 THEN $35
    WHEN $36 THEN $37
    WHEN $38 THEN $39
    WHEN $40 THEN $41
    WHEN $42 THEN $43
    WHEN $44 THEN $45
    WHEN $46 THEN $47
    WHEN $48 THEN $49
    WHEN $50 THEN $51
    WHEN $52 THEN $53
    ELSE $54::text
  END
 AS construction_period_label,
       CASE
         WHEN b.construction_year IS NOT NULL THEN $55
         WHEN bb.gbaup IS NOT NULL AND (
  CASE bb.gbaup
    WHEN $56 THEN $57  -- avant 1919 (open-ended; conservative midpoint)
    WHEN $58 THEN $59  -- 1919-1945 midpoint
    WHEN $60 THEN $61  -- 1946-1960
    WHEN $62 THEN $63  -- 1961-1970
    WHEN $64 THEN $65  -- 1971-1980
    WHEN $66 THEN $67  -- 1981-1985
    WHEN $68 THEN $69  -- 1986-1990
    WHEN $70 THEN $71  -- 1991-1995
    WHEN $72 THEN $73  -- 1996-2000
    WHEN $74 THEN $75  -- 2001-2005
    WHEN $76 THEN $77  -- 2006-2010
    WHEN $78 THEN $79  -- 2011-2015
    WHEN $80 THEN $81  -- 2016+ (open-ended; conservative)
    ELSE $82::integer
  END
) IS NOT NULL THEN $83
         ELSE $84
       END AS construction_year_source,
       b.renovation_year,
       b.demolition_year,
       b.floors_above_ground,
       b.floors_below_ground,
       b.dwelling_count,
       b.footprint_m2,
       b.volume_m3,
       b.heater_type_1_label,
       b.heater_energy_1_label,
       b.heater_type_2_label,
       b.heater_energy_2_label,
       b.hot_water_type_1_label,
       b.hot_water_energy_1_label,
       b.hot_water_type_2_label,
       b.hot_water_energy_2_label,
       b.geometry AS footprint_geometry,
       b.wgs84_point,
       b.lv95_e,
       b.lv95_n,
       en.idc_last_value,
       en.idc_last_year,
       en.idc_avg_3years,
       en.idc_bracket_3years,
       en.idc_bracket_1year,
       en.minergie_standard,
       en.minergie_certificate,
       en.minergie_ebf_m2,
       en.idc_sre_m2,
       now() AS updated_at
FROM silver_ch.cadastral_buildings b
  LEFT JOIN silver_ch.cadastral_energy en ON en.egid::text = b.egid::text
  LEFT JOIN bronze_ch.bfs_rebl_buildings bb ON bb.egid::text = b.egid::text
