{{ config(
    materialized = 'table',
    tags = ['ntd', 'commodities', 'campaigns'],
    indexes = [
      {'columns': ['reported_by_parent']},
      {'columns': ['county']},
      {'columns': ['sub_county']}
    ]
) }}

WITH campaign_counties AS (
  SELECT
    d.cycle_name,
    d.start_date,
    d.end_date,
    UNNEST(d.target_counties) AS target_county
  FROM {{ ref('ntd_campaign_dates') }} d
  --{# Optional cycle filter: pass with --vars 'ntd_cycle: 2025_01' #}
  -- {% if var('ntd_cycle', none) %}
    --WHERE d.cycle_name = {{ var('ntd_cycle') | tojson }}
  -- {% endif %}
),

commodity_base AS (
  SELECT
    mv.county,
    mv.sub_county,
    mv.community_unit,
    nc.reported_by_parent,
    nc.rs_mebendazole,
    nc.rs_albendazole,
    nc.rs_praziquantel
  FROM {{ ref('ntd_commodity_received') }} nc
  JOIN {{ ref('mv_location_hierarchy') }} mv
    ON nc.reported_by_parent = mv.chp_area_id
  -- keep only rows where county is in targeted counties for the (optionally filtered) cycles
  JOIN campaign_counties cc
    ON LOWER(mv.county) = LOWER(cc.target_county)
)

SELECT
  county,
  sub_county,
  community_unit,
  reported_by_parent,
  SUM(COALESCE(rs_mebendazole, 0))  AS rs_mebendazole,
  SUM(COALESCE(rs_albendazole, 0))  AS rs_albendazole,
  SUM(COALESCE(rs_praziquantel, 0)) AS rs_praziquantel
FROM commodity_base
GROUP BY
  county,
  sub_county,
  community_unit,
  reported_by_parent
HAVING
    SUM(COALESCE(rs_mebendazole, 0))  > 0
 OR SUM(COALESCE(rs_albendazole, 0))  > 0
 OR SUM(COALESCE(rs_praziquantel, 0)) > 0
;