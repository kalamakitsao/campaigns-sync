{{ config(
    materialized = 'table',
    tags = ['ntd', 'commodities', 'campaigns'],
    indexes = [
      {'columns': ['reported_by_parent']},
      {'columns': ['county']},
      {'columns': ['sub_county']},
      {'columns': ['cycle_name']},
      {'columns': ['reported_date']}
    ]
) }}

WITH campaign_counties AS (
  SELECT
    d.cycle_name,
    d.start_date::date AS start_date,
    d.end_date::date   AS end_date,
    UNNEST(d.target_counties) AS target_county
  FROM {{ ref('ntd_campaign_dates') }} d
  {% if var('ntd_cycle', none) %}
    WHERE d.cycle_name = {{ var('ntd_cycle') | tojson }}
  {% endif %}
),

commodity_base AS (
  SELECT
    cc.cycle_name,
    mv.county,
    mv.sub_county,
    mv.community_unit,
    nc.reported_by_parent,
    nc.reported::date AS reported_date,
    nc.rs_mebendazole,
    nc.rs_albendazole,
    nc.rs_praziquantel
  FROM {{ ref('ntd_commodity_received') }} nc
  JOIN {{ ref('mv_location_hierarchy') }} mv
    ON nc.reported_by_parent = mv.chp_area_id
  JOIN campaign_counties cc
    ON LOWER(mv.county) = LOWER(cc.target_county)
   AND nc.reported::date BETWEEN cc.start_date AND cc.end_date
)

SELECT
  cycle_name,
  county,
  sub_county,
  community_unit,
  reported_by_parent,
  -- reported_date is used only to map to cycle; exclude from grouping unless you want daily rows
  SUM(COALESCE(rs_mebendazole, 0))  AS rs_mebendazole,
  SUM(COALESCE(rs_albendazole, 0))  AS rs_albendazole,
  SUM(COALESCE(rs_praziquantel, 0)) AS rs_praziquantel
FROM commodity_base
GROUP BY
  cycle_name,
  county,
  sub_county,
  community_unit,
  reported_by_parent
HAVING
    SUM(COALESCE(rs_mebendazole, 0))  > 0
 OR SUM(COALESCE(rs_albendazole, 0))  > 0
 OR SUM(COALESCE(rs_praziquantel, 0)) > 0
;