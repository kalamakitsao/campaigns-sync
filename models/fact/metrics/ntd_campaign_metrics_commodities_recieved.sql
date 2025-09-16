-- models/campaigns/ntd_campaign_metrics_commodities_recieved.sql
{{ config(
    materialized = 'table',
    unique_key = ['cycle_name', 'chp_area_id'],
    tags = ['ntd', 'commodities', 'campaigns']
) }}

WITH campaign_counties AS (
  SELECT
    d.cycle_name,
    d.start_date::date AS start_date,
    d.end_date::date   AS end_date,
    UNNEST(d.target_counties) AS target_county
  FROM {{ ref('ntd_campaign_dates') }} d
),

commodity_base AS (
  SELECT
    cc.cycle_name,
    mv.county,
    mv.sub_county,
    mv.community_unit,
    nc.reported_by_parent AS chp_area_id,
    nc.reported::date     AS reported_date,
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
  chp_area_id,
  SUM(COALESCE(rs_mebendazole,  0))::bigint AS rs_mebendazole,
  SUM(COALESCE(rs_albendazole,  0))::bigint AS rs_albendazole,
  SUM(COALESCE(rs_praziquantel, 0))::bigint AS rs_praziquantel
FROM commodity_base
GROUP BY
  cycle_name,
  county,
  sub_county,
  community_unit,
  chp_area_id