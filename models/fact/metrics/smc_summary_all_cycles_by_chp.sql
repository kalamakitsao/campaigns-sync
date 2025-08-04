-- models/campaigns/campaign_summary_smc.sql

{{ config(
    materialized = 'incremental',
    unique_key = ['chp_area_id', 'cycle'],
    on_schema_change = 'append_new_columns',
    tags = ['smc', 'campaigns']
) }}

WITH first_cycle_start AS (
  SELECT MIN(start_date) AS cycle_1_start
  FROM {{ ref('smc_cycle_dates') }}
),

campaign_targets AS (
  SELECT
    chp.county AS county_name,
    chp.sub_county AS sub_county_name,
    chp.community_unit AS community_health_unit_name,
    chp.chp_area AS chp_area_name,
    chp.chp_area_id,
    COUNT(children.uuid) FILTER (
      WHERE
        (fc.cycle_1_start::date - children.date_of_birth::date) BETWEEN 90 AND 1850
        AND children.muted IS NULL
    ) AS target_children
  FROM {{ ref('mv_location_hierarchy') }} chp
  JOIN {{ ref('household') }} hholds ON chp.chp_area_id = hholds.chv_area_id
  JOIN {{ ref('patient_f_client') }} children ON hholds.uuid = children.household_id
  CROSS JOIN first_cycle_start fc
  WHERE chp.county = 'Turkana'
    AND chp.sub_county ILIKE '%central%'
  GROUP BY chp.county, chp.sub_county, chp.community_unit, chp.chp_area, chp.chp_area_id
),

campaign_cycles AS (
  SELECT 
    scd.cycle_name,
    scd.start_date,
    scd.end_date,
    UNNEST(scd.target_counties) AS target_county
  FROM {{ ref('smc_cycle_dates') }} scd
),

target_counties AS (
  SELECT DISTINCT target_county FROM campaign_cycles
),

campaigns_with_cycle AS (
  SELECT
    c.*,
    cd.cycle_name
  FROM {{ ref('campaign_service_smc') }} c
  JOIN campaign_cycles cd 
    ON c.reported::date BETWEEN cd.start_date AND cd.end_date
),

chp_hierarchy AS (
  SELECT DISTINCT
    ch.county AS county_name,
    ch.sub_county AS sub_county_name,
    ch.community_unit AS community_health_unit_name,
    ch.chp_area_id,
    ch.chp_area AS chp_area_name
  FROM {{ ref('mv_location_hierarchy') }} ch
  JOIN target_counties tc ON ch.county = tc.target_county
),

campaigns AS (
  SELECT
    chp.county_name,
    chp.sub_county_name,
    chp.community_health_unit_name,
    chp.chp_area_name AS chp,
    chp.chp_area_id,
    cwc.cycle_name AS cycle,
    p.uuid AS patient_id,
    cwc.smc_treatment_given,
    cwc.redose,
    COALESCE(cwc.calc_pink_spaq::int, 0)  AS pink_spaq,
    COALESCE(cwc.calc_green_spaq::int, 0) AS green_spaq
  FROM campaigns_with_cycle cwc
  JOIN {{ ref('patient_f_client') }} p ON p.uuid = cwc.patient_id
  JOIN {{ ref('household') }} hh ON p.household_id = hh.uuid
  JOIN chp_hierarchy chp ON hh.chv_area_id = chp.chp_area_id
)

SELECT
  c.county_name,
  c.sub_county_name,
  c.community_health_unit_name,
  c.chp,
  c.chp_area_id,
  c.cycle,
  COALESCE(t.target_children, 0) AS target_children,
  COUNT(DISTINCT c.patient_id) AS children_reached,
  SUM(CASE WHEN smc_treatment_given = 'yes' THEN 1 ELSE 0 END) AS children_treated,
  SUM(CASE WHEN smc_treatment_given = 'no' THEN 1 ELSE 0 END) AS children_excluded,
  SUM(CASE WHEN redose = 'yes' THEN 1 ELSE 0 END) AS children_redosed,
  SUM(pink_spaq)  AS pink_spaq_issued,
  SUM(green_spaq) AS green_spaq_issued
FROM campaigns c
LEFT JOIN campaign_targets t
  ON c.chp_area_id = t.chp_area_id
GROUP BY
  c.county_name,
  c.sub_county_name,
  c.community_health_unit_name,
  c.chp,
  c.chp_area_id,
  c.cycle,
  t.target_children
ORDER BY
  c.county_name,
  c.sub_county_name,
  c.community_health_unit_name;