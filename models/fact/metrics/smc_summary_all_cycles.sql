{{ config(
    materialized = 'incremental',
    unique_key = ['county_name', 'sub_county_name', 'community_health_unit_name', 'cycle'],
    on_schema_change = 'append_new_columns',
    tags = ['smc', 'campaigns', 'summary'],
    indexes = [
        {'columns': ['county_name']},
        {'columns': ['sub_county_name']},
        {'columns': ['community_health_unit_name']},
        {'columns': ['cycle']}
    ]
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
    COUNT(children.uuid) FILTER (
      WHERE
        (fc.cycle_1_start::date - children.date_of_birth::date) BETWEEN 90 AND 1850
        AND children.muted IS NULL
    ) AS target_children
  FROM {{ ref('mv_location_hierarchy') }} chp
  JOIN {{ ref('household') }} hholds
    ON chp.chp_area_id = hholds.chv_area_id
  JOIN {{ ref('patient_f_client') }} children
    ON hholds.uuid = children.household_id
  CROSS JOIN first_cycle_start fc
  GROUP BY chp.county, chp.sub_county, chp.community_unit
),

campaign_cycles AS (
  SELECT 
    scd.cycle_name AS cycle,
    scd.start_date,
    scd.end_date,
    UNNEST(scd.target_counties) AS target_county,
    UNNEST(scd.target_sub_counties) AS target_sub_county
  FROM {{ ref('smc_cycle_dates') }} scd
),

campaigns_with_cycle AS (
  SELECT
    c.*,
    cc.cycle
  FROM {{ ref('campaign_service_smc') }} c
  JOIN campaign_cycles cc 
    ON c.reported::date BETWEEN cc.start_date AND cc.end_date
),

chp_hierarchy AS (
  SELECT DISTINCT
    ch.county AS county_name,
    ch.sub_county AS sub_county_name,
    ch.community_unit AS community_health_unit_name,
    ch.chp_area_id
  FROM {{ ref('mv_location_hierarchy') }} ch
  JOIN campaign_cycles cc
    ON ch.county = cc.target_county
   AND ch.sub_county = cc.target_sub_county
),

campaigns AS (
  SELECT
    chp.county_name,
    chp.sub_county_name,
    chp.community_health_unit_name,
    cwc.cycle,
    p.uuid AS patient_id,
    cwc.smc_treatment_given
  FROM campaigns_with_cycle cwc
  JOIN {{ ref('patient_f_client') }} p
    ON p.uuid = cwc.patient_id
  JOIN {{ ref('household') }} hh
    ON p.household_id = hh.uuid
  JOIN chp_hierarchy chp
    ON hh.chv_area_id = chp.chp_area_id
)

SELECT
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  c.cycle,
  t.target_children,
  COUNT(DISTINCT c.patient_id) AS children_reached,
  SUM(CASE WHEN c.smc_treatment_given = 'yes' THEN 1 ELSE 0 END) AS children_treated
FROM campaign_targets t
JOIN campaigns c
  ON c.county_name = t.county_name
 AND c.sub_county_name = t.sub_county_name
 AND c.community_health_unit_name = t.community_health_unit_name
GROUP BY
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  c.cycle,
  t.target_children
ORDER BY
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  c.cycle;