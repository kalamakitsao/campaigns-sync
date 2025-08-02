{{ config(
    materialized = 'table',
    tags = ['smc', 'campaign', 'summary'],
    description = 'SMC campaign summary by cycle with target, reached, treated, and new children'
) }}

WITH latest_year AS (
  SELECT MAX(campaign_year)::int AS year
  FROM {{ ref('smc_cycle_dates') }}
  WHERE campaign_name = 'SMC'
),

cycle_dates AS (
  SELECT *
  FROM {{ ref('smc_cycle_dates') }}
  WHERE campaign_name = 'SMC'
    AND campaign_year::int = (SELECT year FROM latest_year)
),

first_cycle_start AS (
  SELECT MIN(start_date) AS cycle_1_start
  FROM cycle_dates
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
  JOIN {{ ref('household') }} hholds ON chp.chp_area_id = hholds.chv_area_id
  JOIN {{ ref('patient_f_client') }} children ON hholds.uuid = children.household_id
  CROSS JOIN first_cycle_start fc
  WHERE chp.county = 'Turkana'
    AND chp.sub_county ILIKE '%central%'
  GROUP BY chp.county, chp.sub_county, chp.community_unit
),

campaigns_joined AS (
  SELECT
    chp.county AS county_name,
    chp.sub_county AS sub_county_name,
    chp.community_unit AS community_health_unit_name,
    cd.cycle_name,
    c.patient_id,
    c.smc_treatment_given
  FROM {{ ref('campaign_service_smc') }} c
  JOIN {{ ref('patient_f_client') }} p ON p.uuid = c.patient_id
  JOIN {{ ref('household') }} h ON p.household_id = h.uuid
  JOIN {{ ref('mv_location_hierarchy') }} chp ON chp.chp_area_id = h.chv_area_id
  JOIN cycle_dates cd
    ON c.reported::date BETWEEN cd.start_date AND cd.end_date
    AND chp.county = ANY(cd.target_counties)
),

first_treatment_cycle AS (
  SELECT
    patient_id,
    MIN(cycle_name) AS first_cycle
  FROM campaigns_joined
  WHERE smc_treatment_given = 'yes'
  GROUP BY patient_id
),

new_in_campaign AS (
  SELECT
    cj.county_name,
    cj.sub_county_name,
    cj.community_health_unit_name,
    cj.cycle_name,
    COUNT(DISTINCT cj.patient_id) AS new_in_campaign
  FROM campaigns_joined cj
  JOIN first_treatment_cycle ftc
    ON cj.patient_id = ftc.patient_id
    AND cj.cycle_name = ftc.first_cycle
  GROUP BY cj.county_name, cj.sub_county_name, cj.community_health_unit_name, cj.cycle_name
)

SELECT
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  t.target_children,
  cj.cycle_name AS cycle,
  COUNT(DISTINCT cj.patient_id) AS children_reached,
  COUNT(DISTINCT cj.patient_id) FILTER (WHERE cj.smc_treatment_given = 'yes') AS children_treated,
  COALESCE(nc.new_in_campaign, 0) AS new_in_campaign
FROM campaign_targets t
JOIN campaigns_joined cj
  ON t.sub_county_name = cj.sub_county_name
  AND t.community_health_unit_name = cj.community_health_unit_name
LEFT JOIN new_in_campaign nc
  ON cj.sub_county_name = nc.sub_county_name
  AND cj.community_health_unit_name = nc.community_health_unit_name
  AND cj.cycle_name = nc.cycle_name
GROUP BY 
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  t.target_children,
  cj.cycle_name,
  nc.new_in_campaign
ORDER BY 
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  cj.cycle_name