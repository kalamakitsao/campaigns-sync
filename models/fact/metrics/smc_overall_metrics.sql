{{ config(
    materialized = 'table',
    tags = ['smc', 'campaign', 'summary'],
    description = 'SMC campaign summary with targets, reach, treatment compliance, and per-child cycle counts'
    indexes = [
        {'columns': ['county_name']},
        {'columns': ['sub_county_name']},
        {'columns': ['community_health_unit_name']},
        {'columns': ['county_name', 'sub_county_name', 'community_health_unit_name']}
    ]
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

campaigns_summary AS (
  SELECT
    county_name,
    sub_county_name,
    community_health_unit_name,
    COUNT(DISTINCT patient_id) AS children_reached,
    COUNT(DISTINCT patient_id) FILTER (WHERE smc_treatment_given = 'yes') AS children_treated
  FROM campaigns_joined
  GROUP BY county_name, sub_county_name, community_health_unit_name
),

child_cycle_counts AS (
  SELECT
    cj.patient_id,
    COUNT(DISTINCT cj.cycle_name) AS cycle_count
  FROM campaigns_joined cj
  WHERE cj.smc_treatment_given = 'yes'
  GROUP BY cj.patient_id
),

cycle_distribution AS (
  SELECT
    cj.county_name,
    cj.sub_county_name,
    cj.community_health_unit_name,
    COUNT(DISTINCT CASE WHEN ccc.cycle_count = 1 THEN cj.patient_id END) AS cycles_1,
    COUNT(DISTINCT CASE WHEN ccc.cycle_count = 2 THEN cj.patient_id END) AS cycles_2,
    COUNT(DISTINCT CASE WHEN ccc.cycle_count = 3 THEN cj.patient_id END) AS cycles_3,
    COUNT(DISTINCT CASE WHEN ccc.cycle_count = 4 THEN cj.patient_id END) AS cycles_4,
    COUNT(DISTINCT CASE WHEN ccc.cycle_count >= 5 THEN cj.patient_id END) AS cycles_5_or_more
  FROM child_cycle_counts ccc
  JOIN campaigns_joined cj ON cj.patient_id = ccc.patient_id
  GROUP BY cj.county_name, cj.sub_county_name, cj.community_health_unit_name
)

SELECT
  t.county_name,
  t.sub_county_name,
  t.community_health_unit_name,
  t.target_children,
  s.children_reached,
  s.children_treated,
  d.cycles_1,
  d.cycles_2,
  d.cycles_3,
  d.cycles_4,
  d.cycles_5_or_more
FROM campaign_targets t
LEFT JOIN campaigns_summary s
  ON t.sub_county_name = s.sub_county_name
  AND t.community_health_unit_name = s.community_health_unit_name
LEFT JOIN cycle_distribution d
  ON t.sub_county_name = d.sub_county_name
  AND t.community_health_unit_name = d.community_health_unit_name