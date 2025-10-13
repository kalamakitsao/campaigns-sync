{{ config(
    materialized = 'table',
    unique_key = ['chp_area_id', 'cycle'],
    on_schema_change = 'append_new_columns',
    tags = ['smc', 'campaigns']
) }}

WITH campaign_cycles AS (
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

-- Tag each SMC form with the cycle it falls in
campaigns_with_cycle AS (
  SELECT
    c.*,
    cd.cycle_name
  FROM {{ ref('campaign_service_smc') }} c
  JOIN campaign_cycles cd 
    ON c.reported::date BETWEEN cd.start_date::date AND cd.end_date::date
),

-- Keep only the latest form per (cycle_name, patient_id)
latest_cwc AS (
  SELECT *
  FROM (
    SELECT
      cwc.*,
      ROW_NUMBER() OVER (
        PARTITION BY cwc.cycle_name, cwc.patient_id
        ORDER BY cwc.reported DESC, cwc.uuid DESC
      ) AS rn
    FROM campaigns_with_cycle cwc
    WHERE cwc.patient_id IS NOT NULL
  ) t
  WHERE rn = 1
),

chp_hierarchy AS (
  SELECT DISTINCT
    ch.county AS county_name,
    ch.sub_county AS sub_county_name,
    ch.community_unit AS chu_name,
    ch.chp_area_id,
    ch.chp_area AS chp_area_name
  FROM {{ ref('mv_location_hierarchy') }} ch
  JOIN target_counties tc ON ch.county = tc.target_county
),

-- Use only deduped forms downstream
campaigns AS (
  SELECT
    chp.chp_area_id,
    chp.county_name,
    chp.sub_county_name,
    chp.chu_name AS community_health_unit_name,
    chp.chp_area_name,
    cwc.cycle_name AS cycle,
    cwc.reported::date AS campaign_date,
    p.uuid,
    cwc.reported_by,
    cwc.patient_id,
    p.sex,
    cwc.patient_age_in_months,
    cwc.smc_treatment_given,
    cwc.redose,
    cwc.fever_status,
    cwc.smc_danger_signs,
    cwc.caregiver_consent,
    cwc.spaq_allergy,
    cwc.taken_al,
    cwc.cotrimazole,
    COALESCE(cwc.calc_pink_spaq::int, 0)::int  AS pink_spaq,
    COALESCE(cwc.calc_green_spaq::int, 0)::int AS green_spaq
  FROM latest_cwc cwc
  JOIN {{ ref('patient_f_client') }} p ON p.uuid = cwc.patient_id
  JOIN {{ ref('household') }} hh ON p.household_id = hh.uuid
  JOIN chp_hierarchy chp ON hh.chv_area_id = chp.chp_area_id
)

SELECT
  county_name,
  sub_county_name,
  community_health_unit_name,
  chp_area_name,
  chp_area_id,
  cycle,

  COUNT(DISTINCT reported_by) AS chps_reporting,
  COUNT(uuid) AS campaign_forms_submitted,
  -- Safe to switch to COUNT(patient_id) if you want; dedupe assures uniqueness per cycle
  COUNT(patient_id) AS children_reached,

  -- Reach
  SUM(CASE WHEN sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS reach_f_3_11m,
  SUM(CASE WHEN sex = 'male'   AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS reach_m_3_11m,
  SUM(CASE WHEN sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS reach_f_12_59m,
  SUM(CASE WHEN sex = 'male'   AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS reach_m_12_59m,

  -- Treatment
  SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' THEN 1 ELSE 0 END) AS female_children_treated,
  SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS treated_f_3_11,
  SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'male'   AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS treated_m_3_11,
  SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS treated_f_12_59,
  SUM(CASE WHEN smc_treatment_given = 'yes' AND sex = 'male'   AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS treated_m_12_59,

  -- Redose
  SUM(CASE WHEN redose = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS redosed_f_3_11m,
  SUM(CASE WHEN redose = 'yes' AND sex = 'male'   AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS redosed_m_3_11m,
  SUM(CASE WHEN redose = 'yes' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS redosed_f_12_59m,
  SUM(CASE WHEN redose = 'yes' AND sex = 'male'   AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS redosed_m_12_59m,

  -- Referral
  SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS referred_f_3_11m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'male'   AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS referred_m_3_11m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS referred_f_12_59m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND (fever_status = 'yes' OR smc_danger_signs = 'yes') AND sex = 'male'   AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS referred_m_12_59m,

  -- Exclusion
  SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'female' AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS excluded_f_3_11m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'male'   AND patient_age_in_months BETWEEN 3 AND 11 THEN 1 ELSE 0 END) AS excluded_m_3_11m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'female' AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS excluded_f_12_59m,
  SUM(CASE WHEN smc_treatment_given = 'no' AND sex = 'male'   AND patient_age_in_months BETWEEN 12 AND 59 THEN 1 ELSE 0 END) AS excluded_m_12_59m,

  -- Exclusion reasons
  SUM(CASE WHEN smc_treatment_given = 'no' AND caregiver_consent = 'no' THEN 1 ELSE 0 END) AS no_treatment_no_consent,
  SUM(CASE WHEN smc_treatment_given = 'no' AND smc_danger_signs  = 'yes' THEN 1 ELSE 0 END) AS no_treatment_danger_signs,
  SUM(CASE WHEN smc_treatment_given = 'no' AND spaq_allergy      = 'yes' THEN 1 ELSE 0 END) AS no_treatment_spaq_allergy,
  SUM(CASE WHEN smc_treatment_given = 'no' AND taken_al          = 'yes' THEN 1 ELSE 0 END) AS no_treatment_taken_al,
  SUM(CASE WHEN smc_treatment_given = 'no' AND fever_status      = 'yes' THEN 1 ELSE 0 END) AS no_treatment_fever,
  SUM(CASE WHEN smc_treatment_given = 'no' AND cotrimazole       = 'yes' THEN 1 ELSE 0 END) AS no_treatment_cotrimazole,

  -- Blister pack usage
  SUM(pink_spaq)  AS pink_blister_packs_used,
  SUM(green_spaq) AS green_blister_packs_used,

  -- Coverage metrics
  COUNT(patient_id)::float / NULLIF(COUNT(DISTINCT reported_by), 0) AS avg_children_per_chp,
  SUM(CASE WHEN smc_treatment_given = 'yes' THEN 1 ELSE 0 END)::float / NULLIF(COUNT(patient_id), 0) AS treatment_coverage_rate

FROM campaigns
GROUP BY 
  county_name, 
  sub_county_name,
  community_health_unit_name,
  cycle,
  chp_area_name,
  chp_area_id