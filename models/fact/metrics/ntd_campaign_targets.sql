-- models/campaigns/ntd_population_by_age_buckets.sql
{{ config(
    materialized = 'incremental',
    unique_key = ['chp_area_id', 'cycle_name'],
    tags = ['ntd', 'population']
) }}

WITH campaign_cycles AS (
    SELECT 
        d.cycle_name,
        d.start_date::date  AS start_date,
        d.target_date::date AS target_date,
        UNNEST(d.target_counties) AS target_county
    FROM {{ ref('ntd_campaign_dates') }} d
    {# Optional: filter to a single cycle -> dbt run --vars 'ntd_cycle: 2025_01' #}
    {% if var('ntd_cycle', none) %}
      WHERE d.cycle_name = {{ var('ntd_cycle') | tojson }}
    {% endif %}
),

ppn_with_age AS (
    SELECT
        mv.chp_area_id,
        cc.cycle_name,
        p.*,
        (
            DATE_PART('year', COALESCE(cc.target_date, cc.start_date)) 
            - DATE_PART('year', p.date_of_birth)
        ) * 12
        +
        (
            DATE_PART('month', COALESCE(cc.target_date, cc.start_date)) 
            - DATE_PART('month', p.date_of_birth)
        ) AS age_in_months_at_campaign
    FROM {{ ref('patient_f_client') }} p
    JOIN {{ ref('household') }} hh 
      ON p.household_id = hh.uuid
    JOIN {{ ref('mv_location_hierarchy') }} mv 
      ON hh.chv_area_id = mv.chp_area_id
    JOIN campaign_cycles cc 
      ON LOWER(mv.county) = LOWER(cc.target_county)
    -- Optional: guard against missing DOB rows if needed
    -- WHERE p.date_of_birth IS NOT NULL
),

aggregated AS (
    SELECT
        chps.county         AS county_name,
        chps.sub_county     AS sub_county_name,
        chps.community_unit AS chu_name,
        chps.chp_area_id,
        chps.chp_area       AS chp_area_name,
        pwa.cycle_name,

        COUNT(pwa.uuid)                                                    AS count_total_population,
        COUNT(pwa.uuid) FILTER (WHERE pwa.sex = 'male')                    AS count_total_population_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.sex = 'female')                  AS count_total_population_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 60)  AS count_total_above_5_years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 12)  AS count_total_above_1_years,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign < 6)   AS count_total_below_6months,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign < 6 AND pwa.sex = 'male')   AS count_total_below_6months_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign < 6 AND pwa.sex = 'female') AS count_total_below_6months_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 6  AND 84) AS count_total_btn_6months_and_7years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 6  AND 84 AND pwa.sex = 'male')   AS count_total_btn_6months_and_7years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 6  AND 84 AND pwa.sex = 'female') AS count_total_btn_6months_and_7years_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 84)  AS count_total_above_7years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 84 AND pwa.sex = 'male')   AS count_total_above_7years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 84 AND pwa.sex = 'female') AS count_total_above_7years_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 12 AND 59) AS count_total_1years_to_4years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 12 AND 59 AND pwa.sex = 'male')   AS count_total_1years_to_4years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 12 AND 59 AND pwa.sex = 'female') AS count_total_1years_to_4years_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 24 AND 59) AS count_total_2years_to_5years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 24 AND 59 AND pwa.sex = 'male')   AS count_total_2years_to_5years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 24 AND 59 AND pwa.sex = 'female') AS count_total_2years_to_5years_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 60 AND 168) AS count_total_5years_to_14years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 60 AND 168 AND pwa.sex = 'male')   AS count_total_5years_to_14years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign BETWEEN 60 AND 168 AND pwa.sex = 'female') AS count_total_5years_to_14years_female,

        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 180) AS count_total_over_15_years,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 180 AND pwa.sex = 'male')   AS count_total_over_15_years_male,
        COUNT(pwa.uuid) FILTER (WHERE pwa.age_in_months_at_campaign > 180 AND pwa.sex = 'female') AS count_total_over_15_years_female
    FROM ppn_with_age pwa
    JOIN {{ ref('mv_location_hierarchy') }} chps 
      ON pwa.chp_area_id = chps.chp_area_id
    GROUP BY
        chps.county,
        chps.sub_county,
        chps.community_unit,
        chps.chp_area_id,
        chps.chp_area,
        pwa.cycle_name
)

SELECT *
FROM aggregated
