-- models/campaigns/ntd_campaign_reach.sql
-- Aggregates NTD campaign reach metrics by CHW area and joins to targets.

{{ config(
    materialized = 'table',
    unique_key = ['chp_area_uuid'],
    on_schema_change = 'append_new_columns',
    tags = ['ntd', 'campaigns'],
    indexes = [
      {'columns': ['chp_area_uuid']}
    ]
) }}

WITH campaign_reach AS (
  SELECT
    chp_area_id,cycle_name,

    -- Tablet counts
    SUM(count_praziquantel_tablets_given)                         AS count_praziquantel_tablets_given,
    SUM(count_mebendazole_tablets_given)                          AS count_mebendazole_tablets_given,
    SUM(count_albendazole_tablets_given)                          AS count_albendazole_tablets_given,

    -- Mebendazole reach (age/sex bands)
    SUM(count_total_1years_to_4years_male_treated_with_mebe)      AS count_total_1years_to_4years_male_treated_with_mebe,
    SUM(count_total_1years_to_4years_female_treated_with_mebe)    AS count_total_1years_to_4years_female_treated_with_mebe,
    SUM(count_total_5years_to_15years_male_treated_with_mebe)     AS count_total_5years_to_15years_male_treated_with_mebe,
    SUM(count_total_5years_to_15years_female_treated_with_mebe)   AS count_total_5years_to_15years_female_treated_with_mebe,
    SUM(count_total_15_plus_years_female_treated_with_mebe)       AS count_total_15_plus_years_female_treated_with_mebe,
    SUM(count_total_15_plus_years_male_treated_with_mebe)         AS count_total_15_plus_years_male_treated_with_mebe,

    -- Albendazole reach (age/sex bands)
    SUM(count_total_1years_to_4years_female_treated_with_albe)    AS count_total_1years_to_4years_female_treated_with_albe,
    SUM(count_total_1years_to_4years_male_treated_with_albe)      AS count_total_1years_to_4years_male_treated_with_albe,
    SUM(count_total_5years_to_15years_female_treated_with_albe)   AS count_total_5years_to_15years_female_treated_with_albe,
    SUM(count_total_15_plus_years_female_treated_with_albe)       AS count_total_15_plus_years_female_treated_with_albe,
    SUM(count_total_15_plus_years_male_treated_with_albe)         AS count_total_15_plus_years_male_treated_with_albe,

    -- Praziquantel reach (age/sex bands)
    SUM(count_total_2years_to_4years_male_treated_with_prazi)     AS count_total_2years_to_4years_male_treated_with_prazi,
    SUM(count_total_2years_to_4years_female_treated_with_prazi)   AS count_total_2years_to_4years_female_treated_with_prazi,
    SUM(count_total_5years_to_14years_male_treated_with_prazi)    AS count_total_5years_to_14years_male_treated_with_prazi,
    SUM(count_total_5years_to_14years_female_treated_with_prazi)  AS count_total_5years_to_14years_female_treated_with_prazi,
    SUM(count_total_15_plus_years_male_treated_with_prazi)        AS count_total_15_plus_years_male_treated_with_prazi,
    SUM(count_total_15_plus_years_female_treated_with_prazi)      AS count_total_15_plus_years_female_treated_with_prazi,

    -- Totals by drug
    SUM(count_total_treated_with_mebe)                            AS count_total_treated_with_mebe,
    SUM(count_total_treated_with_albe)                            AS count_total_treated_with_albe,
    SUM(count_total_with_prazi)                                   AS count_total_with_prazi
  FROM {{ ref('ntd_campaign_metrics') }}
  GROUP BY 1,2
)

SELECT
  nt.*,

  COALESCE(cr.count_praziquantel_tablets_given, 0)                        AS count_praziquantel_tablets_given,
  COALESCE(cr.count_mebendazole_tablets_given, 0)                         AS count_mebendazole_tablets_given,
  COALESCE(cr.count_albendazole_tablets_given, 0)                         AS count_albendazole_tablets_given,

  COALESCE(cr.count_total_1years_to_4years_male_treated_with_mebe, 0)     AS count_total_1years_to_4years_male_treated_with_mebe,
  COALESCE(cr.count_total_1years_to_4years_female_treated_with_mebe, 0)   AS count_total_1years_to_4years_female_treated_with_mebe,
  COALESCE(cr.count_total_5years_to_15years_male_treated_with_mebe, 0)    AS count_total_5years_to_15years_male_treated_with_mebe,
  COALESCE(cr.count_total_5years_to_15years_female_treated_with_mebe, 0)  AS count_total_5years_to_15years_female_treated_with_mebe,
  COALESCE(cr.count_total_15_plus_years_female_treated_with_mebe, 0)      AS count_total_15_plus_years_female_treated_with_mebe,
  COALESCE(cr.count_total_15_plus_years_male_treated_with_mebe, 0)        AS count_total_15_plus_years_male_treated_with_mebe,

  COALESCE(cr.count_total_1years_to_4years_female_treated_with_albe, 0)   AS count_total_1years_to_4years_female_treated_with_albe,
  COALESCE(cr.count_total_1years_to_4years_male_treated_with_albe, 0)     AS count_total_1years_to_4years_male_treated_with_albe,
  COALESCE(cr.count_total_5years_to_15years_female_treated_with_albe, 0)  AS count_total_5years_to_15years_female_treated_with_albe,
  COALESCE(cr.count_total_15_plus_years_female_treated_with_albe, 0)      AS count_total_15_plus_years_female_treated_with_albe,
  COALESCE(cr.count_total_15_plus_years_male_treated_with_albe, 0)        AS count_total_15_plus_years_male_treated_with_albe,

  COALESCE(cr.count_total_2years_to_4years_male_treated_with_prazi, 0)    AS count_total_2years_to_4years_male_treated_with_prazi,
  COALESCE(cr.count_total_2years_to_4years_female_treated_with_prazi, 0)  AS count_total_2years_to_4years_female_treated_with_prazi,
  COALESCE(cr.count_total_5years_to_14years_male_treated_with_prazi, 0)   AS count_total_5years_to_14years_male_treated_with_prazi,
  COALESCE(cr.count_total_5years_to_14years_female_treated_with_prazi, 0) AS count_total_5years_to_14years_female_treated_with_prazi,
  COALESCE(cr.count_total_15_plus_years_male_treated_with_prazi, 0)       AS count_total_15_plus_years_male_treated_with_prazi,
  COALESCE(cr.count_total_15_plus_years_female_treated_with_prazi, 0)     AS count_total_15_plus_years_female_treated_with_prazi,

  COALESCE(cr.count_total_treated_with_mebe, 0)                           AS count_total_treated_with_mebe,
  COALESCE(cr.count_total_treated_with_albe, 0)                           AS count_total_treated_with_albe,
  COALESCE(cr.count_total_with_prazi, 0)                                   AS count_total_with_prazi

FROM {{ ref('ntd_campaign_targets') }} nt
LEFT JOIN campaign_reach cr
  ON nt.chp_area_id = cr.chp_area_id and nt.cycle_name = cr.cycle_name;