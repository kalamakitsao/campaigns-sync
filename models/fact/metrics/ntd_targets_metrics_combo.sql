-- models/campaigns/ntd_campaign_reach_with_commodities.sql
-- Compare targets, services (reach), and commodities received per CHW area & cycle.

{{ config(
    materialized = 'table',
    unique_key = ['chp_area_id', 'cycle_name'],
    on_schema_change = 'append_new_columns',
    tags = ['ntd', 'campaigns']
) }}

with campaign_cycles as (
  select
    d.cycle_name,
    d.start_date::date as start_date,
    d.end_date::date   as end_date,
    unnest(d.target_counties) as target_county
  from {{ ref('ntd_campaign_dates') }} d
),

metrics_base as (
  select *
  from {{ ref('ntd_campaign_metrics') }}
),

targets_base as (
  select *
  from {{ ref('ntd_campaign_targets') }}
),

campaign_reach as (
  select
    chp_area_id,
    cycle_name,

    sum(coalesce(count_praziquantel_tablets_given, 0))::bigint as count_praziquantel_tablets_given,
    sum(coalesce(count_mebendazole_tablets_given, 0))::bigint  as count_mebendazole_tablets_given,
    sum(coalesce(count_albendazole_tablets_given, 0))::bigint  as count_albendazole_tablets_given,

    sum(coalesce(count_total_1years_to_4years_male_treated_with_mebe, 0))::bigint    as count_total_1years_to_4years_male_treated_with_mebe,
    sum(coalesce(count_total_1years_to_4years_female_treated_with_mebe, 0))::bigint  as count_total_1years_to_4years_female_treated_with_mebe,
    sum(coalesce(count_total_5years_to_15years_male_treated_with_mebe, 0))::bigint   as count_total_5years_to_15years_male_treated_with_mebe,
    sum(coalesce(count_total_5years_to_15years_female_treated_with_mebe, 0))::bigint as count_total_5years_to_15years_female_treated_with_mebe,
    sum(coalesce(count_total_15_plus_years_female_treated_with_mebe, 0))::bigint     as count_total_15_plus_years_female_treated_with_mebe,
    sum(coalesce(count_total_15_plus_years_male_treated_with_mebe, 0))::bigint       as count_total_15_plus_years_male_treated_with_mebe,

    sum(coalesce(count_total_1years_to_4years_female_treated_with_albe, 0))::bigint  as count_total_1years_to_4years_female_treated_with_albe,
    sum(coalesce(count_total_1years_to_4years_male_treated_with_albe, 0))::bigint    as count_total_1years_to_4years_male_treated_with_albe,
    sum(coalesce(count_total_5years_to_15years_male_treated_with_albe, 0))::bigint   as count_total_5years_to_15years_male_treated_with_albe,
    sum(coalesce(count_total_5years_to_15years_female_treated_with_albe, 0))::bigint as count_total_5years_to_15years_female_treated_with_albe,
    sum(coalesce(count_total_15_plus_years_female_treated_with_albe, 0))::bigint     as count_total_15_plus_years_female_treated_with_albe,
    sum(coalesce(count_total_15_plus_years_male_treated_with_albe, 0))::bigint       as count_total_15_plus_years_male_treated_with_albe,

    sum(coalesce(count_total_2years_to_4years_male_treated_with_prazi, 0))::bigint    as count_total_2years_to_4years_male_treated_with_prazi,
    sum(coalesce(count_total_2years_to_4years_female_treated_with_prazi, 0))::bigint  as count_total_2years_to_4years_female_treated_with_prazi,
    sum(coalesce(count_total_5years_to_14years_male_treated_with_prazi, 0))::bigint   as count_total_5years_to_14years_male_treated_with_prazi,
    sum(coalesce(count_total_5years_to_14years_female_treated_with_prazi, 0))::bigint as count_total_5years_to_14years_female_treated_with_prazi,
    sum(coalesce(count_total_15_plus_years_male_treated_with_prazi, 0))::bigint       as count_total_15_plus_years_male_treated_with_prazi,
    sum(coalesce(count_total_15_plus_years_female_treated_with_prazi, 0))::bigint     as count_total_15_plus_years_female_treated_with_prazi,

    sum(coalesce(count_total_treated_with_mebe, 0))::bigint  as count_total_treated_with_mebe,
    sum(coalesce(count_total_treated_with_albe, 0))::bigint  as count_total_treated_with_albe,
    sum(coalesce(count_total_with_prazi, 0))::bigint         as count_total_with_prazi
  from metrics_base
  group by 1,2
),

commodities_base as (
  select
    mv.chp_area_id,
    cc.cycle_name,

    sum(coalesce(nc.rs_mebendazole, 0))::bigint   as rs_mebendazole_received,
    sum(coalesce(nc.rs_albendazole, 0))::bigint   as rs_albendazole_received,
    sum(coalesce(nc.rs_praziquantel, 0))::bigint  as rs_praziquantel_received
  from {{ ref('ntd_commodity_received') }} nc
  join {{ ref('mv_location_hierarchy') }} mv
    on nc.reported_by_parent = mv.chp_area_id
  join campaign_cycles cc
    on lower(mv.county) = lower(cc.target_county)
   and nc.reported::date between cc.start_date and cc.end_date 
  group by 1,2
)

select
  nt.*,

  coalesce(cr.count_praziquantel_tablets_given, 0) as count_praziquantel_tablets_given,
  coalesce(cr.count_mebendazole_tablets_given, 0)  as count_mebendazole_tablets_given,
  coalesce(cr.count_albendazole_tablets_given, 0)  as count_albendazole_tablets_given,

  coalesce(cr.count_total_1years_to_4years_male_treated_with_mebe, 0)    as count_total_1years_to_4years_male_treated_with_mebe,
  coalesce(cr.count_total_1years_to_4years_female_treated_with_mebe, 0)  as count_total_1years_to_4years_female_treated_with_mebe,
  coalesce(cr.count_total_5years_to_15years_male_treated_with_mebe, 0)   as count_total_5years_to_15years_male_treated_with_mebe,
  coalesce(cr.count_total_5years_to_15years_female_treated_with_mebe, 0) as count_total_5years_to_15years_female_treated_with_mebe,
  coalesce(cr.count_total_15_plus_years_female_treated_with_mebe, 0)     as count_total_15_plus_years_female_treated_with_mebe,
  coalesce(cr.count_total_15_plus_years_male_treated_with_mebe, 0)       as count_total_15_plus_years_male_treated_with_mebe,

  coalesce(cr.count_total_1years_to_4years_female_treated_with_albe, 0)  as count_total_1years_to_4years_female_treated_with_albe,
  coalesce(cr.count_total_1years_to_4years_male_treated_with_albe, 0)    as count_total_1years_to_4years_male_treated_with_albe,
  coalesce(cr.count_total_5years_to_15years_male_treated_with_albe, 0)   as count_total_5years_to_15years_male_treated_with_albe,
  coalesce(cr.count_total_5years_to_15years_female_treated_with_albe, 0) as count_total_5years_to_15years_female_treated_with_albe,
  coalesce(cr.count_total_15_plus_years_female_treated_with_albe, 0)     as count_total_15_plus_years_female_treated_with_albe,
  coalesce(cr.count_total_15_plus_years_male_treated_with_albe, 0)       as count_total_15_plus_years_male_treated_with_albe,

  coalesce(cr.count_total_2years_to_4years_male_treated_with_prazi, 0)   as count_total_2years_to_4years_male_treated_with_prazi,
  coalesce(cr.count_total_2years_to_4years_female_treated_with_prazi, 0) as count_total_2years_to_4years_female_treated_with_prazi,
  coalesce(cr.count_total_5years_to_14years_male_treated_with_prazi, 0)  as count_total_5years_to_14years_male_treated_with_prazi,
  coalesce(cr.count_total_5years_to_14years_female_treated_with_prazi, 0)as count_total_5years_to_14years_female_treated_with_prazi,
  coalesce(cr.count_total_15_plus_years_male_treated_with_prazi, 0)      as count_total_15_plus_years_male_treated_with_prazi,
  coalesce(cr.count_total_15_plus_years_female_treated_with_prazi, 0)    as count_total_15_plus_years_female_treated_with_prazi,

  coalesce(cr.count_total_treated_with_mebe, 0) as count_total_treated_with_mebe,
  coalesce(cr.count_total_treated_with_albe, 0) as count_total_treated_with_albe,
  coalesce(cr.count_total_with_prazi, 0)        as count_total_with_prazi,

  coalesce(cb.rs_mebendazole_received, 0)  as rs_mebendazole_received,
  coalesce(cb.rs_albendazole_received, 0)  as rs_albendazole_received,
  coalesce(cb.rs_praziquantel_received, 0) as rs_praziquantel_received

from targets_base nt
left join campaign_reach cr
       on nt.chp_area_id = cr.chp_area_id
      and nt.cycle_name  = cr.cycle_name
left join commodities_base cb
       on nt.chp_area_id = cb.chp_area_id
      and nt.cycle_name  = cb.cycle_name