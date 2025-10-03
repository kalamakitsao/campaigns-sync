{%- set age_indexes = patient_age_indexes() -%}

{% set custom_fields %}
  {{ patient_age_columns() }},

  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'_id','')                                                         AS contact_uuid,
  TRIM(NULLIF(couchdb.doc->'fields'->>'patient_name',''))                                                               AS patient_name,
  NULLIF(couchdb.doc->'fields'->>'patient_id','')                                                                       AS patient_id,
  COALESCE(NULLIF(couchdb.doc->'fields'->>'patient_gender',''), NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'sex','')) AS sex,
  COALESCE(NULLIF(couchdb.doc->'fields'->>'patient_date_of_birth',''), NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'date_of_birth','')) AS patient_date_of_birth,

  NULLIF(couchdb.doc->'fields'->>'county_id','')                                                                         AS county_id,
  COALESCE(NULLIF(couchdb.doc->'fields'->>'chw_area_id',''), NULLIF(couchdb.doc->'fields'->>'chp_area_id',''))          AS chp_area_id_form,

  NULLIF(couchdb.doc->'fields'->>'over_one','')                                                                          AS over_one,
  NULLIF(couchdb.doc->'fields'->>'over_ten','')                                                                          AS over_ten,
  NULLIF(couchdb.doc->'fields'->>'over_eighteen','')                                                                     AS over_eighteen,

  NULLIF(couchdb.doc->'fields'->>'needs_signoff','')::boolean                                                            AS needs_signoff,
  NULLIF(couchdb.doc->'fields'->>'symptoms_referral','')::boolean                                                        AS symptoms_referral,
  NULLIF(couchdb.doc->'fields'->>'snake_bite_referral','')::boolean                                                      AS snake_bite_referral,
  NULLIF(couchdb.doc->'fields'->>'snake_bite_symptoms','')::boolean                                                      AS snake_bite_symptoms,
  NULLIF(couchdb.doc->'fields'->>'lf_symptoms','')::boolean                                                              AS lf_symptoms,
  NULLIF(couchdb.doc->'fields'->>'has_hydrocele_symptoms','')::boolean                                                   AS has_hydrocele_symptoms,
  NULLIF(couchdb.doc->'fields'->>'swollen_breasts_symptoms','')::boolean                                                 AS swollen_breasts_symptoms,
  NULLIF(couchdb.doc->'fields'->>'has_cl_symptoms','')::boolean                                                          AS has_cl_symptoms,
  NULLIF(couchdb.doc->'fields'->>'has_vl_symptoms','')::boolean                                                          AS has_vl_symptoms,
  NULLIF(couchdb.doc->'fields'->>'schisto_symptoms','')::boolean                                                         AS schisto_symptoms,
  NULLIF(couchdb.doc->'fields'->>'marked_pregnant','')::boolean                                                          AS marked_pregnant,

  (lower(NULLIF(couchdb.doc->'fields'->>'has_bloody_urine','')) in ('1','true','yes','t','y'))                           AS has_bloody_urine,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_genital_ulcers','')) in ('1','true','yes','t','y'))                         AS has_genital_ulcers,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_snakebite_wound','')) in ('1','true','yes','t','y'))                        AS has_snakebite_wound,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_involuntary_urination','')) in ('1','true','yes','t','y'))                  AS has_involuntary_urination,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_swelling_of_the_scrotum','')) in ('1','true','yes','t','y'))                AS has_swelling_of_the_scrotum,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_snakebite_wound_with_pus','')) in ('1','true','yes','t','y'))               AS has_snakebite_wound_with_pus,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_vaginal_bloody_discharge','')) in ('1','true','yes','t','y'))               AS has_vaginal_bloody_discharge,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_mouth_or_nose_sore_or_ulcer','')) in ('1','true','yes','t','y'))            AS has_mouth_or_nose_sore_or_ulcer,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_scarring_due_to_skin_ulcers','')) in ('1','true','yes','t','y'))            AS has_scarring_due_to_skin_ulcers,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_swelling_of_one_or_both_arms','')) in ('1','true','yes','t','y'))           AS has_swelling_of_one_or_both_arms,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_swelling_of_one_or_both_legs','')) in ('1','true','yes','t','y'))           AS has_swelling_of_one_or_both_legs,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_swelling_of_one_or_both_breasts','')) in ('1','true','yes','t','y'))        AS has_swelling_of_one_or_both_breasts,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_snakebite_wound_with_a_bad_smell','')) in ('1','true','yes','t','y'))       AS has_snakebite_wound_with_a_bad_smell,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_blisters_around_the_snakebite_area','')) in ('1','true','yes','t','y'))     AS has_blisters_around_the_snakebite_area,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_genital_itching_or_burning_sensation','')) in ('1','true','yes','t','y'))   AS has_genital_itching_or_burning_sensation,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_weight_loss_with_distended_abdomen_vl','')) in ('1','true','yes','t','y'))  AS has_weight_loss_with_distended_abdomen_vl,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_snakebite_wound_with_a_darkened_colour','')) in ('1','true','yes','t','y','17')) AS has_snakebite_wound_with_a_darkened_colour,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_weight_loss_with_distended_abdomen_sch','')) in ('1','true','yes','t','y')) AS has_weight_loss_with_distended_abdomen_sch,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_skin_lesions_on_exposed_parts_of_the_body','')) in ('1','true','yes','t','y')) AS has_skin_lesions_on_exposed_parts_of_the_body,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_papule_nodule_bump_or_lump_on_any_part_of_the_body','')) in ('1','true','yes','t','y')) AS has_papule_nodule_bump_or_lump_on_any_part_of_the_body,
  (lower(NULLIF(couchdb.doc->'fields'->>'has_snakebite_wound_with_swelling_around_the_bitten_area','')) in ('1','true','yes','t','y')) AS has_snakebite_wound_with_swelling_around_the_bitten_area,

  NULLIF(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_lf','')                                                  AS ntd_symptoms_lf,
  NULLIF(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','')                                                 AS ntd_symptoms_sch,
  NULLIF(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_cut_leish','')                                           AS ntd_symptoms_cut_leish,
  NULLIF(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_vite_leish','')                                          AS ntd_symptoms_vite_leish,
  NULLIF(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','')                                           AS ntd_symptoms_snakebite,

  NULLIF(couchdb.doc->'fields'->>'is_lf_endemic_county','')::boolean                                                      AS is_lf_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_sch_endemic_county','')::boolean                                                     AS is_sch_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_hydrocele_endemic_county','')::boolean                                               AS is_hydrocele_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_sb_endemic_county','')::boolean                                                      AS is_sb_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_vl_endemic_county','')::boolean                                                      AS is_vl_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_cl_endemic_county','')::boolean                                                      AS is_cl_endemic_county,
  NULLIF(couchdb.doc->'fields'->>'is_leish_bilharzia_endemic_county','')::boolean                                         AS is_leish_bilharzia_endemic_county,

  /* requested fields + resilient paths */
  (lower(NULLIF(COALESCE(
      couchdb.doc->'fields'->>'has_post_kalazar',
      couchdb.doc->'fields'->'ntd_screening'->>'has_post_kalazar'
  ),'')) in ('1','true','yes','t','y'))                                                                                  AS has_post_kalazar,

  COALESCE(
    couchdb.doc->'fields'->'hydrocele_follow_up'->>'patient_condition_hydro',
    couchdb.doc->'fields'->'hydro_follow_up'->>'patient_condition_hydro',
    couchdb.doc->'fields'->>'patient_condition_hydro'
  )                                                                                                                       AS patient_condition_hydro,

  COALESCE(
    couchdb.doc->'fields'->'lymphoedema_follow_up'->>'patient_condition_lympho',
    couchdb.doc->'fields'->'lympho_follow_up'->>'patient_condition_lympho',
    couchdb.doc->'fields'->>'patient_condition_lympho'
  )                                                                                                                       AS patient_condition_lympho,

  (lower(NULLIF(COALESCE(
      couchdb.doc->'fields'->'lymphoedema_follow_up'->>'lympho_has_wounds_on_affected_area',
      couchdb.doc->'fields'->>'lympho_has_wounds_on_affected_area'
  ),'')) in ('1','true','yes','t','y'))                                                                                   AS lympho_has_wounds_on_affected_area,

  COALESCE(
    couchdb.doc->'fields'->'sb_screening_follow_up'->>'patient_condition_sb',
    couchdb.doc->'fields'->'snakebite_follow_up'->>'patient_condition_sb',
    couchdb.doc->'fields'->>'patient_condition_sb'
  )                                                                                                                       AS patient_condition_sb,

  COALESCE(
    couchdb.doc->'fields'->'sch_screening_follow_up'->>'patient_condition_sch',
    couchdb.doc->'fields'->'sch_follow_up'->>'patient_condition_sch',
    couchdb.doc->'fields'->>'patient_condition_sch'
  )                                                                                                                       AS patient_condition_sch,

  NULLIF(couchdb.doc->'fields'->'sb_screening_follow_up'->>'patient_condition_sb','')                                     AS sb_patient_condition,
  NULLIF(couchdb.doc->'fields'->'sb_screening_follow_up'->>'note_wound_with_darkened_colour','')                          AS sb_note_wound_with_darkened_colour,

  NULLIF(couchdb.doc->'fields'->'inputs'->>'source','')                                                                   AS input_source,
  NULLIF(couchdb.doc->'fields'->'inputs'->>'source_id','')                                                                AS input_source_id,

  TRIM(NULLIF(couchdb.doc->>'from',''))                                                                                   AS reporter_phone,
  NULLIF(couchdb.doc->'fields'->'meta'->>'instanceID','')                                                                 AS instance_id,

  (couchdb.doc->'geolocation'->>'latitude')::float8                                                                       AS latitude,
  (couchdb.doc->'geolocation'->>'longitude')::float8                                                                      AS longitude,
  (couchdb.doc->'geolocation'->>'accuracy')::float8                                                                       AS geo_accuracy_m
{% endset %}

{{ cht_form_model('ntd_service_subsequent', custom_fields, age_indexes) }}