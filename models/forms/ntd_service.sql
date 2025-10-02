-- ntd_service form model

{%- set age_indexes = patient_age_indexes() -%}

{% set custom_fields %}
  {{ patient_age_columns() }},

  NULLIF(couchdb.doc->'fields'->>'patient_id','')                                        AS patient_id,
  TRIM(NULLIF(couchdb.doc->'fields'->>'patient_name',''))                                AS patient_name,
  COALESCE(
    NULLIF(couchdb.doc->'fields'->>'patient_gender',''),
    NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'sex','')
  )                                                                                      AS sex,
  COALESCE(
    NULLIF(couchdb.doc->'fields'->>'patient_date_of_birth',''),
    NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'date_of_birth','')
  )                                                                                      AS patient_date_of_birth,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_code','')           AS chu_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_name','')           AS chu_name,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_code','') AS link_facility_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_name','') AS link_facility_name,

  NULLIF(couchdb.doc->'fields'->>'lf_symptoms','')::boolean                                 AS lf_symptoms,
  NULLIF(couchdb.doc->'fields'->>'schisto_symptoms','')::boolean                            AS schisto_symptoms,
  NULLIF(couchdb.doc->'fields'->>'snake_bite_symptoms','')::boolean                         AS snake_bite_symptoms,
  NULLIF(couchdb.doc->'fields'->>'symptoms_referral','')::boolean                           AS symptoms_referral,
  NULLIF(couchdb.doc->'fields'->>'needs_signoff','')::boolean                               AS needs_signoff,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_lf','') || ' ') LIKE '% swelling_of_one_or_both_legs %')::boolean     AS lf_swelling_of_one_or_both_legs,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_lf','') || ' ') LIKE '% swelling_of_one_or_both_arms %')::boolean     AS lf_swelling_of_one_or_both_arms,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_lf','') || ' ') LIKE '% swelling_of_one_or_both_breasts %')::boolean  AS lf_swelling_of_one_or_both_breasts,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_lf','') || ' ') LIKE '% none %')::boolean                              AS lf_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_hydrocele','') || ' ') LIKE '% swelling_of_the_scrotum %')::boolean    AS hydrocele_swelling_of_the_scrotum,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_hydrocele','') || ' ') LIKE '% none %')::boolean                       AS hydrocele_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% weight_loss_with_enlarged_stomach_sch %')::boolean AS sch_weight_loss_with_enlarged_stomach,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% bloody_urine %')::boolean                         AS sch_bloody_urine,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% vaginal_bloody_discharge %')::boolean             AS sch_vaginal_bloody_discharge,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% genital_itching_or_burning_sensation %')::boolean AS sch_genital_itching_or_burning_sensation,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% involuntary_urination %')::boolean                AS sch_involuntary_urination,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% genital_ulcers %')::boolean                       AS sch_genital_ulcers,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_sch','') || ' ') LIKE '% none %')::boolean                                 AS sch_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_cut_leish','') || ' ') LIKE '% skin_lesions_on_exposed_parts_of_the_body %')::boolean AS cl_skin_lesions_on_exposed_parts_of_the_body,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_cut_leish','') || ' ') LIKE '% mouth_or_nose_sore_or_ulcer %')::boolean             AS cl_mouth_or_nose_sore_or_ulcer,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_cut_leish','') || ' ') LIKE '% none %')::boolean                                    AS cl_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_vite_leish','') || ' ') LIKE '% weight_loss_with_enlarged_stomach_vl %')::boolean AS vl_weight_loss_with_enlarged_stomach,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_vite_leish','') || ' ') LIKE '% none %')::boolean                                  AS vl_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% swelling_and_pain_around_bite_area %')::boolean           AS sb_swelling_and_pain_around_bite_area,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% blistering_and_bruising %')::boolean                      AS sb_blistering_and_bruising,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% bleeding_from_organs_eg_gums_or_wounds %')::boolean       AS sb_bleeding_from_organs_eg_gums_or_wounds,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% difficulty_breathing_or_swallowing %')::boolean           AS sb_difficulty_breathing_or_swallowing,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% drowsiness_dizziness_or_collapse_in_severe_cases %')::boolean AS sb_drowsiness_dizziness_or_collapse,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% inability_to_open_eyes %')::boolean                       AS sb_inability_to_open_eyes,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% suspected_snakebite_without_visible_sign %')::boolean     AS sb_suspected_snakebite_without_visible_sign,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% eye_tearing_because_of_snake_spit %')::boolean            AS sb_eye_tearing_because_of_snake_spit,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% eye_pain_because_of_snake_spit %')::boolean               AS sb_eye_pain_because_of_snake_spit,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% blurred_vision_because_of_snake_spit %')::boolean         AS sb_blurred_vision_because_of_snake_spit,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% suspected_snake_spit_in_eyes %')::boolean                 AS sb_suspected_snake_spit_in_eyes,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_snakebite','') || ' ') LIKE '% none %')::boolean                                         AS sb_none,

  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% inward_turning_of_eyelashes %')::boolean   AS trich_inward_turning_of_eyelashes,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% blurry_vision %')::boolean                 AS trich_blurry_vision,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% painful_eyes %')::boolean                  AS trich_painful_eyes,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% tearing %')::boolean                       AS trich_tearing,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% redness_of_the_eye %')::boolean            AS trich_redness_of_the_eye,
  ((' ' || COALESCE(couchdb.doc->'fields'->'ntd_screening'->>'ntd_symptoms_trichiasis','') || ' ') LIKE '% none %')::boolean                          AS trich_none,

  TRIM(NULLIF(couchdb.doc->>'from',''))                                                           AS reporter_phone,
  (couchdb.doc->'geolocation'->>'latitude')::float8                                               AS latitude,
  (couchdb.doc->'geolocation'->>'longitude')::float8                                              AS longitude,
  (couchdb.doc->'geolocation'->>'accuracy')::float8                                               AS geo_accuracy_m
{% endset %}

{{ cht_form_model('ntd_service', custom_fields, age_indexes) }}