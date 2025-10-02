{%- set age_indexes = patient_age_indexes() -%}

{% set custom_fields %}
  {{ patient_age_columns() }},

  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'_id','')                       AS contact_uuid,
  TRIM(NULLIF(couchdb.doc->'fields'->>'patient_name',''))                             AS patient_name,
  NULLIF(couchdb.doc->'fields'->>'patient_id','')                                     AS patient_id,
  COALESCE(
    NULLIF(couchdb.doc->'fields'->>'patient_gender',''),
    NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'sex','')
  )                                                                                   AS sex,
  COALESCE(
    NULLIF(couchdb.doc->'fields'->>'patient_date_of_birth',''),
    NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->>'date_of_birth','')
  )                                                                                   AS patient_date_of_birth,

  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_code','')            AS chu_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_name','')            AS chu_name,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_code','')  AS link_facility_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_name','')  AS link_facility_name,

  NULLIF(couchdb.doc->'fields'->>'chw_area_id','')                                      AS chp_area_id_form,
  NULLIF(couchdb.doc->'fields'->>'county_id','')                                        AS county_id,

  NULLIF(couchdb.doc->'fields'->>'over_one','')                                         AS over_one,
  NULLIF(couchdb.doc->'fields'->>'over_two','')                                         AS over_two,
  NULLIF(couchdb.doc->'fields'->>'months_since_reported','')::int                        AS months_since_reported,

  NULLIF(couchdb.doc->'fields'->'ntd_referral_follow_up'->>'is_available','')           AS is_available,
  NULLIF(couchdb.doc->'fields'->'ntd_referral_follow_up'->>'went_to_facility','')       AS went_to_facility,
  NULLIF(couchdb.doc->'fields'->'ntd_referral_follow_up'->>'health_status','')          AS health_status,

  NULLIF(couchdb.doc->'fields'->>'needs_signoff','')::boolean                            AS needs_signoff,
  NULLIF(couchdb.doc->'fields'->>'marked_pregnant','')::boolean                          AS marked_pregnant,

  NULLIF(couchdb.doc->'fields'->'inputs'->>'source','')                                  AS input_source,          -- e.g., 'task'
  NULLIF(couchdb.doc->'fields'->'inputs'->>'source_id','')                               AS input_source_id,       -- task uuid if present
  NULLIF(couchdb.doc->'fields'->'inputs'->>'t_reported_date','')                         AS t_reported_date,       -- ISO string from task
  NULLIF(couchdb.doc->'fields'->>'reported_date_from_parent_form','')                    AS reported_date_from_parent_form,

  TRIM(NULLIF(couchdb.doc->>'from',''))                                                  AS reporter_phone,
  NULLIF(couchdb.doc->'fields'->'meta'->>'instanceID','')                                AS instance_id,

  /* ------------ Geolocation (top-level) ------------ */
  (couchdb.doc->'geolocation'->>'latitude')::float8                                      AS latitude,
  (couchdb.doc->'geolocation'->>'longitude')::float8                                     AS longitude,
  (couchdb.doc->'geolocation'->>'accuracy')::float8                                      AS geo_accuracy_m
{% endset %}

{{ cht_form_model('ntd_referral_follow_up', custom_fields, age_indexes) }}