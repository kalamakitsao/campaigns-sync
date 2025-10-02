{%- set age_indexes = patient_age_indexes() -%}

{% set custom_fields %}
  {{ patient_age_columns() }},

  /* -------- Person / contact -------- */
  NULLIF(couchdb.doc->'fields'->>'patient_id','')                                   AS patient_id,

  /* -------- CHU / Facility context (from nested parent->parent->contact) -------- */
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_code','')           AS chu_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'chu_name','')           AS chu_name,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_code','') AS link_facility_code,
  NULLIF(couchdb.doc->'fields'->'inputs'->'contact'->'parent'->'parent'->'contact'->>'link_facility_name','') AS link_facility_name,

  /* -------- First aid type patient condition & commodities -------- */
  NULLIF(couchdb.doc->'fields'->'type_of_first_aid'->>'first_aid_emergencies','')   AS first_aid_emergencies,
  NULLIF(couchdb.doc->'fields'->'first_aider_instructions'->>'unresponsive_not_breathing_no_pulse','') AS unresponsive_not_breathing_no_pulse,
  NULLIF(couchdb.doc->'fields'->'bleeding_instructions'->>'has_bleeding_stopped','') AS has_bleeding_stopped,
  NULLIF(couchdb.doc->'fields'->'commodities_used'->>'used_commodities','')         AS used_commodities,
  NULLIF(couchdb.doc->'fields'->'commodities_used'->>'used_paracetamol_bottles','')         AS used_paracetamol_bottles,
  NULLIF(couchdb.doc->'fields'->'commodities_used'->>'used_paracetamol_tablets','')         AS used_paracetamol_tablets,
  NULLIF(couchdb.doc->'fields'->'meta'->>'instanceID','')                          AS instance_id,

  /* -------- Geolocation (top-level) -------- */
  (couchdb.doc->'geolocation'->>'latitude')::float8                                 AS latitude,
  (couchdb.doc->'geolocation'->>'longitude')::float8                                AS longitude,
  (couchdb.doc->'geolocation'->>'accuracy')::float8                                 AS geo_accuracy_m
{% endset %}

{{ cht_form_model('first_aid', custom_fields, age_indexes) }}