-- Junk dimension: demographic profile, not a real customer identity (FAERS
-- reports are anonymous). Grain: 1 row per distinct (sex, agegroup, age_bucket).
select distinct
    md5(
        coalesce(patient_sex, 'NA') || '|' ||
        coalesce(patient_agegroup, 'NA') || '|' ||
        patient_age_bucket
    ) as patient_sk,
    patient_sex,
    patient_agegroup,
    patient_age_bucket
from {{ ref('stg_drug_adverse_events_reports') }}