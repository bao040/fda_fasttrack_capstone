-- Grain: 1 row per safetyreportid. reaction/drug VARIANT columns are handled
-- by their own flatten models (stg_drug_adverse_events_reactions/_drugs), not
-- selected here — this model stays at report-header scalar attributes only.
-- patient_age_years normalizes patient_onsetage using its unit code so
-- dim_patient can bucket on a single consistent scale:
--   800=Decade, 801=Year, 802=Month, 803=Week, 804=Day, 805=Hour
with base as (
    select
        safetyreportid,
        safetyreportversion,
        primarysourcecountry,
        occurcountry,
        reporttype,
        authoritynumb,
        companynumb,
        transmissiondate,
        receivedate,
        receiptdate,
        serious,
        seriousnessdeath,
        seriousnesslifethreatening,
        seriousnesshospitalization,
        seriousnessdisabling,
        seriousnesscongenitalanomali,
        seriousnessother,
        duplicate,
        duplicate_source,
        duplicate_numb,
        reporter_country,
        reporter_qualification,
        reporter_literaturereference,
        sender_type,
        sender_organization,
        receiver_type,
        receiver_organization,
        patient_onsetage,
        patient_onsetage_unit,
        case patient_onsetage_unit
            when '800' then try_cast(patient_onsetage as float) * 10
            when '801' then try_cast(patient_onsetage as float)
            when '802' then try_cast(patient_onsetage as float) / 12
            when '803' then try_cast(patient_onsetage as float) / 52.1775
            when '804' then try_cast(patient_onsetage as float) / 365.25
            when '805' then try_cast(patient_onsetage as float) / 8760
            else null
        end as patient_age_years,
        patient_weight,
        patient_sex,
        patient_agegroup,
        narrative
    from {{ source('landing', 'drug_adverse_events_reports') }}
)
 
select
    *,
    case
        when patient_age_years is null then 'Unknown'
        when patient_age_years < 18 then '0-17'
        when patient_age_years < 31 then '18-30'
        when patient_age_years < 46 then '31-45'
        when patient_age_years < 61 then '46-60'
        when patient_age_years < 76 then '61-75'
        else '76+'
    end as patient_age_bucket
from base