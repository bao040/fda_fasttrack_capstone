-- Grain: 1 row per (safetyreportid, reaction). Deliberately NOT joined to
-- bridge_report_drug/dim_drug — openFDA's own docs confirm no drug is linked
-- to any specific reaction within a report, so drug-level analysis must join
-- separately via safetyreportid at query time, not be baked into this grain.
-- Includes reports with no listed reaction as a placeholder row
-- (reactionmeddrapt IS NULL) per team decision — not filtered out here.
select
    md5(r.safetyreportid || '|' || r.reaction_index::string) as reaction_sk,
    r.safetyreportid,
    r.reactionmeddrapt,
    r.reactionmeddraversionpt,
    r.reactionoutcome,
    rep.serious,
    rep.seriousnessdeath,
    rep.seriousnesslifethreatening,
    rep.seriousnesshospitalization,
    rep.seriousnessdisabling,
    rep.seriousnesscongenitalanomali,
    rep.seriousnessother,
    rep.receivedate,
    pat.patient_sk,
    rptr.reporter_sk
from {{ ref('stg_drug_adverse_events_reactions') }} r
left join {{ ref('stg_drug_adverse_events_reports') }} rep
    on r.safetyreportid = rep.safetyreportid
left join {{ ref('dim_patient') }} pat
    on coalesce(rep.patient_sex, 'NA') = coalesce(pat.patient_sex, 'NA')
   and coalesce(rep.patient_agegroup, 'NA') = coalesce(pat.patient_agegroup, 'NA')
   and rep.patient_age_bucket = pat.patient_age_bucket
left join {{ ref('dim_reporter') }} rptr
    on coalesce(rep.reporter_country, 'NA') = coalesce(rptr.reporter_country, 'NA')
   and coalesce(rep.reporter_qualification, 'NA') = coalesce(rptr.reporter_qualification, 'NA')
   and coalesce(rep.sender_organization, 'NA') = coalesce(rptr.sender_organization, 'NA')