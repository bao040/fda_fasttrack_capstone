-- Grain: 1 row per distinct (reporter_country, reporter_qualification, sender_organization).
select distinct
    md5(
        coalesce(reporter_country, 'NA') || '|' ||
        coalesce(reporter_qualification, 'NA') || '|' ||
        coalesce(sender_organization, 'NA')
    ) as reporter_sk,
    reporter_country,
    reporter_qualification,
    sender_organization
from {{ ref('stg_drug_adverse_events_reports') }}