
-- BQ1 — Death Rate by Pharmacologic Class

with report_drug_class as (
    select distinct
        b.safetyreportid,
        d.pharm_class_epc
    from marts.bridge_report_drug b
    join marts.dim_drug d on b.drug_sk = d.drug_sk
    where d.pharm_class_epc is not null
),
report_death as (
    select distinct safetyreportid, seriousnessdeath
    from marts.fact_adverse_event_reaction
)
select
    rdc.pharm_class_epc,
    count(distinct rdc.safetyreportid) as total_reports,
    count(distinct case when rd.seriousnessdeath then rdc.safetyreportid end) as death_reports,
    round(
        100.0 * count(distinct case when rd.seriousnessdeath then rdc.safetyreportid end)
        / count(distinct rdc.safetyreportid),
        2
    ) as death_rate_pct
from report_drug_class rdc
join report_death rd on rdc.safetyreportid = rd.safetyreportid
group by rdc.pharm_class_epc
having count(distinct rdc.safetyreportid) >= 20
order by death_rate_pct desc
limit 10;
 
-- =============================================================================
-- BQ2 — Most Frequent Reactions Among Serious Reports

select
    reactionmeddrapt,
    count(*) as serious_reaction_count
from marts.fact_adverse_event_reaction
where serious = true
  and reactionmeddrapt is not null
group by reactionmeddrapt
order by serious_reaction_count desc
limit 15;
 
-- =============================================================================
-- BQ3 — Monthly Trend of Serious Reports

with distinct_reports as (
    select distinct safetyreportid, receivedate, serious
    from marts.fact_adverse_event_reaction
)
select
    date_trunc('month', receivedate) as report_month,
    count(*) as total_reports,
    count_if(serious) as serious_reports,
    round(100.0 * count_if(serious) / count(*), 2) as serious_rate_pct
from distinct_reports
group by report_month
order by report_month;
 
-- =============================================================================
-- BQ4 — Death Rate by Manufacturer


with report_manufacturer as (
    select distinct
        b.safetyreportid,
        d.manufacturer_name
    from marts.bridge_report_drug b
    join marts.dim_drug d on b.drug_sk = d.drug_sk
    where d.manufacturer_name is not null
),
report_death_2 as (
    select distinct safetyreportid, seriousnessdeath
    from marts.fact_adverse_event_reaction
)
select
    rm.manufacturer_name,
    count(distinct rm.safetyreportid) as total_reports,
    count(distinct case when rd.seriousnessdeath then rm.safetyreportid end) as death_reports,
    round(
        100.0 * count(distinct case when rd.seriousnessdeath then rm.safetyreportid end)
        / count(distinct rm.safetyreportid),
        2
    ) as death_rate_pct
from report_manufacturer rm
join report_death_2 rd on rm.safetyreportid = rd.safetyreportid
group by rm.manufacturer_name
having count(distinct rm.safetyreportid) >= 20
order by death_rate_pct desc
limit 10;
 
 
-- =============================================================================
-- BQ5 — Reaction Completeness Check (reference/bonus, not a core question)

select
    primarysourcecountry,
    count(*) as total_reports,
    count_if(reactionmeddrapt is null) as reports_with_no_reaction,
    round(100.0 * count_if(reactionmeddrapt is null) / count(*), 2) as missing_reaction_pct
from marts.fact_adverse_event_reaction f
join staging.stg_drug_adverse_events_reports r on f.safetyreportid = r.safetyreportid
group by primarysourcecountry
order by total_reports desc
limit 10;