-- Stakeholder-facing pass-rate summary, covering more than one DQ dimension:
--   completeness: does every reaction row have a reaction term?
--   validity:     does product_ndc match the expected NDC format?
--   consistency:  does every bridge row actually resolve to a known drug?
--   uniqueness:   is product_ndc truly unique in dim_drug?
with rules as (
    select
        'fact_adverse_event_reaction' as table_name,
        'completeness: reactionmeddrapt not null' as rule_name,
        count(*) as total_rows,
        count_if(reactionmeddrapt is not null) as passing_rows
    from {{ ref('fact_adverse_event_reaction') }}
 
    union all
 
    select
        'bridge_report_drug',
        'validity: product_ndc format',
        count(*),
        count_if(product_ndc is null or product_ndc rlike '^[0-9]{4,5}-[0-9]{3,4}$')
    from {{ ref('bridge_report_drug') }}
 
    union all
 
    select
        'bridge_report_drug',
        'consistency: drug matched to dim_drug',
        count(*),
        count_if(drug_sk is not null)
    from {{ ref('bridge_report_drug') }}
 
    union all
 
    select
        'dim_drug',
        'uniqueness: product_ndc',
        count(*),
        count(distinct product_ndc)
    from {{ ref('dim_drug') }}
)
 
select
    table_name,
    rule_name,
    total_rows,
    passing_rows,
    round(100.0 * passing_rows / nullif(total_rows, 0), 2) as pass_rate_pct
from rules
order by pass_rate_pct