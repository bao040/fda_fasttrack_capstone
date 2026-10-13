-- Grain: 1 row per (safetyreportid, drug, product_ndc candidate).
-- LEFT JOIN dim_drug — many drugs won't match (foreign/unapproved products,
-- herbal supplements, or simply missing openfda enrichment; see conversation
-- notes on the NITROFURANTOIN/CARBIMAZOLE/CAMELLIA SINENSIS examples).
select
    d.safetyreportid,
    d.drugcharacterization,
    d.medicinalproduct,
    d.drugindication,
    d.drugadministrationroute,
    d.drugdosagetext,
    d.drugstartdate,
    d.drugenddate,
    d.activesubstancename,
    d.product_ndc,
    dd.drug_sk
from {{ ref('stg_drug_adverse_events_drugs') }} d
left join {{ ref('dim_drug') }} dd
    on d.product_ndc = dd.product_ndc