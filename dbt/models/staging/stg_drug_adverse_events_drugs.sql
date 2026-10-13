-- Grain: 1 row per (safetyreportid, drug, product_ndc candidate).
-- Only openfda_product_ndc is flattened — it's the sole field with a join
-- target in this schema (dim_drug, keyed by product_ndc). openfda_package_ndc
-- and openfda_rxcui use different identifier systems/granularities with no
-- corresponding dimension table here, so flattening them would add rows with
-- nothing to join into. The other openfda_* attributes (brand_name,
-- manufacturer_name, pharm_class_epc...) are dropped here on purpose — they
-- duplicate dim_drug's more authoritative NDC Directory data and would create
-- two conflicting sources of truth for the same attribute.
-- Both flatten levels use outer => true so reports with no drug[], or drugs
-- with no openfda enrichment, are kept (NULL product_ndc) instead of
-- silently disappearing from the output.
select
    t.safetyreportid,
    d.value:drugcharacterization::string as drugcharacterization,
    d.value:medicinalproduct::string as medicinalproduct,
    d.value:drugindication::string as drugindication,
    d.value:drugadministrationroute::string as drugadministrationroute,
    d.value:drugdosagetext::string as drugdosagetext,
    d.value:drugstartdate::date as drugstartdate,
    d.value:drugenddate::date as drugenddate,
    d.value:activesubstancename::string as activesubstancename,
    ndc.value::string as product_ndc
from {{ source('landing', 'drug_adverse_events_reports') }} t,
     lateral flatten(input => t.drug, outer => true) d,
     lateral flatten(input => d.value:openfda_product_ndc, outer => true) ndc