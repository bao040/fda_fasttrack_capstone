-- Grain: 1 row per (product_ndc, package). outer => true keeps any product
-- with an empty/null packaging array as a single NULL row instead of
-- silently dropping it.
select
    t.product_ndc,
    p.value:package_ndc::string as package_ndc,
    p.value:description::string as description,
    p.value:marketing_start_date::date as marketing_start_date,
    p.value:marketing_end_date::date as marketing_end_date,
    p.value:sample::boolean as is_sample
from {{ source('landing', 'drug_ndc_products') }} t,
     lateral flatten(input => t.packaging, outer => true) p