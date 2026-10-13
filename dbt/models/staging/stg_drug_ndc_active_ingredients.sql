-- Grain: 1 row per (product_ndc, active ingredient). outer => true keeps any
-- product with an empty/null active_ingredients array as a single NULL row
-- instead of silently dropping it.
select
    t.product_ndc,
    i.value:name::string as ingredient_name,
    i.value:strength::string as strength
from {{ source('landing', 'drug_ndc_products') }} t,
     lateral flatten(input => t.active_ingredients, outer => true) i