-- Grain: 1 row per product_ndc. active_ingredients/packaging are handled by
-- separate flatten models below, not included here — this model stays at
-- product-level scalar attributes only (plus route/rxcui_list, kept as
-- arrays since they're rarely joined on individually).
select
    trim(product_ndc) as product_ndc,
    product_id,
    spl_id,
    trim(generic_name) as generic_name,
    trim(brand_name) as brand_name,
    trim(brand_name_base) as brand_name_base,
    trim(labeler_name) as labeler_name,
    dosage_form,
    product_type,
    marketing_category,
    application_number,
    route,
    finished,
    marketing_start_date,
    listing_expiration_date,
    dea_schedule,
    trim(manufacturer_name) as manufacturer_name,
    rxcui_list,
    spl_set_id,
    unii,
    pharm_class_epc,
    pharm_class_moa,
    pharm_class_pe,
    pharm_class_cs
from {{ source('landing', 'drug_ndc_products') }}