-- Grain: 1 row per product_ndc (current version only). Historical versions
-- live in dim_drug_snapshot — this model is just the "as of now" slice,
-- used for joining into facts/bridges.
select
    md5(product_ndc) as drug_sk,
    product_ndc,
    product_id,
    spl_id,
    generic_name,
    brand_name,
    brand_name_base,
    labeler_name,
    dosage_form,
    product_type,
    marketing_category,
    application_number,
    route,
    finished,
    marketing_start_date,
    listing_expiration_date,
    dea_schedule,
    manufacturer_name,
    rxcui_list,
    spl_set_id,
    unii,
    pharm_class_epc,
    pharm_class_moa,
    pharm_class_pe,
    pharm_class_cs,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('dim_drug_snapshot') }}
where dbt_valid_to is null