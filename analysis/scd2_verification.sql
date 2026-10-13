select 
    product_ndc,
    brand_name,
    manufacturer_name,
    dbt_valid_from,
    dbt_valid_to
from snapshots.dim_drug_snapshot
where product_ndc = 'sample_ndc'
order by dbt_valid_from;