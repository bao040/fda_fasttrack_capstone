-- DQ dimension: validity. Rows that pass the NDC format check (or have no
-- product_ndc at all, which is a separate completeness concern, not a
-- validity failure) continue through the normal model.
select *
from {{ ref('bridge_report_drug') }}
where product_ndc is null
   or product_ndc rlike '^[0-9]{4,5}-[0-9]{3,4}$'