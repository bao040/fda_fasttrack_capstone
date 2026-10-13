-- DQ dimension: validity. Rows with a product_ndc that doesn't match the
-- expected NDC format (4-5 digit labeler, 3-4 digit product) — routed here
-- instead of silently failing the whole build.
select
    *,
    'invalid_ndc_format' as failure_reason
from {{ ref('bridge_report_drug') }}
where product_ndc is not null
  and not (product_ndc rlike '^[0-9]{4,5}-[0-9]{3,4}$')