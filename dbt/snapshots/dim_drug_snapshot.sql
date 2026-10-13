{% snapshot dim_drug_snapshot %}
 
{{
    config(
        unique_key='product_ndc',
        strategy='check',
        check_cols=['brand_name', 'manufacturer_name', 'pharm_class_epc', 'dea_schedule', 'marketing_category'],
    )
}}
 
select * from {{ ref('stg_drug_ndc_products') }}
 
{% endsnapshot %}