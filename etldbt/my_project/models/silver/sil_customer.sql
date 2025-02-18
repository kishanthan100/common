{{ config(
    schema='silver',
    materialized='table'
) }}

WITH source AS (
    SELECT 
        name AS customer_id,  -- Set as primary key
        owner,
        creation,
        modified,
        modified_by,
        docstatus,
        customer_name,
        customer_type,
        customer_group,
        customer_primary_address,
        primary_address,
        default_commission_rate
        
    FROM {{ ref('stg_customer') }} 
    WHERE name IS NOT NULL  -- Ensure no null values in primary key
)

SELECT DISTINCT * FROM source
