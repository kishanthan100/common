{{ config(schema='silver') }}

WITH source AS (
    SELECT 
    
    name, owner, creation, modified, modified_by, docstatus, 
    customer_name, customer_type, customer_group, customer_primary_address, 
    primary_address, default_commission_rate
        
        
          -- Extract item_code from JSON
    FROM {{ ref('stg_customer_details') }} 
)

SELECT  * FROM source
