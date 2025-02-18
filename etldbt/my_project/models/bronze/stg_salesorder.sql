
WITH source AS (
    SELECT * FROM {{ source('erp_raw', 'sales_order') }}
)

SELECT * FROM source
