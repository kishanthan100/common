
WITH source AS (
    SELECT * FROM {{ source('erp_raw', 'customer_details') }}
)

SELECT * FROM source