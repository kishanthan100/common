
WITH source AS (
    SELECT * FROM {{ source('erp_raw', 'payment_details') }}
)

SELECT * FROM source