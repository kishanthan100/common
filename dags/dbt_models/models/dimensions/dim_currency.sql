WITH currency_data AS (
    SELECT DISTINCT
        currency AS currency_id,
        currency,
        conversion_rate
    FROM {{ source('apidata', 'sales') }}
)

SELECT * FROM currency_data
