{{ config(schema='silver') }}

WITH source AS (
    SELECT 
        DISTINCT 
        name AS it_sales_order_name, 
        json_extract(items, '$[*].name') AS item_specific_name,  -- Extract name from JSON
        json_extract(items, '$[*].item_code') AS item_code,
        json_extract(items, '$[*].item_name') AS item_name,
        json_extract(items, '$[*].creation') AS creation,
        json_extract(items, '$[*].delivery_date') AS delivery_date,
        json_extract(items, '$[*].qty') AS quantity,
        json_extract(items, '$[*].price_list_rate') AS price_list_rate,
        json_extract(items, '$[*].transaction_date') AS transaction_date,
        json_extract(items, '$[*].description') AS description_of_item
    FROM {{ ref('stg_salesorder') }} 
    WHERE name IS NOT NULL
)

SELECT 
    it_sales_order_name AS it_sales_order_name,
    UNNEST(item_specific_name) AS item_specific_name,
    REPLACE(CAST(UNNEST(item_code)AS VARCHAR),'"' , '') AS item_code,
    REPLACE(CAST(UNNEST(item_name)AS VARCHAR),'"','' ) AS item_name,
    CAST(UNNEST(creation) AS TIMESTAMP) AS creation,
    CAST(UNNEST(delivery_date) AS DATE) AS delivery_date,
    CAST(UNNEST(quantity) AS INTEGER) AS quantity,
    CAST(UNNEST(price_list_rate) AS DECIMAL(10,2)) AS price_list_rate,
    CAST(UNNEST(transaction_date) AS DATE) AS transaction_date,
    UNNEST(description_of_item) AS description_of_item
FROM source
