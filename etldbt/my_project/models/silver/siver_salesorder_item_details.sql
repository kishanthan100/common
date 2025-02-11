{{ config(schema='silver') }}

WITH source AS (
    SELECT 
    DISTINCT   
        json_extract(items, '$[*].name') AS item_specific_name,  -- Extract name from JSON
        json_extract(items, '$[*].item_code') AS item_code,
        json_extract(items, '$[*].item_name') AS item_name,
        json_extract(items, '$[*].creation') AS creation,
        json_extract(items, '$[*].delivery_date') AS delivery_date,
        json_extract(items, '$[*].qty') AS quantity,
        json_extract(items, '$[*].price_list_rate') AS price_list_rate,
        json_extract(items, '$[*].transaction_date') AS transaction_date

        
          -- Extract item_code from JSON
    FROM {{ ref('stg_sales_order') }} 
)

SELECT 
    UNNEST(item_specific_name) AS item_specific_name,
    UNNEST(item_code) As item_code,
    UNNEST(item_name) AS item_name ,
    UNNEST(creation)AS creation ,
    UNNEST(delivery_date) AS delivery_date,
    UNNEST(quantity) AS quantity,
    UNNEST(price_list_rate) AS price_list_rate,
    UNNEST(transaction_date) AS transaction_date
    
FROM source
