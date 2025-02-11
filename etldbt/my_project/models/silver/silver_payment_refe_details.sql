{{ config(schema='silver') }}

WITH source AS (
    SELECT 
    DISTINCT   
        json_extract(references_data, '$[*].name') AS name,  -- Extract name from JSON
        json_extract(references_data, '$[*].due_date') AS due_date,
        json_extract(references_data, '$[*].total_amount') AS total_amount,
        json_extract(references_data, '$[*].outstanding_amount') AS outstanding_amount

        
          -- Extract item_code from JSON
    FROM {{ ref('stg_payment_entry') }} 
)

SELECT 
    UNNEST(name) AS name,
    UNNEST(due_date) AS due_date,
    UNNEST(total_amount) AS total_amount, 
    UNNEST(outstanding_amount) AS outstanding_amount
    
FROM source
