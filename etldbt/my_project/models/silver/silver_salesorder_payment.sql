{{ config(schema='silver',
    primary_key='name') }}

WITH source AS (
    SELECT 
    DISTINCT 
        name AS py_sales_order_name, 
        json_extract(payment_schedule, '$[*].name') AS payment_specific_name,  -- Extract name from JSON
        json_extract(payment_schedule, '$[*].owner') AS owner,
        json_extract(payment_schedule, '$[*].creation') AS creation,
        json_extract(payment_schedule, '$[*].modified') AS modified,
        json_extract(payment_schedule, '$[*].due_date') AS due_date,
        json_extract(payment_schedule, '$[*].discount') AS discount,
        json_extract(payment_schedule, '$[*].payment_amount') AS payment_amount,
        json_extract(payment_schedule, '$[*].outstanding') AS outstanding,
        json_extract(payment_schedule, '$[*].discounted_amount') AS discounted_amount
        


        
          -- Extract item_code from JSON
    FROM {{ ref('stg_sales_order') }} 
    WHERE name IS NOT NULL
)

SELECT 
    py_sales_order_name AS py_sales_order_name,
    UNNEST(payment_specific_name) AS payment_specific_name,
    UNNEST(owner) As owner,
    UNNEST(due_date) AS due_date ,
    UNNEST(creation)AS creation ,
    UNNEST(modified) AS modified,
    UNNEST(discount) AS discount,
    UNNEST(payment_amount) AS payment_amount,
    UNNEST(outstanding) AS outstanding,
    UNNEST(discounted_amount) AS discounted_amount
    
    
FROM source
