-- WITH sales AS (
--     SELECT * FROM {{ source('apidata', 'sales') }}
-- )

-- SELECT
--     DISTINCT
--     customer AS customer_id,
--     customer_name,
--     customer_address,
--     customer_group,
--     territory,
--     language AS customer_language
-- FROM sales
-- WHERE customer IS NOT NULL




WITH customers AS (
    SELECT DISTINCT
        customer AS customer_id,
        customer_name,
        customer_group,
        territory
    FROM {{ source('apidata', 'sales') }}
)

SELECT * FROM customers
