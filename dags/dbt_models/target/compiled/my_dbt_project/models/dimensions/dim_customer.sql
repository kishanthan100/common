-- WITH sales AS (
--     SELECT * FROM "sales_database"."main"."sales"
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
    FROM "sales_database"."main"."sales"
)

SELECT * FROM customers