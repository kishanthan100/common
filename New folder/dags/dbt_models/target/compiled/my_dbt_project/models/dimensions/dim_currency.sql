WITH currency_data AS (
    SELECT DISTINCT
        currency AS currency_id,
        currency,
        conversion_rate
    FROM "sales_database"."main"."sales"
)

SELECT * FROM currency_data