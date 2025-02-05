WITH companies AS (
    SELECT DISTINCT
        company AS company_id,
        company
    FROM "sales_database"."main"."sales"
)

SELECT * FROM companies