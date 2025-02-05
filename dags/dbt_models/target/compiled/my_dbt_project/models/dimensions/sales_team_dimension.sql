WITH sales AS (
    SELECT * FROM "sales_database"."main"."sales"
)

SELECT
    DISTINCT
    sales_team->>'name' AS sales_team_id,
    sales_team->>'region' AS sales_team_region
FROM sales
WHERE sales_team IS NOT NULL