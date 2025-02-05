-- /opt/airflow/dags/dbt_models/transform_users.sql


WITH sales AS (
    SELECT * FROM "sales_database"."main"."sales"
)

SELECT
   *
FROM sales
WHERE name = 'ABC private LTD-0002'