
  
  create view "sales_database"."main"."dim_currency__dbt_tmp" as (
    WITH currency_data AS (
    SELECT DISTINCT
        currency AS currency_id,
        currency,
        conversion_rate
    FROM "sales_database"."main"."sales"
)

SELECT * FROM currency_data
  );
