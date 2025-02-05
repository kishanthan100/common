
  
    
    

    create  table
      "sales_database"."main"."sales_fact__dbt_tmp"
  
    as (
      -- WITH sales AS (
--     SELECT * FROM "sales_database"."main"."sales"
-- ),
-- customer AS (
--     SELECT * FROM "sales_database"."main"."customer_dimension"
-- ),
-- product AS (
--     SELECT * FROM "sales_database"."main"."product_dimension"
-- ),
-- sales_team AS (
--     SELECT * FROM "sales_database"."main"."sales_team_dimension"
-- ),
-- date AS (
--     SELECT * FROM "sales_database"."main"."date_dimension"
-- )

-- SELECT
--     s.name AS sales_id,
--     c.customer_id,
--     p.product_id,
--     st.sales_team_id,
--     d.date_id,
--     s.total_qty,
--     s.total_net_weight,
--     s.total,
--     s.net_total,
--     s.grand_total,
--     s.tax_category
-- FROM sales s
-- JOIN customer c 
--     ON s.customer = c.customer_id
-- -- Handling product as JSON in DuckDB
-- JOIN LATERAL (
--     SELECT json_extract(s.items, '$[0].product_id') AS product_id
--     FROM (VALUES (s.items)) AS items(item)
--     WHERE json_extract(item, '$[0].product_id') IS NOT NULL
-- ) AS p ON TRUE
-- -- Handling sales_team as JSON in DuckDB
-- JOIN LATERAL (
--     SELECT json_extract(s.sales_team, '$[0].sales_team_id') AS sales_team_id
--     FROM (VALUES (s.sales_team)) AS team(item)
--     WHERE json_extract(item, '$[0].sales_team_id') IS NOT NULL
-- ) AS st ON TRUE
-- JOIN date d 
--     ON s.transaction_date = d.date_id




WITH sales_data AS (
    SELECT
        name AS sales_order_id,
        transaction_date,
        customer,
        company,
        total AS sales_amount,
        net_total,
        discount_amount,
        base_total_taxes_and_charges AS tax_amount,
        grand_total,
        currency,
        conversion_rate,
        items, -- JSON field with product details
        taxes -- JSON field with tax details
    FROM "sales_database"."main"."sales"
)

SELECT * FROM sales_data
    );
  
  