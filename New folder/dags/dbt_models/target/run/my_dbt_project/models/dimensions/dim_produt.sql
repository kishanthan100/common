
  
  create view "sales_database"."main"."dim_produt__dbt_tmp" as (
    -- WITH sales AS (
--     SELECT * 
--     FROM "sales_database"."main"."sales"
-- )

-- SELECT 
--     sales.name,
--     json_extract(items, '$[0].name') AS product_name,  -- Example to extract the first element's "name" field from JSON array
--     json_extract(items, '$[0].item_code') AS product_item_code
-- FROM sales
-- WHERE json_extract(items, '$[0].name') IS NOT NULL  -- Filter to only keep rows with a non-null product name



WITH exploded_items AS (
    SELECT
        name AS sales_order_id,
        JSON_EXTRACT(items, '$[*].item_code') AS product_code,
        JSON_EXTRACT(items, '$[*].item_name') AS product_name,
        JSON_EXTRACT(items, '$[*].qty') AS quantity_sold,
        JSON_EXTRACT(items, '$[*].rate') AS unit_price
    FROM "sales_database"."main"."sales"
)

SELECT 
    sales_order_id,
    UNNEST(product_code) AS product_id,
    UNNEST(product_name) AS product_name,
    UNNEST(quantity_sold) AS quantity_sold,
    UNNEST(unit_price) AS unit_price
FROM exploded_items
  );
