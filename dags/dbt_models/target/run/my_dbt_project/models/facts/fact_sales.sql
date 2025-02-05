
  
    
    

    create  table
      "sales_database"."main"."fact_sales__dbt_tmp"
  
    as (
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
  
  