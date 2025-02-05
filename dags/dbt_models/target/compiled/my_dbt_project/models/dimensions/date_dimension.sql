-- WITH sales AS (
--     SELECT * FROM "sales_database"."main"."sales"
-- )
-- SELECT
--     transaction_date,
--     strftime(transaction_date, '%Y-%m-%d') AS date_id,  -- Change the format to YYYY-MM-DD
--     strftime(transaction_date, '%A') AS weekday_name,
--     strftime(transaction_date, '%m') AS month,
--     strftime(transaction_date, '%Y') AS year,
--     strftime(transaction_date, '%d') AS day,
--     EXTRACT(QUARTER FROM transaction_date) AS quarter
-- FROM sales
-- WHERE transaction_date IS NOT NULL


WITH date_data AS (
    SELECT DISTINCT
        transaction_date AS date_id,
        EXTRACT(YEAR FROM transaction_date) AS year,
        EXTRACT(MONTH FROM transaction_date) AS month,
        EXTRACT(DAY FROM transaction_date) AS day,
        EXTRACT(WEEK FROM transaction_date) AS week_of_year,
        EXTRACT(QUARTER FROM transaction_date) AS quarter
    FROM "sales_database"."main"."sales"
)

SELECT * FROM date_data