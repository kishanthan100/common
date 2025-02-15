import duckdb
import pandas as pd, json



c = duckdb.connect("duckdb/erp.duckdb")
data = c.execute("SHOW TABLES ").fetch_df()
# data1 = c.execute("DROP VIEW stg_sales_order ").fetch_df()
# column = c.execute("PRAGMA table_info(sales_order)").fetch_df()
# bronze_table= c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='bronze'").fetchdf()
# print(f"these are bronze tables {bronze_table}")
# print("************************************************************************************************************************")

silver_table= c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='silver'").fetchdf()
print(f"these are silver_table {silver_table}")
print("************************************************************************************************************************")
sc = c.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
print(f"schema exist in database {sc}")

print("************************************************************************************************************************")

# #data = c.execute(" SELECT * FROM bronze.stg_sales_order").fetchdf()
# print('data is',data)
# print("************************************************************************************************************************")

# # silver_sales_details = c.execute("SELECT billing_status FROM silver.silver_salesorder_details LIMIT 5").fetchdf()
# # print(f"silver_sales_details{silver_sales_details}")

# print("###################################################################")

# silver_item_details = c.execute("SELECT * FROM silver.siver_salesorder_item_details").fetchdf()
# print(f"silver_item_details{silver_item_details}")
# print("***************************************************************")

# silver_payment_one_details = c.execute("SELECT * FROM silver.silver_payment_details").fetchdf()
# print(f"silver_payment_details{silver_payment_one_details}")

# silver_payment_one_details = c.execute("SELECT * FROM silver.silver_payment_details").fetchdf()
# print(f"silver_item_details{silver_payment_one_details}")

# bronze_payment_one_details = c.execute("SELECT * FROM bronze.stg_payment_entry LIMIT 5").fetchdf()
# print(f"bronze_payment_details{bronze_payment_one_details}")

# bronze_payment_one_details = c.execute("SELECT references_data FROM bronze.stg_payment_entry LIMIT 5").fetchdf()
# print(f"bronze_payment_details{bronze_payment_one_details}")




# pay_pae_det = c.execute(
#     "SELECT * FROM silver.silver_payment_refe_details WHERE name LIKE '%s4dmk2ucgf%'"
# ).fetchdf()
# print(pay_pae_det)

# sil_customer_col = c.execute("PRAGMA table_info(silver.silver_onlycustomer_details)").fetchdf()
# print(f"silver customer details")
# print(sil_customer_col)

# sil_customer_details = c.execute("SELECT creation,quantity, item_code FROM silver.siver_salesorder_item_details").fetchdf()
# print(sil_customer_details)

# sales_main = c.execute("SELECT sales_order_name FROM silver.siver_salesorder_details").fetchdf()
# print(sales_main)

# payment_main = c.execute('SELECT "references" FROM payment_details LIMIT 5').fetchdf()
# print(payment_main)

# COLUMN = c.execute("PRAGMA table_info(silver.silver_salesorder_payment)").fetchall()
# print(COLUMN)

# payment_main = c.execute('SELECT creation, due_date, payment_amount,outstanding, discounted_amount FROM silver.silver_salesorder_payment LIMIT 5').fetchdf()
# print(payment_main)


# COLUMN = c.execute("PRAGMA table_info(silver.silver_payment_details)").fetchall()
# print(COLUMN)

payment_main = c.execute('SELECT * FROM silver.silver_onlycustomer_details LIMIT 5').fetchdf()
print(payment_main)

# count = c.execute("SELECT COUNT(*) FROM bronze.stg_customer_details").fetchall()
# print(count)

count = c.execute("SELECT  name FROM silver.silver_salesorder_details").fetchall()
print(count)