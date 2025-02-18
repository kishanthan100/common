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

print(f"sales order  *********************************************************")
sil_salesorder = c.execute("PRAGMA table_info(silver.sil_salesorder_item)").fetchall()
print(sil_salesorder)




print(f"sales order **************************************************************************")
sil_salesorder = c.execute("SELECT * FROM silver.sil_salesorder").fetchdf()
print(sil_salesorder)





print(f"sales order item **************************************************************************")
sil_salesorder_item = c.execute("SELECT * FROM silver.sil_salesorder_item").fetchdf()
print(sil_salesorder_item)




print(f"sales order payment **************************************************************************")
sil_salesorder_payment = c.execute("SELECT * FROM silver.sil_salesorder_payment").fetchdf()
print(sil_salesorder_payment)



print(f" payment **************************************************************************")
sil_payment = c.execute("SELECT * FROM silver.sil_payment").fetchdf()
print(sil_payment)



print("************************************************************************************************************************")
# query1 = """
# SELECT items FROM bronze.stg_sales_order
# """
# df = c.execute(query1).fetchdf()
# df["items"] = df["items"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
# items_df = pd.json_normalize(df["items"].explode()).reset_index(drop=True)
# items_df = items_df[['name','item_code']]
# filter = items_df[items_df['name']=='tmgjer2o9l']
# print(items_df)
# print(filter)

# print(f"join sales, s_item, s_payment******************************************************************************************************")
# query = """
#         SELECT 
#         s.name AS Sales_Order_Name,
#         s.customer_name AS Customer_Name,
#         spd.payment_amount AS Total Payment,
#         spd.outstanding AS Outstanding Payment,
#         spd.due_date AS Due_DATE,
#         sid.delivery_date AS Item_Delivery_Date
#         --sid.item_code,
#         --COUNT(sid.item_code) AS code
        
#         FROM silver.sil_salesorder s
        
#         JOIN silver.sil_salesorder_item sid
#         ON s.name=sid.it_sales_order_name
#         JOIN silver.sil_salesorder_payment spd
#         ON s.name=spd.py_sales_order_name

#         WHERE  spd.outstanding = 7235 AND spd.due_date = '2024-12-12'
#         --WHERE sid.item_code = 'OKRA'

#         --GROUP BY
#         --s.name,
#         --s.customer_name,
#         --spd.payment_amount,
#         --spd.outstanding,
#         --spd.due_date,
#         --sid.delivery_date,
#         --sid.item_code

#         """
# join = c.execute(query).fetchdf()
# print(join)


print(f"join sales, s_item, s_payment******************************************************************************************************")
query2 = """
        WITH total_payments AS (
    SELECT
        SUM(CAST(payment_amount AS INTEGER)) AS total_payment
    FROM silver.sil_salesorder_payment
)
SELECT 
    s.customer,
    SUM(CAST(spd.payment_amount AS INTEGER)) AS total_payment,
    (SUM(CAST(spd.payment_amount AS INTEGER)) / total_payments.total_payment) * 100 AS payment_percentage
FROM 
    silver.sil_salesorder_payment spd
JOIN 
    silver.sil_salesorder s
    ON s.name = spd.py_sales_order_name
CROSS JOIN 
    total_payments  -- This allows access to the total payment value for each row
GROUP BY 
    s.customer, total_payments.total_payment



        """
join2 = c.execute(query2).fetchdf()
print(join2)
