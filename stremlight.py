import streamlit as st
import json
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px

# DuckDB Database Path
DUCKDB_PATH = "duckdb/erp.duckdb"

# Sidebar Design Improvements
st.sidebar.title("📊 Dashboard Filters")


# Add a section to show tables (Sales, Payment, etc.) as clickable buttons
section = st.sidebar.radio("Select Section to View", [
    "Out Standing Payment Details",
    "Today Payment",
    "pi",
    "barchart"
])

# Function to Load Sales Data


@st.cache_data
def load_salesorder_item_payment_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
        SELECT 
        s.name AS Sales_Order_Name,
        s.customer_name AS Customer_Name,
        spd.payment_amount AS Total_Payment,
        spd.outstanding AS Outstanding_Payment,
        spd.due_date AS Due_Date,
        sid.delivery_date AS Item_Delivery_Date,
        sid.item_code,
        --COUNT(sid.item_code) AS code
        
        FROM silver.sil_salesorder s
        
        JOIN silver.sil_salesorder_item sid
        ON s.name=sid.it_sales_order_name
        JOIN silver.sil_salesorder_payment spd
        ON s.name=spd.py_sales_order_name

        --WHERE  spd.outstanding = 7235 AND spd.due_date = '2024-12-12'
        --WHERE sid.item_code = 'OKRA'

        --GROUP BY
        --s.name,
        --s.customer_name,
        --spd.payment_amount,
        --spd.outstanding,
        --spd.due_date,
        --sid.delivery_date,
        --sid.item_code

        """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

@st.cache_data
def today_payment():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
        SELECT 
        s.name AS Sales_Order_Name,
        s.customer_name AS Customer_Name,
        spd.payment_amount AS Total_Payment,
        spd.outstanding AS Outstanding_Payment,
        spd.due_date AS Due_Date,
        sid.delivery_date AS Item_Delivery_Date,
        sid.item_code,
        --COUNT(sid.item_code) AS code
        
        FROM silver.sil_salesorder s
        
        JOIN silver.sil_salesorder_item sid
        ON s.name=sid.it_sales_order_name
        JOIN silver.sil_salesorder_payment spd
        ON s.name=spd.py_sales_order_name

        --WHERE  spd.outstanding = 7235 AND spd.due_date = '2024-12-12'
        WHERE spd.due_date = CURRENT_DATE

        --GROUP BY
        --s.name,
        --s.customer_name,
        --spd.payment_amount,
        --spd.outstanding,
        --spd.due_date,
        --sid.delivery_date,
        --sid.item_code

        """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df


@st.cache_data
def pichart():
    conn = duckdb.connect(DUCKDB_PATH)
    query2 = """
    WITH total_payments AS (
        SELECT SUM(CAST(payment_amount AS INTEGER)) AS total_payment
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
        total_payments  
    GROUP BY 
        s.customer, total_payments.total_payment
    """
    df = conn.execute(query2).fetchdf()
    conn.close()
    fig = px.pie(df, names="customer", values="payment_percentage", title="Customer Payment Percentage")
    st.plotly_chart(fig)


def barchart():
    conn = duckdb.connect(DUCKDB_PATH)

    # Query to get payment percentage
    query2 = """
        WITH total_payments AS (
            SELECT SUM(CAST(payment_amount AS INTEGER)) AS total_payment
            FROM silver.sil_salesorder_payment spd
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
            total_payments  
        GROUP BY 
            s.customer, total_payments.total_payment
    """

    # Execute query and fetch data
    df = conn.execute(query2).fetchdf()

    # Close the connection
    conn.close()

    # Create Bar Chart
    fig = px.bar(df, x="customer", y="payment_percentage", 
                title="Customer Payment Percentage", 
                labels={"payment_percentage": "Payment %", "customer": "Customer"}, 
                text_auto=True, color="payment_percentage", 
                color_continuous_scale="blues")

    # Display in Streamlit
    
    st.plotly_chart(fig)




if section == "Out Standing Payment Details":
    st.title("📊 Out Standing Payment Details")
    df_salesorder_item_payment = load_salesorder_item_payment_data()
    st.dataframe(df_salesorder_item_payment)

if section == "Today Payment":
    st.title("📊 Today Payment")
    df_salesorder_item_payment = today_payment()
    st.dataframe(df_salesorder_item_payment)

if section == "pi":
    st.title("📊 Customers Payment as PI-CHART")
    df_salesorder_item_payment = pichart()
    st.dataframe(df_salesorder_item_payment)

if section == "barchart":
    st.title("📊 Customers Payment as BAR-CHART")
    df_salesorder_item_payment = barchart()
    st.dataframe(df_salesorder_item_payment)
