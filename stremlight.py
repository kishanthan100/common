import streamlit as st
import duckdb
import pandas as pd

# DuckDB Database Path
DUCKDB_PATH = "duckdb/erp.duckdb"

# Sidebar Design Improvements
st.sidebar.title("📊 Dashboard Filters")

# Add a section to show tables (Sales, Payment, etc.) as clickable buttons
section = st.sidebar.radio("Select Section to View", [
    "Sales Dashboard", 
    "Payment Dashboard", 
    "Customer Dashboard", 
    "Sales Order & Item", 
    "Sales Order, Item & Payment"
])

# Function to Load Sales Data
@st.cache_data
def load_salesorder_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
    SELECT *
    FROM silver.silver_salesorder_item_details
    """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

# Function to Load Payment Data
@st.cache_data
def load_payment_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
    SELECT *
    FROM silver.silver_payment_details
    """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

# Function to Load Customer Data
@st.cache_data
def load_customer_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
    SELECT *
    FROM silver.silver_onlycustomer_details
    """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

# Function to Load Sales Order & Item Data
@st.cache_data
def load_salesorder_item_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
    SELECT 
        sd.name,
        sd.total_qty,
        sd.grand_total,
        sd.advance_paid,
        sd.discount_amount,
        sd.owner,
        sd.customer_name,
        sid.item_code,
        sid.delivery_date,
        sid.transaction_date
    FROM silver.silver_salesorder_details sd
    JOIN silver.silver_salesorder_item_details sid
    ON sd.name = sid.it_sales_order_name
    """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

# Function to Load Sales Order, Item & Payment Data
@st.cache_data
def load_salesorder_item_payment_data():
    conn = duckdb.connect(DUCKDB_PATH)
    query = """
    SELECT 
        sd.name AS sales_order_name,
        sd.customer_name,
        sd.total_qty,
        sd.grand_total,
        sd.advance_paid,
        spd.outstanding,
        spd.due_date,
        sd.discount_amount,
        sd.owner,
        sid.item_code,
        sid.delivery_date,
        sid.transaction_date,
        spd.payment_amount
    FROM silver.silver_salesorder_details sd
    JOIN silver.silver_salesorder_item_details sid
    ON sd.name = sid.it_sales_order_name
    JOIN silver.silver_salesorder_payment spd
    ON sd.name = spd.py_sales_order_name
    """
    df = conn.execute(query).df()  # Convert result to Pandas DataFrame
    conn.close()
    return df

# Display Data Based on Sidebar Section Choice
if section == "Sales Dashboard":
    st.title("📊 Sales Dashboard")
    df_salesorder = load_salesorder_data()
    st.dataframe(df_salesorder)

elif section == "Payment Dashboard":
    st.title("📊 Payment Dashboard")
    df_payment = load_payment_data()
    st.dataframe(df_payment)

elif section == "Customer Dashboard":
    st.title("📊 Customer Dashboard")
    df_customer = load_customer_data()
    st.dataframe(df_customer)

elif section == "Sales Order & Item":
    st.title("📊 Sales Order & Item Dashboard")
    df_salesorder_item = load_salesorder_item_data()
    st.dataframe(df_salesorder_item)

elif section == "Sales Order, Item & Payment":
    st.title("📊 Sales Order, Item & Payment Dashboard")
    df_salesorder_item_payment = load_salesorder_item_payment_data()
    st.dataframe(df_salesorder_item_payment)
