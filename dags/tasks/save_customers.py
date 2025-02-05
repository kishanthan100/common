import json
import main as main
import os
import duckdb 
# fetch payments data

    
def fetch_erp_customer_api():

    connection=main.Connect()
    api=main.API()
    resultsdata=[]
    doctype="Customer"
    results=api.get_dataframe(doctype)
    for i in results.get('name'):
        resultsdata.append(i)
    resultsdata = resultsdata[:20]
    
    details_of_sales_order=[]
    for res in resultsdata:
        details = api.get_doc_sales(doctype, res)
        details_of_sales_order.append(details)
 
    
    if details_of_sales_order:
        
        data = details_of_sales_order 
        file_path = "/opt/airflow/apidata/erp_customer_data.json" 

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(data, f)

        # Return the file path
        return file_path
    else:
        raise Exception(f"no data found in results") 


def savecustomer_data_to_duckdb(file_path):
    DUCKDB_PATH = "/opt/airflow/apidata/customer_database.duckdb" 
    # Connect to DuckDB (this will create the database file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_PATH)

    # Read the data from the JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)

    save_customers(conn,data)


def save_customers(conn, data):
    """Creates and inserts data into the customers table."""

    # Create the customers table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            name TEXT NULL,
            owner TEXT NULL,
            creation TIMESTAMP NULL,
            modified TIMESTAMP NULL,
            modified_by TEXT NULL,
            docstatus INTEGER NULL,
            idx INTEGER NULL,
            naming_series TEXT NULL,
            customer_name TEXT NULL,
            customer_type TEXT NULL,
            customer_group TEXT NULL,
            territory TEXT NULL,
            is_internal_customer INTEGER NULL,
            language TEXT NULL,
            customer_primary_address TEXT NULL,
            primary_address TEXT NULL,
            default_commission_rate FLOAT NULL,
            so_required INTEGER NULL,
            dn_required INTEGER NULL,
            is_frozen INTEGER NULL,
            disabled INTEGER NULL,
            doctype TEXT NULL,
            credit_limits JSON NULL,
            companies JSON NULL,
            sales_team JSON NULL,
            accounts JSON NULL,
            portal_users JSON NULL
        )
    """)

    # Fetch table structure for debugging
    t = conn.execute('DESCRIBE customers;').fetchall()
    print('Table Structure:', t)

    # Insert data into the customers table
    for customer in data:
        conn.execute("""
            INSERT INTO customers (
                name, owner, creation, modified, modified_by, docstatus, idx,
                naming_series, customer_name, customer_type, customer_group, territory, 
                is_internal_customer, language, customer_primary_address, primary_address, 
                default_commission_rate, so_required, dn_required, is_frozen, disabled, 
                doctype, credit_limits, companies, sales_team, accounts, portal_users
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, (
            customer.get('name', None), customer.get('owner', None), 
            customer.get('creation', None), customer.get('modified', None), 
            customer.get('modified_by', None), customer.get('docstatus', None),
            customer.get('idx', None), customer.get('naming_series', None), 
            customer.get('customer_name', None), customer.get('customer_type', None), 
            customer.get('customer_group', None), customer.get('territory', None), 
            customer.get('is_internal_customer', None), customer.get('language', None), 
            customer.get('customer_primary_address', None), customer.get('primary_address', None), 
            customer.get('default_commission_rate', None), customer.get('so_required', None), 
            customer.get('dn_required', None), customer.get('is_frozen', None), 
            customer.get('disabled', None), customer.get('doctype', None), 
            json.dumps(customer.get('credit_limits', [])), 
            json.dumps(customer.get('companies', [])), 
            json.dumps(customer.get('sales_team', [])), 
            json.dumps(customer.get('accounts', [])), 
            json.dumps(customer.get('portal_users', []))
        ))

    # Fetch and print inserted data for debugging
    result = conn.execute('SELECT * FROM customers').fetchdf()
    print("Inserted Data:", result)

    # Close the connection
    conn.close()
