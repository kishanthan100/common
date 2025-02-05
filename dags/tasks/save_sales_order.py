import json
import main as main
import os

import duckdb 


def fetch_erp_sales_api():

    connection=main.Connect()
    api=main.API()
    resultsdata=[]
    doctype="Sales Order"
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
        file_path = "/opt/airflow/apidata/erp_sales_data.json" 

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(data, f)

        # Return the file path
        return file_path
    else:
        raise Exception(f"no data found in results") 
    

def savesales_data_to_duckdb(file_path):
    DUCKDB_PATH = "/opt/airflow/apidata/sales_database.duckdb" 
    # Connect to DuckDB (this will create the database file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_PATH)

    # Read the data from the JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)

    save_sales(conn,data)



def save_sales(conn,data):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            name TEXT NULL,
            owner TEXT NULL,
            creation TIMESTAMP NULL,
            modified TIMESTAMP NULL,
            modified_by TEXT NULL,
            docstatus INTEGER NULL,
            idx INTEGER NULL,
            workflow_state TEXT NULL,
            title TEXT NULL,
            naming_series TEXT NULL,
            customer TEXT NULL,
            customer_name TEXT NULL,
            order_type TEXT NULL,
            transaction_date DATE NULL,
            delivery_date DATE NULL,
            custom_shipping_date DATE NULL,
            company TEXT NULL,
            skip_delivery_note INTEGER NULL,
            currency TEXT NULL,
            conversion_rate FLOAT NULL,
            selling_price_list TEXT NULL,
            price_list_currency TEXT NULL,
            plc_conversion_rate FLOAT NULL,
            ignore_pricing_rule INTEGER NULL,
            reserve_stock INTEGER NULL,
            total_qty FLOAT NULL,
            total_net_weight FLOAT NULL,
            custom_total_buying_price FLOAT NULL,
            custom_total_margin_percentage FLOAT NULL,
            base_total FLOAT NULL,
            base_net_total FLOAT NULL,
            custom_total_margin_amount FLOAT NULL,
            total FLOAT NULL,
            net_total FLOAT NULL,
            tax_category TEXT NULL,
            base_total_taxes_and_charges FLOAT NULL,
            total_taxes_and_charges FLOAT NULL,
            base_grand_total FLOAT NULL,
            base_rounding_adjustment FLOAT NULL,
            base_rounded_total FLOAT NULL,
            base_in_words TEXT NULL,
            grand_total FLOAT NULL,
            rounding_adjustment FLOAT NULL,
            rounded_total FLOAT NULL,
            in_words TEXT NULL,
            advance_paid FLOAT NULL,
            disable_rounded_total INTEGER NULL,
            apply_discount_on TEXT NULL,
            base_discount_amount FLOAT NULL,
            additional_discount_percentage FLOAT NULL,
            discount_amount FLOAT NULL,
            customer_address TEXT NULL,
            address_display TEXT NULL,
            customer_group TEXT NULL,
            territory TEXT NULL,
            shipping_address_name TEXT NULL,
            shipping_address TEXT NULL,
            status TEXT NULL,
            delivery_status TEXT NULL,
            per_delivered FLOAT NULL,
            per_billed FLOAT NULL,
            per_picked FLOAT NULL,
            billing_status TEXT NULL,
            amount_eligible_for_commission FLOAT NULL,
            commission_rate FLOAT NULL,
            total_commission FLOAT NULL,
            loyalty_points INTEGER NULL,
            loyalty_amount FLOAT NULL,
            group_same_items INTEGER NULL,
            language TEXT NULL,
            is_internal_customer INTEGER NULL,
            party_account_currency TEXT NULL,
            doctype TEXT NULL,
            items JSON NULL,
            taxes JSON NULL,
            sales_team JSON NULL,
            payment_schedule JSON NULL,
            pricing_rules JSON NULL,
            packed_items JSON NULL,
            
           
        )
    """)

    t = conn.execute('DESCRIBE sales;').fetchall()
    print('descrr',t)
    # Insert user data into the table
    for sales in data:
        
        # Ensure all fields are either provided or set to None
        # print(f"Filtered keys in the sales data: {sales.keys()}")
        # print(f"Number of filtered values: {len(sales)}")
        # print(f"Filtered sales data: {sales}")
        # print(f"Types of values: {[type(value) for value in sales.values()]}")
       
       

        conn.execute(""" 
            INSERT INTO sales (
                name, owner, creation, modified, modified_by, docstatus, idx, 
                workflow_state, title, naming_series, customer, customer_name, 
                order_type, transaction_date, delivery_date, custom_shipping_date, 
                company, skip_delivery_note, currency, conversion_rate, 
                selling_price_list, price_list_currency, plc_conversion_rate, 
                ignore_pricing_rule, reserve_stock, total_qty, total_net_weight, 
                custom_total_buying_price, custom_total_margin_percentage, 
                base_total, base_net_total, custom_total_margin_amount, 
                total, net_total, tax_category, base_total_taxes_and_charges, 
                total_taxes_and_charges, base_grand_total, base_rounding_adjustment, 
                base_rounded_total, base_in_words, grand_total, rounding_adjustment, 
                rounded_total, in_words, advance_paid, disable_rounded_total, 
                apply_discount_on, base_discount_amount, additional_discount_percentage, 
                discount_amount, customer_address, address_display, customer_group, 
                territory, shipping_address_name, shipping_address, status, 
                delivery_status, per_delivered, per_billed, per_picked, billing_status, 
                amount_eligible_for_commission, commission_rate, total_commission, 
                loyalty_points, loyalty_amount, group_same_items, language, 
                is_internal_customer, party_account_currency, doctype,items,taxes,sales_team,payment_schedule,pricing_rules,packed_items
            ) VALUES (
             ?, ?, ?,?, ?, ?,?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            sales.get('name', None), sales.get('owner', None), 
            sales.get('creation', None), sales.get('modified', None), 
            sales.get('modified_by', None), sales.get('docstatus', None),
            sales.get('idx', None), sales.get('workflow_state', None), 
            sales.get('title', None), sales.get('naming_series', None), 
            sales.get('customer', None), sales.get('customer_name', None),
            sales.get('order_type', None), sales.get('transaction_date', None), 
            sales.get('delivery_date', None), sales.get('custom_shipping_date', None), 
            sales.get('company', None), sales.get('skip_delivery_note', None),
            sales.get('currency', None), sales.get('conversion_rate', None), 
            sales.get('selling_price_list', None), sales.get('price_list_currency', None), 
            sales.get('plc_conversion_rate', None), sales.get('ignore_pricing_rule', None), 
            sales.get('reserve_stock', None), sales.get('total_qty', None), 
            sales.get('total_net_weight', None), sales.get('custom_total_buying_price', None), 
            sales.get('custom_total_margin_percentage', None), sales.get('base_total', None), 
            sales.get('base_net_total', None), sales.get('custom_total_margin_amount', None), 
            sales.get('total', None), sales.get('net_total', None), 
            sales.get('tax_category', None), sales.get('base_total_taxes_and_charges', None), 
            sales.get('total_taxes_and_charges', None), sales.get('base_grand_total', None), 
            sales.get('base_rounding_adjustment', None), sales.get('base_rounded_total', None),
            sales.get('base_in_words', None), sales.get('grand_total', None), 
            sales.get('rounding_adjustment', None), sales.get('rounded_total', None), 
            sales.get('in_words', None), sales.get('advance_paid', None),
            sales.get('disable_rounded_total', None), sales.get('apply_discount_on', None), 
            sales.get('base_discount_amount', None), sales.get('additional_discount_percentage', None), 
            sales.get('discount_amount', None), sales.get('customer_address', None), 
            sales.get('address_display', None), sales.get('customer_group', None),
            sales.get('territory', None), sales.get('shipping_address_name', None), 
            sales.get('shipping_address', None), sales.get('status', None), 
            sales.get('delivery_status', None), sales.get('per_delivered', None),
            sales.get('per_billed', None), sales.get('per_picked', None), 
            sales.get('billing_status', None), sales.get('amount_eligible_for_commission', None), 
            sales.get('commission_rate', None), sales.get('total_commission', None), 
            sales.get('loyalty_points', None), sales.get('loyalty_amount', None), 
            sales.get('group_same_items', None), sales.get('language', None), 
            sales.get('is_internal_customer', None), sales.get('party_account_currency', None),
            sales.get('doctype', None), 
            json.dumps(sales.get('items', [])),  # Convert items list to JSON string
            json.dumps(sales.get('taxes', [])), 
            json.dumps(sales.get('sales_team', [])), 
            json.dumps(sales.get('payment_schedule', [])), 
            json.dumps(sales.get('pricing_rules', [])), 
            json.dumps(sales.get('packed_items', [])), 

         
           
        ))

    result = conn.execute('SELECT * FROM sales').fetchdf()
    print("last",result)
    conn.close()