import duckdb
import os
import json

# Define file paths
STORESALES = os.path.join(os.path.dirname(__file__), "../jsondata/erp_data.json")
PAYMENT = os.path.join(os.path.dirname(__file__), "../jsondata/payment_details.json")
CUSTOMER = os.path.join(os.path.dirname(__file__), "../jsondata/customer_detailes.json")
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "../duckdb/erp.duckdb")

# Ensure DuckDB folder exists
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

def load_salesorder_to_duckdb():
    """Reads erp_data.json and stores sales order details in DuckDB with proper schema."""
    conn = duckdb.connect(DUCKDB_PATH)

    # Define the table schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_order (
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

    # Check if JSON file exists
    if not os.path.exists(STORESALES):
        print(f"⚠️ JSON file not found: {STORESALES}")
        return

    # Read the JSON file
    with open(STORESALES, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Ensure data is a list of dictionaries
    if isinstance(data, dict):  
        data = [data]  # Convert to list if it's a single dictionary

    # Insert data into DuckDB
    for sales in data:
        conn.execute(""" 
            INSERT INTO sales_order (
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

    result = conn.execute('SELECT * FROM sales_order').fetchdf()
    print("last",result)
    conn.close()
######################################################################################################

def load_payment_to_duckdb():
    """Reads payment_details.json and stores payment details details in DuckDB with proper schema."""
    conn = duckdb.connect(DUCKDB_PATH)

    # Define the table schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_details (
            name TEXT NULL,
            owner TEXT NULL,
            creation DATETIME NULL,
            modified DATETIME NULL,
            modified_by TEXT NULL,
            docstatus INT NULL,
            idx INT NULL,
            naming_series TEXT NULL,
            payment_type TEXT NULL,
            payment_order_status TEXT NULL,
            posting_date DATE NULL,
            company TEXT NULL,
            mode_of_payment TEXT NULL, 
            party_type TEXT NULL,
            party TEXT NULL,
            party_name TEXT NULL,
            book_advance_payments_in_separate_party_account INT NULL,
            reconcile_on_advance_payment_date INT NULL, 
            party_balance FLOAT NULL,
            paid_from TEXT NULL, 
            paid_from_account_type TEXT NULL,
            paid_from_account_currency TEXT NULL,
            paid_from_account_balance FLOAT NULL,
            paid_to TEXT NULL,
            paid_to_account_type TEXT NULL,
            paid_to_account_currency TEXT NULL,
            paid_to_account_balance FLOAT NULL,
            paid_amount FLOAT NULL,
            paid_amount_after_tax FLOAT NULL,
            source_exchange_rate FLOAT NULL,
            base_paid_amount FLOAT NULL,
            base_paid_amount_after_tax FLOAT NULL,
            received_amount FLOAT NULL,
            received_amount_after_tax FLOAT NULL,
            target_exchange_rate FLOAT NULL,
            base_received_amount FLOAT NULL,
            base_received_amount_after_tax FLOAT NULL,
            total_allocated_amount FLOAT NULL,
            base_total_allocated_amount FLOAT NULL,
            unallocated_amount FLOAT NULL,
            difference_amount FLOAT NULL,
            apply_tax_withholding_amount INT NULL,
            base_total_taxes_and_charges FLOAT NULL,
            total_taxes_and_charges FLOAT NULL,
            reference_date DATE NULL,
            status TEXT NULL,
            custom_remarks INT NULL,
            remarks TEXT NULL,
            base_in_words TEXT NULL,
            is_opening TEXT NULL,
            in_words TEXT NULL,
            title TEXT NULL,
            doctype TEXT NULL,
            references_data JSON NULL,
            taxes JSON NULL,
            deductions JSON NULL,
        )
    """)

    # Check if JSON file exists
    if not os.path.exists(PAYMENT):
        print(f"⚠️ JSON file not found: {PAYMENT}")
        return

    # Read the JSON file
    with open(PAYMENT, "r", encoding="utf-8") as f:
        data1 = json.load(f)

    # Ensure data is a list of dictionaries
    if isinstance(data1, dict):  
        data1 = [data1]  # Convert to list if it's a single dictionary

    # Insert data into DuckDB
    for payment in data1:
        conn.execute(""" 
            INSERT INTO payment_details (
                name, owner, creation, modified, modified_by, docstatus, idx, naming_series,payment_type, payment_order_status,
                posting_date, company, mode_of_payment, party_type, party, party_name, book_advance_payments_in_separate_party_account, reconcile_on_advance_payment_date, party_balance, paid_from,
                paid_from_account_type, paid_from_account_currency, paid_from_account_balance, paid_to, paid_to_account_type, paid_to_account_currency, paid_to_account_balance, paid_amount, paid_amount_after_tax, source_exchange_rate,
                base_paid_amount, base_paid_amount_after_tax, received_amount, received_amount_after_tax, target_exchange_rate, base_received_amount, base_received_amount_after_tax, total_allocated_amount, base_total_allocated_amount, unallocated_amount,                
                difference_amount, apply_tax_withholding_amount, base_total_taxes_and_charges, total_taxes_and_charges, reference_date, status,                
                custom_remarks, remarks, base_in_words, is_opening, in_words, title, doctype, references_data,taxes ,deductions
                 
                
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                
            )

        """, (
            payment.get('name', None), payment.get('owner', None), 
            payment.get('creation', None), payment.get('modified', None), 
            payment.get('modified_by', None), payment.get('docstatus', None),
            payment.get('idx', None), payment.get('naming_series', None),  
            payment.get('payment_type', None), payment.get('payment_order_status', None),
            payment.get('posting_date', None), payment.get('company', None), 
            payment.get('mode_of_payment', None), payment.get('party_type', None), 
            payment.get('party', None), payment.get('party_name', None),
            payment.get('book_advance_payments_in_separate_party_account', None), payment.get('reconcile_on_advance_payment_date', None), 
            payment.get('party_balance', None), payment.get('paid_from', None), 
            payment.get('paid_from_account_type', None), payment.get('paid_from_account_currency', None), 
            payment.get('paid_from_account_balance', None), payment.get('paid_to', None), 
            payment.get('paid_to_account_type', None), payment.get('paid_to_account_currency', None), 
            payment.get('paid_to_account_balance', None), payment.get('paid_amount', None), 
            payment.get('paid_amount_after_tax', None), payment.get('source_exchange_rate', None), 
            payment.get('base_paid_amount', None), payment.get('base_paid_amount_after_tax', None), 
            payment.get('received_amount', None), payment.get('received_amount_after_tax', None), 
            payment.get('target_exchange_rate', None), payment.get('base_received_amount', None), 
            payment.get('base_received_amount_after_tax', None), payment.get('total_allocated_amount', None),
            payment.get('base_total_allocated_amount', None), payment.get('unallocated_amount', None), 
            payment.get('difference_amount', None), payment.get('apply_tax_withholding_amount', None), 
            payment.get('base_total_taxes_and_charges', None), payment.get('total_taxes_and_charges', None),
            payment.get('reference_date', None), payment.get('status', None), 
            payment.get('custom_remarks', None), payment.get('remarks', None), 
            payment.get('base_in_words', None), payment.get('is_opening', None), 
            payment.get('in_words', None), payment.get('title', None),
            payment.get('doctype', None), 
            json.dumps(payment.get('references', [])),
            json.dumps(payment.get('deductions', [])),  # Convert items list to JSON string
            json.dumps(payment.get('taxes', []))
            
         
           
        ))
    print("payment table created")
    result1 = conn.execute('SELECT * FROM payment_details').fetchdf()
    print("last",result1)
    
    conn.close()
######################################################################################################

def load_customer_to_duckdb():
    """Reads customer_details.json and stores customer details details in DuckDB with proper schema."""
    conn = duckdb.connect(DUCKDB_PATH)

    # Define the table schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS customer_details (
            name TEXT,  
            owner TEXT,  
            creation DATETIME,  
            modified DATETIME,  
            modified_by TEXT,  
            docstatus INT,  
            idx INT,  
            naming_series TEXT,  
            customer_name TEXT,  
            customer_type TEXT,  
            customer_group TEXT,  
            territory TEXT,  
            is_internal_customer INT,  
            language TEXT,  
            customer_primary_address TEXT,  
            primary_address TEXT,  
            default_commission_rate FLOAT,  
            so_required INT,  
            dn_required INT,  
            is_frozen INT,  
            disabled INT,  
            doctype TEXT,  
            credit_limits JSON,  
            accounts JSON,  
            portal_users JSON,  
            sales_team JSON,  
            companies JSON  

        )
    """)

    # Check if JSON file exists
    if not os.path.exists(CUSTOMER):
        print(f"⚠️ JSON file not found: {CUSTOMER}")
        return

    # Read the JSON file
    with open(CUSTOMER, "r", encoding="utf-8") as f:
        data1 = json.load(f)

    # Ensure data is a list of dictionaries
    if isinstance(data1, dict):  
        data1 = [data1]  # Convert to list if it's a single dictionary

    # Insert data into DuckDB
    for customer in data1:
        conn.execute(""" 
            INSERT INTO customer_details (
               
                name, owner, creation, modified, modified_by, docstatus, idx, naming_series, 
                customer_name, customer_type, customer_group, territory, is_internal_customer, 
                language, customer_primary_address, primary_address, default_commission_rate, 
                so_required, dn_required, is_frozen, disabled, doctype, credit_limits, 
                accounts, portal_users, sales_team, companies


                 
                
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?, ?, ?, ?, ?, 
                
            )

        """, (
            customer.get("name", None), customer.get("owner", None), customer.get("creation", None),
            customer.get("modified", None), customer.get("modified_by", None), customer.get("docstatus", None),
            customer.get("idx", None), customer.get("naming_series", None), customer.get("customer_name", None),
            customer.get("customer_type", None), customer.get("customer_group", None), customer.get("territory", None),
            customer.get("is_internal_customer", None), customer.get("language", None), 
            customer.get("customer_primary_address", None), customer.get("primary_address", None), 
            customer.get("default_commission_rate", None), customer.get("so_required", None), 
            customer.get("dn_required", None), customer.get("is_frozen", None), customer.get("disabled", None), 
            customer.get("doctype", None),
            json.dumps(customer.get('accounts', [])),
            json.dumps(customer.get('sales_team', [])),  # Convert items list to JSON string
            json.dumps(customer.get('credit_limits', [])),
            json.dumps(customer.get('portal_users', [])),
            json.dumps(customer.get('companies', []))
            
         
           
        ))
    print("customer_details table created")
    result2 = conn.execute('SELECT * FROM customer_details').fetchdf()
    print("last",result2)
    
    conn.close()