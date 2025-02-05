
import json
import main as main
import os
import duckdb 


def fetch_erp_payment_api():

    connection=main.Connect()
    api=main.API()
    resultsdata=[]
    doctype="Payment Entry"
    results=api.get_dataframe(doctype)
    for i in results.get('name'):
        resultsdata.append(i)
    resultsdata = resultsdata[:20]
    
    details_of_payment=[]
    for res in resultsdata:
        details = api.get_doc_sales(doctype, res)
        details_of_payment.append(details)
 
    
    if details_of_payment:
        
        data = details_of_payment 
        file_path = "/opt/airflow/apidata/erp_payment_data.json" 

        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(data, f)

        # Return the file path
        return file_path
    else:
        raise Exception(f"no data found in results") 
    



def savepayment_data_to_duckdb(file_path):
    DUCKDB_PATH = "/opt/airflow/apidata/payment_database.duckdb" 
    # Connect to DuckDB (this will create the database file if it doesn't exist)
    conn = duckdb.connect(DUCKDB_PATH)

    # Read the data from the JSON file
    with open(file_path, 'r') as f:
        data = json.load(f)

    save_payments(conn,data)


def save_payments(conn, data):
   
    
    # Create the payments table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            name TEXT NULL,
            owner TEXT NULL,
            creation TIMESTAMP NULL,
            modified TIMESTAMP NULL,
            modified_by TEXT NULL,
            docstatus INTEGER NULL,
            idx INTEGER NULL,
            naming_series TEXT NULL,
            payment_type TEXT NULL,
            payment_order_status TEXT NULL,
            posting_date DATE NULL,
            company TEXT NULL,
            mode_of_payment TEXT NULL,
            party_type TEXT NULL,
            party TEXT NULL,
            party_name TEXT NULL,
            book_advance_payments_in_separate_party_account INTEGER NULL,
            reconcile_on_advance_payment_date INTEGER NULL,
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
            apply_tax_withholding_amount INTEGER NULL,
            base_total_taxes_and_charges FLOAT NULL,
            total_taxes_and_charges FLOAT NULL,
            reference_date DATE NULL,
            status TEXT NULL,
            custom_remarks INTEGER NULL,
            remarks TEXT NULL,
            base_in_words TEXT NULL,
            is_opening TEXT NULL,
            in_words TEXT NULL,
            title TEXT NULL,
            doctype TEXT NULL,
            deductions JSON NULL,
            taxes JSON NULL,
            payment_references JSON NULL,
        )
    """)

    # Fetch column details for debugging
    t = conn.execute('DESCRIBE payments;').fetchall()
    print('Table Structure:', t)

    # Insert data into the payments table
    for payment in data:
        conn.execute("""
            INSERT INTO payments (
                name, owner, creation, modified, modified_by, docstatus, idx,
                naming_series, payment_type, payment_order_status, posting_date, 
                company, mode_of_payment, party_type, party, party_name,
                book_advance_payments_in_separate_party_account,
                reconcile_on_advance_payment_date, party_balance, paid_from, 
                paid_from_account_type, paid_from_account_currency, 
                paid_from_account_balance, paid_to, paid_to_account_type, 
                paid_to_account_currency, paid_to_account_balance, paid_amount, 
                paid_amount_after_tax, source_exchange_rate, base_paid_amount, 
                base_paid_amount_after_tax, received_amount, received_amount_after_tax, 
                target_exchange_rate, base_received_amount, base_received_amount_after_tax, 
                total_allocated_amount, base_total_allocated_amount, unallocated_amount, 
                difference_amount, apply_tax_withholding_amount, 
                base_total_taxes_and_charges, total_taxes_and_charges, 
                reference_date, status, custom_remarks, remarks, base_in_words, 
                is_opening, in_words, title, doctype, deductions, taxes, payment_references
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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
            payment.get('book_advance_payments_in_separate_party_account', None),
            payment.get('reconcile_on_advance_payment_date', None), 
            payment.get('party_balance', None), payment.get('paid_from', None), 
            payment.get('paid_from_account_type', None), 
            payment.get('paid_from_account_currency', None), 
            payment.get('paid_from_account_balance', None), 
            payment.get('paid_to', None), payment.get('paid_to_account_type', None), 
            payment.get('paid_to_account_currency', None), 
            payment.get('paid_to_account_balance', None), 
            payment.get('paid_amount', None), 
            payment.get('paid_amount_after_tax', None), 
            payment.get('source_exchange_rate', None), 
            payment.get('base_paid_amount', None), 
            payment.get('base_paid_amount_after_tax', None), 
            payment.get('received_amount', None), 
            payment.get('received_amount_after_tax', None), 
            payment.get('target_exchange_rate', None), 
            payment.get('base_received_amount', None), 
            payment.get('base_received_amount_after_tax', None), 
            payment.get('total_allocated_amount', None), 
            payment.get('base_total_allocated_amount', None), 
            payment.get('unallocated_amount', None), 
            payment.get('difference_amount', None), 
            payment.get('apply_tax_withholding_amount', None), 
            payment.get('base_total_taxes_and_charges', None), 
            payment.get('total_taxes_and_charges', None), 
            payment.get('reference_date', None), payment.get('status', None), 
            payment.get('custom_remarks', None), payment.get('remarks', None), 
            payment.get('base_in_words', None), payment.get('is_opening', None), 
            payment.get('in_words', None), payment.get('title', None), 
            payment.get('doctype', None), 
            json.dumps(payment.get('deductions', [])), 
            json.dumps(payment.get('taxes', [])), 
            json.dumps(payment.get('references', []))
        ))

    # Fetch and print inserted data for debugging
    result = conn.execute('SELECT * FROM payments').fetchdf()
    print("Inserted Data:", result)

    # Close the connection
    conn.close()

