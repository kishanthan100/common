from scripts import main
import os, json, requests


def fetch_salesorder():
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
        file_path = "/opt/airflow/jsondata/erp_data.json" 
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        # Return the file path  
    print(f"slaes ingested")
   
    
######################################################################################################
def fetch_paymententry():
    api=main.API()
    doctype = 'Payment Entry'
    final = api.get_dataframe(doctype)
    paymentorder_name = []
    for i in final.get('name'):
        paymentorder_name.append(i)
    #print(paymentorder_name)
    final_paymentdetails = []
    for name in paymentorder_name:
        payment_details = api.get_doc_sales(doctype, name)
        final_paymentdetails.append(payment_details)
    if final_paymentdetails:
        payment = final_paymentdetails 
        file_path= "/opt/airflow/jsondata/payment_details.json" 
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(payment, f, indent=4)
        # Return the file path
        return file_path
    print(f"payment ingested")
######################################################################################################
def fetch_customerdetails():
    api=main.API()
    doctype = 'Customer'
    final = api.get_dataframe(doctype)
    customer_name = []
    for i in final.get('name'):
        customer_name.append(i)
    #print(customer_name)
    customer_details = []
    for name in customer_name:
        customer = api.get_doc_sales(doctype, name)
        customer_details.append(customer)
    if customer_details:
        payment = customer_details 
        file_path= "/opt/airflow/jsondata/customer_detailes.json" 
        # Ensure the directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        # Save the data to the file
        with open(file_path, 'w') as f:
            json.dump(payment, f, indent=4)
        # Return the file path
        #return file_path
    print(f"customer ingested")