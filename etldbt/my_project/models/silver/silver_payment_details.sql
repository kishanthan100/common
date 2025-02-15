{{ config(schema='silver') }}

WITH source AS (
    SELECT 
    
    name, owner, creation, modified, modified_by, docstatus, idx, naming_series,payment_type, payment_order_status,
                posting_date, company, mode_of_payment, party_type, party, party_name, book_advance_payments_in_separate_party_account, reconcile_on_advance_payment_date, party_balance, paid_from,
                paid_from_account_type, paid_from_account_currency, paid_from_account_balance, paid_to, paid_to_account_type, paid_to_account_currency, paid_to_account_balance, paid_amount, paid_amount_after_tax, source_exchange_rate,
                base_paid_amount, base_paid_amount_after_tax, received_amount, received_amount_after_tax, target_exchange_rate, base_received_amount, base_received_amount_after_tax, total_allocated_amount, base_total_allocated_amount, unallocated_amount,                
                difference_amount, apply_tax_withholding_amount, base_total_taxes_and_charges, total_taxes_and_charges, reference_date, status,                
                custom_remarks, remarks, base_in_words, is_opening, in_words, title, doctype  
        
        
          -- Extract item_code from JSON
    FROM {{ ref('stg_payment_entry') }} 
    WHERE name IS NOT NULL
)

SELECT  * FROM source
