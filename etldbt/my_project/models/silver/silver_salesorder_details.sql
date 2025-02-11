{{ config(schema='silver') }}

WITH source AS (
    SELECT creation, 
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
                loyalty_points, loyalty_amount, group_same_items
    FROM {{ ref('stg_sales_order') }}  -- ref() will auto-resolve schema
)

SELECT * FROM source
