WITH companies AS (
    SELECT DISTINCT
        company AS company_id,
        company
    FROM {{ source('apidata', 'sales') }}
)

SELECT * FROM companies
