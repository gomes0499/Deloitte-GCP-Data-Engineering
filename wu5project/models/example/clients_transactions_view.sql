SELECT
    c.*,
    ft.transaction_id,
    ft.transaction_date,
    ft.transaction_type,
    ft.amount,
    ft.currency,
    ft.description
FROM {{ source('wu5_raw_data', 'clients') }} c
JOIN {{ source('wu5_raw_data', 'financial_transactions') }} ft ON c.client_id = ft.client_id
