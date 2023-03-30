SELECT
    p.*,
    ft.transaction_id,
    ft.transaction_date,
    ft.transaction_type,
    ft.amount,
    ft.currency,
    ft.description
FROM {{ source('wu5_raw_data', 'projects') }} p
JOIN {{ source('wu5_raw_data', 'financial_transactions') }} ft ON p.project_id = ft.project_id