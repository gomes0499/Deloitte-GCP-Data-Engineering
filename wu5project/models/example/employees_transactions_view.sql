SELECT
    e.*,
    ft.transaction_id,
    ft.transaction_date,
    ft.transaction_type,
    ft.amount,
    ft.currency,
    ft.description
FROM {{ source('wu5_raw_data', 'employees') }} e
JOIN {{ source('wu5_raw_data', 'financial_transactions') }} ft ON e.employee_id = ft.employee_id
