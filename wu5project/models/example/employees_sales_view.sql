SELECT
    e.*,
    s.sales_id,
    s.product_service_id,
    s.sale_date,
    s.quantity,
    s.unit_price,
    s.discount,
    s.total_amount
FROM {{ source('wu5_raw_data', 'employees') }} e
JOIN {{ source('wu5_raw_data', 'sales') }} s ON e.employee_id = s.employee_id