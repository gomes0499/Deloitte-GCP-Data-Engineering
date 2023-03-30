SELECT
    cb.*,
    s.sales_id,
    s.product_service_id,
    s.sale_date,
    s.quantity,
    s.unit_price,
    s.discount,
    s.total_amount
FROM {{ source('wu5_raw_data', 'customer_behavior') }} cb
JOIN {{ source('wu5_raw_data', 'sales') }} s ON cb.customer_id = s.customer_id