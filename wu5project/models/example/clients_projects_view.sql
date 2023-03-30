SELECT
    c.*,
    p.project_id,
    p.project_name,
    p.project_type,
    p.start_date AS project_start_date,
    p.end_date AS project_end_date,
    p.project_status
FROM {{ source('wu5_raw_data', 'clients') }} c
JOIN {{ source('wu5_raw_data', 'projects') }} p ON c.client_id = p.client_id