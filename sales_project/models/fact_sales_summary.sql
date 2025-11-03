{{ config(materialized='table') }}

SELECT
    supplier_name,
    DATE_TRUNC('month', order_date) AS month,
    SUM(revenue) AS total_revenue,
    getdate() AS latest_update
FROM {{ ref('stg_sales_data') }}
GROUP BY 1, 2
ORDER BY 2, 1