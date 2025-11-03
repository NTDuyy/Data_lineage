{{ config(materialized='view') }}

SELECT
   c.c_name AS customer_name,
   s.s_name AS supplier_name,
   p.p_name AS product_name,
   lo.lo_revenue AS revenue,
   d.d_date AS order_date,
   getdate() AS latest_update
FROM {{ source('redshift', 'lineorder') }} lo
JOIN {{ source('redshift', 'customer') }} c ON lo.lo_custkey = c.c_custkey
JOIN {{ source('redshift', 'supplier') }} s ON lo.lo_suppkey = s.s_suppkey
JOIN {{ source('redshift', 'part') }} p ON lo.lo_partkey = p.p_partkey
JOIN {{ source('redshift', 'dwdate') }} d ON lo.lo_orderdate = d.d_datekey