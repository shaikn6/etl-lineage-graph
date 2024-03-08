-- Pipeline: retail_etl | Step 4: Aggregate to daily sales summary
-- Groups enriched_orders + dim_customers to produce daily_sales_summary.

INSERT INTO warehouse.daily_sales_summary (
    order_date,
    region_code,
    category,
    customer_segment,
    total_orders,
    total_quantity,
    total_revenue,
    total_cost,
    total_gross_margin
)
SELECT
    eo.order_date,
    eo.region_code,
    eo.category,
    dc.customer_segment,
    COUNT(DISTINCT eo.order_id)            AS total_orders,
    SUM(eo.quantity)                       AS total_quantity,
    SUM(eo.quantity * eo.unit_price)       AS total_revenue,
    SUM(eo.quantity * eo.unit_cost)        AS total_cost,
    SUM(eo.quantity * eo.gross_margin)     AS total_gross_margin
FROM staging.enriched_orders  eo
JOIN staging.dim_customers    dc ON eo.customer_id = dc.customer_id
WHERE eo.order_status = 'COMPLETED'
GROUP BY
    eo.order_date,
    eo.region_code,
    eo.category,
    dc.customer_segment;
