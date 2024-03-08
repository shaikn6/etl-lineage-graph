-- Pipeline: retail_etl | Step 1: Extract raw orders from source system
-- Filters last 90 days to keep incremental loads manageable.

INSERT INTO staging.raw_orders (
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    order_status,
    region_code,
    extracted_at
)
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.status          AS order_status,
    o.region_code,
    NOW()             AS extracted_at
FROM source.orders o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
  AND o.status NOT IN ('CANCELLED', 'TEST');
