-- Pipeline: retail_etl | Step 3: Enrich staging orders with product dimension
-- Joins raw_orders → product catalogue to resolve category and margin.

INSERT INTO staging.enriched_orders (
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    order_date,
    quantity,
    unit_price,
    unit_cost,
    gross_margin,
    order_status,
    region_code
)
SELECT
    ro.order_id,
    ro.customer_id,
    ro.product_id,
    p.product_name,
    p.category,
    ro.order_date,
    ro.quantity,
    ro.unit_price,
    p.unit_cost,
    (ro.unit_price - p.unit_cost)  AS gross_margin,
    ro.order_status,
    ro.region_code
FROM staging.raw_orders    ro
JOIN source.products        p  ON ro.product_id = p.product_id
WHERE p.is_active = TRUE;
