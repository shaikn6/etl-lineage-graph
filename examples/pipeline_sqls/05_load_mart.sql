-- Pipeline: retail_etl | Step 5: Load final data mart
-- Applies business rules: rolling 90-day revenue window and margin thresholds.

INSERT INTO mart.sales_performance (
    report_date,
    region_code,
    category,
    customer_segment,
    total_orders,
    total_revenue,
    revenue_90d,
    margin_pct,
    performance_tier
)
WITH rolling_window AS (
    SELECT
        dss.order_date,
        dss.region_code,
        dss.category,
        dss.customer_segment,
        dss.total_orders,
        dss.total_revenue,
        dss.total_gross_margin,
        SUM(dss.total_revenue) OVER (
            PARTITION BY dss.region_code, dss.category, dss.customer_segment
            ORDER BY dss.order_date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS revenue_90d
    FROM warehouse.daily_sales_summary dss
)
SELECT
    rw.order_date                                                 AS report_date,
    rw.region_code,
    rw.category,
    rw.customer_segment,
    rw.total_orders,
    rw.total_revenue,
    rw.revenue_90d,
    ROUND(rw.total_gross_margin / NULLIF(rw.total_revenue, 0), 4) AS margin_pct,
    CASE
        WHEN rw.revenue_90d >= 500000 THEN 'PLATINUM'
        WHEN rw.revenue_90d >= 100000 THEN 'GOLD'
        WHEN rw.revenue_90d >= 10000  THEN 'SILVER'
        ELSE                               'BRONZE'
    END                                                           AS performance_tier
FROM rolling_window rw
WHERE rw.order_date = CURRENT_DATE - INTERVAL '1 day';
