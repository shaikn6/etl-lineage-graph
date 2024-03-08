-- Pipeline: retail_etl | Step 2: Normalise customer names and assign segment
-- Derives customer_segment from lifetime_value using a CASE expression.

INSERT INTO staging.dim_customers (
    customer_id,
    full_name,
    email,
    customer_segment,
    country_code,
    is_active
)
SELECT
    c.customer_id,
    INITCAP(TRIM(c.first_name || ' ' || c.last_name)) AS full_name,
    LOWER(c.email)                                     AS email,
    CASE
        WHEN c.lifetime_value >= 10000 THEN 'PREMIUM'
        WHEN c.lifetime_value >= 1000  THEN 'STANDARD'
        ELSE                                'BASIC'
    END                                                AS customer_segment,
    c.country_code,
    c.is_active
FROM source.customers c
WHERE c.is_active = TRUE;
