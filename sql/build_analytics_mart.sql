CREATE SCHEMA IF NOT EXISTS analytics;


CREATE TABLE IF NOT EXISTS analytics.dim_customers 
AS 
WITH ranked_customers AS (
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER(PARTITION BY customer_unique_id ORDER BY customer_state, customer_city) AS customer_rank
    FROM raw_data.customers
)
SELECT 
    customer_unique_id,
    customer_city,
    customer_state
FROM ranked_customers
WHERE customer_rank = 1;

ALTER TABLE analytics.dim_customers 
ADD CONSTRAINT dim_customers_pkey PRIMARY KEY (customer_unique_id);


CREATE TABLE IF NOT EXISTS analytics.dim_products
AS
WITH ranked_products AS (
    SELECT
        product_id,
        product_category_name,
        product_weight_g,
        ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY product_category_name) AS product_rank
    FROM raw_data.products
)
SELECT
    product_id,
    product_category_name,
    product_weight_g
FROM ranked_products
WHERE product_rank = 1;


ALTER TABLE analytics.dim_products
ADD CONSTRAINT dim_products_pkey PRIMARY KEY (product_id);


CREATE TABLE IF NOT EXISTS analytics.fact_orders
AS
WITH aggregated_payments AS (
    SELECT
        order_id,
        payment_type,
        payment_value,
        ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY payment_sequential) AS payment_rank
    FROM raw_data.order_payments
)
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    c.customer_unique_id,
    o.order_purchase_timestamp,
    oi.price,
    oi.freight_value,
    ap.payment_type,
    ap.payment_value 
FROM raw_data.order_items oi
JOIN raw_data.products p ON oi.product_id = p.product_id
JOIN raw_data.orders o ON oi.order_id = o.order_id
JOIN raw_data.customers c ON o.customer_id = c.customer_id
JOIN aggregated_payments ap ON ap.order_id = oi.order_id
WHERE payment_rank = 1;


ALTER TABLE analytics.fact_orders
ADD CONSTRAINT fk_product
FOREIGN KEY (product_id)
REFERENCES analytics.dim_products (product_id);

ALTER TABLE analytics.fact_orders
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_unique_id)
REFERENCES analytics.dim_customers (customer_unique_id);