-- ============================================================================
-- TPC-DS TEST QUERIES FOR SQL-TO-PYSPARK TRANSLATOR
-- ============================================================================
-- These queries test the translator against real TPC-DS schema and patterns
-- 
-- Organized by complexity:
-- - BASIC: Simple SELECT with WHERE and aggregates
-- - INTERMEDIATE: Joins, GROUP BY, window functions
-- - ADVANCED: Complex subqueries, CTEs, multi-table joins
--
-- Usage:
--   1. Start data generator: python generate_tpcds_data.py
--   2. Translate queries: python run_tpcds_tests.py
--   3. Verify results are correct
-- ============================================================================


-- ============================================================================
-- BASIC QUERIES (Simple SELECT, WHERE, basic aggregates)
-- ============================================================================

-- BASIC.1: Simple store sales query
SELECT ss_item_sk, ss_store_sk, ss_quantity, ss_sales_price
FROM store_sales
WHERE ss_quantity > 5
LIMIT 100;

-- BASIC.2: Customer lookup
SELECT c_customer_id, c_first_name, c_last_name, c_email_address
FROM customer
WHERE c_customer_sk > 10
LIMIT 50;

-- BASIC.3: Item price analysis
SELECT i_item_id, i_item_desc, i_current_price, i_category
FROM item
WHERE i_current_price > 100
ORDER BY i_current_price DESC
LIMIT 20;

-- BASIC.4: Store information
SELECT s_store_id, s_store_name, s_state, s_employees
FROM store
WHERE s_state IN ('CA', 'NY', 'TX');

-- BASIC.5: Date range query
SELECT d_date_sk, d_date, d_year, d_moy
FROM date_dim
WHERE d_year = 2021
LIMIT 100;

-- BASIC.6: Simple aggregation
SELECT COUNT(*) as total_sales, SUM(ss_quantity) as total_qty
FROM store_sales;

-- BASIC.7: COUNT DISTINCT
SELECT COUNT(DISTINCT ss_store_sk) as num_stores
FROM store_sales;

-- BASIC.8: WHERE with AND condition
SELECT ss_item_sk, ss_sales_price, ss_quantity
FROM store_sales
WHERE ss_quantity > 3 AND ss_sales_price > 50
LIMIT 50;

-- BASIC.9: WHERE with OR condition
SELECT c_customer_id, c_first_name
FROM customer
WHERE c_customer_sk < 100 OR c_customer_sk > 400;

-- BASIC.10: BETWEEN clause
SELECT i_item_id, i_current_price
FROM item
WHERE i_current_price BETWEEN 50 AND 200;


-- ============================================================================
-- INTERMEDIATE QUERIES (Joins, GROUP BY, Window functions)
-- ============================================================================

-- INT.1: Simple INNER JOIN (customer-sales)
SELECT c.c_customer_id, c.c_first_name, s.ss_quantity, s.ss_sales_price
FROM customer c
INNER JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
LIMIT 100;

-- INT.2: JOIN with aggregation
SELECT s.s_store_id, s.s_store_name, COUNT(*) as num_sales, SUM(ss.ss_sales_price) as total_revenue
FROM store s
INNER JOIN store_sales ss ON s.s_store_sk = ss.ss_store_sk
GROUP BY s.s_store_id, s.s_store_name
ORDER BY total_revenue DESC;

-- INT.3: Three-table JOIN
SELECT c.c_customer_id, s.s_store_name, i.i_item_desc, ss.ss_sales_price
FROM customer c
INNER JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
LIMIT 100;

-- INT.4: LEFT JOIN (store-sales)
SELECT s.s_store_id, s.s_store_name, COUNT(ss.ss_item_sk) as num_items_sold
FROM store s
LEFT JOIN store_sales ss ON s.s_store_sk = ss.ss_store_sk
GROUP BY s.s_store_id, s.s_store_name;

-- INT.5: GROUP BY with HAVING
SELECT ss_item_sk, COUNT(*) as sales_count, SUM(ss_quantity) as total_qty
FROM store_sales
GROUP BY ss_item_sk
HAVING COUNT(*) > 10
ORDER BY sales_count DESC
LIMIT 50;

-- INT.6: JOIN with aggregation and filtering
SELECT 
    d.d_year,
    d.d_moy,
    COUNT(*) as num_sales,
    SUM(ss.ss_sales_price) as total_sales
FROM store_sales ss
INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_year IN (2020, 2021)
GROUP BY d.d_year, d.d_moy
ORDER BY d.d_year, d.d_moy;

-- INT.7: Window function - ROW_NUMBER
SELECT 
    ss_item_sk,
    ss_sales_price,
    ROW_NUMBER() OVER (PARTITION BY ss_store_sk ORDER BY ss_sales_price DESC) as rank_in_store
FROM store_sales
WHERE ss_quantity > 2;

-- INT.8: Window function - RANK
SELECT 
    i_category,
    i_current_price,
    RANK() OVER (PARTITION BY i_category ORDER BY i_current_price DESC) as price_rank
FROM item;

-- INT.9: Multiple GROUP BY columns
SELECT 
    ss_store_sk,
    ss_item_sk,
    COUNT(*) as sales_count,
    SUM(ss_quantity) as total_qty,
    AVG(ss_sales_price) as avg_price
FROM store_sales
GROUP BY ss_store_sk, ss_item_sk
ORDER BY sales_count DESC
LIMIT 50;

-- INT.10: String functions in SELECT
SELECT 
    c_customer_id,
    UPPER(c_first_name) as first_name_upper,
    LOWER(c_last_name) as last_name_lower,
    CONCAT(c_first_name, ' ', c_last_name) as full_name
FROM customer
LIMIT 20;


-- ============================================================================
-- ADVANCED QUERIES (Subqueries, complex joins, CTEs)
-- ============================================================================

-- ADV.1: Subquery in WHERE clause
SELECT c_customer_id, c_first_name
FROM customer
WHERE c_customer_sk IN (
    SELECT ss_customer_sk FROM store_sales WHERE ss_quantity > 10
)
LIMIT 50;

-- ADV.2: Scalar subquery
SELECT 
    s_store_id,
    s_store_name,
    (SELECT COUNT(*) FROM store_sales WHERE ss_store_sk = s.s_store_sk) as num_sales
FROM store s
LIMIT 20;

-- ADV.3: Subquery in FROM clause
SELECT 
    store_id,
    store_name,
    total_sales
FROM (
    SELECT 
        s.s_store_id as store_id,
        s.s_store_name as store_name,
        SUM(ss.ss_sales_price) as total_sales
    FROM store s
    LEFT JOIN store_sales ss ON s.s_store_sk = ss.ss_store_sk
    GROUP BY s.s_store_id, s.s_store_name
) sales_summary
ORDER BY total_sales DESC
LIMIT 10;

-- ADV.4: JOIN with subquery
SELECT 
    c.c_customer_id,
    c.c_first_name,
    top_purchases.total_spent
FROM customer c
INNER JOIN (
    SELECT ss_customer_sk, SUM(ss_sales_price) as total_spent
    FROM store_sales
    GROUP BY ss_customer_sk
    HAVING SUM(ss_sales_price) > 5000
) top_purchases ON c.c_customer_sk = top_purchases.ss_customer_sk
ORDER BY top_purchases.total_spent DESC;

-- ADV.5: CASE WHEN for categorization
SELECT 
    i_item_id,
    i_current_price,
    CASE 
        WHEN i_current_price < 50 THEN 'Budget'
        WHEN i_current_price < 200 THEN 'Mid-Range'
        WHEN i_current_price < 500 THEN 'Premium'
        ELSE 'Luxury'
    END as price_category
FROM item;

-- ADV.6: Window function with ROWS BETWEEN
SELECT 
    d_date,
    SUM(ss_sales_price) as daily_sales,
    SUM(SUM(ss_sales_price)) OVER (ORDER BY d_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7day_sales
FROM store_sales ss
INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d_year = 2021
GROUP BY d_date
ORDER BY d_date;

-- ADV.7: Multiple aggregates with GROUP BY
SELECT 
    ss_store_sk,
    COUNT(*) as num_transactions,
    SUM(ss_quantity) as total_quantity,
    AVG(ss_sales_price) as avg_price,
    MIN(ss_sales_price) as min_price,
    MAX(ss_sales_price) as max_price
FROM store_sales
GROUP BY ss_store_sk
ORDER BY num_transactions DESC;

-- ADV.8: DISTINCT with aggregation
SELECT 
    ss_store_sk,
    COUNT(DISTINCT ss_customer_sk) as num_unique_customers,
    COUNT(DISTINCT ss_item_sk) as num_unique_items
FROM store_sales
GROUP BY ss_store_sk;

-- ADV.9: Complex three-table JOIN with aggregation
SELECT 
    d.d_year,
    d.d_moy,
    i.i_category,
    COUNT(*) as num_sales,
    SUM(ss.ss_quantity) as total_qty,
    SUM(ss.ss_sales_price) as total_revenue
FROM store_sales ss
INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE d.d_year IN (2020, 2021)
GROUP BY d.d_year, d.d_moy, i.i_category
ORDER BY d.d_year, d.d_moy;

-- ADV.10: LEFT JOIN with GROUP BY and HAVING
SELECT 
    s.s_store_id,
    s.s_store_name,
    COUNT(DISTINCT ss.ss_customer_sk) as num_customers,
    COUNT(*) as num_transactions,
    COALESCE(SUM(ss.ss_sales_price), 0) as total_revenue
FROM store s
LEFT JOIN store_sales ss ON s.s_store_sk = ss.ss_store_sk
GROUP BY s.s_store_id, s.s_store_name
HAVING COUNT(*) > 100
ORDER BY total_revenue DESC;


-- ============================================================================
-- COMPLEX PRODUCTION-LIKE QUERIES
-- ============================================================================

-- PROD.1: Sales analysis by customer segment
SELECT 
    c.c_customer_id,
    CONCAT(c.c_first_name, ' ', c.c_last_name) as customer_name,
    COUNT(DISTINCT ss.ss_item_sk) as num_products_purchased,
    SUM(ss.ss_quantity) as total_quantity,
    SUM(ss.ss_sales_price) as lifetime_value,
    AVG(ss.ss_sales_price) as avg_transaction_value,
    MAX(ss.ss_sales_price) as max_transaction_value
FROM customer c
LEFT JOIN store_sales ss ON c.c_customer_sk = ss.ss_customer_sk
GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
HAVING SUM(ss.ss_sales_price) IS NOT NULL
ORDER BY lifetime_value DESC
LIMIT 100;

-- PROD.2: Store performance summary
SELECT 
    s.s_store_id,
    s.s_store_name,
    s.s_state,
    COUNT(DISTINCT ss.ss_customer_sk) as num_customers,
    COUNT(*) as num_transactions,
    SUM(ss.ss_quantity) as total_items_sold,
    SUM(ss.ss_sales_price) as total_revenue,
    AVG(ss.ss_sales_price) as avg_transaction_size,
    MIN(ss.ss_sales_price) as min_sale,
    MAX(ss.ss_sales_price) as max_sale
FROM store s
LEFT JOIN store_sales ss ON s.s_store_sk = ss.ss_store_sk
GROUP BY s.s_store_id, s.s_store_name, s.s_state
ORDER BY total_revenue DESC;

-- PROD.3: Category performance with date range
SELECT 
    d.d_year,
    d.d_moy,
    i.i_category,
    i.i_class,
    COUNT(*) as num_sales,
    COUNT(DISTINCT ss.ss_customer_sk) as num_customers,
    SUM(ss.ss_quantity) as total_qty,
    SUM(ss.ss_sales_price) as revenue,
    AVG(ss.ss_sales_price) as avg_price
FROM store_sales ss
INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
WHERE d.d_year = 2021
GROUP BY d.d_year, d.d_moy, i.i_category, i.i_class
ORDER BY d.d_moy, revenue DESC;

-- PROD.4: Top customers for each store
SELECT 
    ss.ss_store_sk,
    s.s_store_name,
    c.c_customer_id,
    c.c_first_name,
    SUM(ss.ss_sales_price) as total_spent,
    COUNT(*) as num_purchases,
    RANK() OVER (PARTITION BY ss.ss_store_sk ORDER BY SUM(ss.ss_sales_price) DESC) as rank
FROM store_sales ss
INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
GROUP BY ss.ss_store_sk, s.s_store_name, c.c_customer_id, c.c_first_name
LIMIT 100;

-- PROD.5: Inventory analysis
SELECT 
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    inv.inv_qty_on_hand,
    COUNT(DISTINCT ss.ss_item_sk) as sales_count,
    SUM(ss.ss_quantity) as units_sold
FROM item i
LEFT JOIN inventory inv ON i.i_item_sk = inv.inv_item_sk
LEFT JOIN store_sales ss ON i.i_item_sk = ss.ss_item_sk
GROUP BY i.i_item_id, i.i_item_desc, i.i_category, inv.inv_qty_on_hand
ORDER BY sales_count DESC
LIMIT 50;
