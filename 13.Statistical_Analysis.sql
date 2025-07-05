-- Statistical_Analysis.sql
-- This script provides a comprehensive lesson on MySQL statistical analysis techniques for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on STDDEV_POP, VARIANCE_POP, and approximations like percentiles and correlations.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.
-- Requires MySQL 8.0+ for window functions used in approximations.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Statistical Analysis
-- =====================================
-- The following queries demonstrate core statistical functions: STDDEV_POP, VARIANCE_POP, AVG, COUNT, 
-- and approximations for percentiles, medians, and correlations for data analysis.

-- 1. Population standard deviation: Calculate STDDEV_POP for order amounts
-- Purpose: Measure the spread of order amounts across all orders
SELECT 
    STDDEV_POP(order_amount) AS population_stddev
FROM orders;

-- 2. Population variance: Calculate VARIANCE_POP for order amounts
-- Purpose: Quantify the variance of order amounts across all orders
SELECT 
    VARIANCE_POP(order_amount) AS population_variance
FROM orders;

-- 3. Grouped statistics: STDDEV_POP and AVG by product category
-- Purpose: Analyze order amount spread and average within each product category
SELECT 
    p.category,
    COUNT(o.order_id) AS order_count,
    AVG(o.order_amount) AS avg_order_amount,
    STDDEV_POP(o.order_amount) AS stddev_order_amount
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category;

-- 4. Outlier detection: Identify orders beyond 2 standard deviations
-- Purpose: Flag orders with amounts outside mean ± 2*STDDEV_POP
SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_amount > (
    SELECT AVG(order_amount) + 2 * STDDEV_POP(order_amount)
    FROM orders
)
OR order_amount < (
    SELECT AVG(order_amount) - 2 * STDDEV_POP(order_amount)
    FROM orders
);

-- 5. Percentile approximation: Estimate 75th percentile of order amounts
-- Purpose: Use window functions to approximate the 75th percentile
WITH ranked_orders AS (
    SELECT 
        order_amount,
        NTILE(100) OVER (ORDER BY order_amount) AS percentile
    FROM orders
)
SELECT 
    MAX(order_amount) AS approx_75th_percentile
FROM ranked_orders
WHERE percentile = 75;

-- 6. Median approximation: Estimate median order amount
-- Purpose: Use NTILE to approximate the median (50th percentile)
WITH ranked_orders AS (
    SELECT 
        order_amount,
        NTILE(100) OVER (ORDER BY order_amount) AS percentile
    FROM orders
)
SELECT 
    AVG(order_amount) AS approx_median
FROM ranked_orders
WHERE percentile IN (50, 51);

-- 7. Coefficient of variation: Calculate relative variability
-- Purpose: Compute STDDEV_POP / AVG to measure relative spread
SELECT 
    STDDEV_POP(order_amount) / AVG(order_amount) * 100 AS coefficient_of_variation
FROM orders;

-- 8. Correlation approximation: Correlate order amount with product price
-- Purpose: Approximate Pearson correlation using aggregated products
SELECT 
    (COUNT(*) * SUM(o.order_amount * p.price) - SUM(o.order_amount) * SUM(p.price)) /
    SQRT((COUNT(*) * SUM(o.order_amount * o.order_amount) - POW(SUM(o.order_amount), 2)) *
         (COUNT(*) * SUM(p.price * p.price) - POW(SUM(p.price), 2))) AS correlation
FROM orders o
JOIN products p ON o.product_id = p.product_id;

-- 9. Z-score calculation: Standardize order amounts
-- Purpose: Compute z-scores to compare order amounts to the mean
SELECT 
    order_id,
    order_amount,
    (order_amount - AVG(order_amount) OVER ()) / STDDEV_POP(order_amount) OVER () AS z_score
FROM orders;

-- 10. Grouped variance: VARIANCE_POP by customer city
-- Purpose: Analyze variability of order amounts by city
SELECT 
    c.city,
    VARIANCE_POP(o.order_amount) AS variance_order_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.city;

-- 11. Rolling standard deviation: Calculate 7-day rolling STDDEV_POP
-- Purpose: Measure variability of order amounts over a 7-day window
SELECT 
    order_id,
    order_date,
    order_amount,
    STDDEV_POP(order_amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_stddev
FROM orders;

-- 12. Cohort variance: Analyze order amount variance by cohort
-- Purpose: Compute VARIANCE_POP for orders within customer cohorts
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(order_date, '%Y-%m')) AS cohort_month
    FROM orders
    GROUP BY customer_id
)
SELECT 
    f.cohort_month,
    VARIANCE_POP(o.order_amount) AS cohort_variance
FROM orders o
JOIN first_orders f ON o.customer_id = f.customer_id
GROUP BY f.cohort_month;

-- 13. Statistical summary: Comprehensive stats for order amounts
-- Purpose: Calculate multiple statistics in a single query
SELECT 
    COUNT(*) AS total_orders,
    AVG(order_amount) AS mean_amount,
    STDDEV_POP(order_amount) AS stddev_amount,
    VARIANCE_POP(order_amount) AS variance_amount,
    MIN(order_amount) AS min_amount,
    MAX(order_amount) AS max_amount
FROM orders;

-- 14. Percentile rank: Calculate percentile rank for orders
-- Purpose: Use CUME_DIST to assign percentile ranks to order amounts
SELECT 
    order_id,
    order_amount,
    CUME_DIST() OVER (ORDER BY order_amount) AS percentile_rank
FROM orders;

-- 15. Correlation by category: Approximate correlation within categories
-- Purpose: Compute correlation between order amount and price per category
SELECT 
    p.category,
    (COUNT(*) * SUM(o.order_amount * p.price) - SUM(o.order_amount) * SUM(p.price)) /
    SQRT((COUNT(*) * SUM(o.order_amount * o.order_amount) - POW(SUM(o.order_amount), 2)) *
         (COUNT(*) * SUM(p.price * p.price) - POW(SUM(p.price), 2))) AS correlation
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category;

-- Lesson Notes:
-- - STDDEV_POP and VARIANCE_POP compute population statistics (all rows); use STDDEV_SAMP and VARIANCE_SAMP for sample statistics.
-- - MySQL lacks native percentile/median functions; use NTILE or CUME_DIST for approximations.
-- - Correlation calculations are approximations due to MySQL’s limited statistical functions.
-- - Window functions (e.g., STDDEV_POP OVER) require MySQL 8.0+.
-- - Indexes on columns like order_amount and order_date improve performance for statistical queries.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv, sales.csv).

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of statistical analysis concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you calculate the population standard deviation of order amounts?
-- Explanation: STDDEV_POP computes the standard deviation across all orders.
-- Answer:
SELECT 
    STDDEV_POP(order_amount) AS population_stddev
FROM orders;

-- Question 2: How can you calculate the population variance of order amounts?
-- Explanation: VARIANCE_POP measures the variance across all orders.
-- Answer:
SELECT 
    VARIANCE_POP(order_amount) AS population_variance
FROM orders;

-- Question 3: How do you compute standard deviation and average by product category?
-- Explanation: GROUP BY with STDDEV_POP and AVG provides category-level statistics.
-- Answer:
SELECT 
    p.category,
    COUNT(o.order_id) AS order_count,
    AVG(o.order_amount) AS avg_order_amount,
    STDDEV_POP(o.order_amount) AS stddev_order_amount
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category;

-- Question 4: How can you identify orders beyond 2 standard deviations from the mean?
-- Explanation: Subqueries calculate mean ± 2*STDDEV_POP to flag outliers.
-- Answer:
SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_amount > (
    SELECT AVG(order_amount) + 2 * STDDEV_POP(order_amount)
    FROM orders
)
OR order_amount < (
    SELECT AVG(order_amount) - 2 * STDDEV_POP(order_amount)
    FROM orders
);

-- Question 5: How do you approximate the 75th percentile of order amounts?
-- Explanation: NTILE(100) divides data into percentiles; select the 75th.
-- Answer:
WITH ranked_orders AS (
    SELECT 
        order_amount,
        NTILE(100) OVER (ORDER BY order_amount) AS percentile
    FROM orders
)
SELECT 
    MAX(order_amount) AS approx_75th_percentile
FROM ranked_orders
WHERE percentile = 75;

-- Question 6: How can you approximate the median order amount?
-- Explanation: NTILE(100) identifies the 50th/51st percentiles for median approximation.
-- Answer:
WITH ranked_orders AS (
    SELECT 
        order_amount,
        NTILE(100) OVER (ORDER BY order_amount) AS percentile
    FROM orders
)
SELECT 
    AVG(order_amount) AS approx_median
FROM ranked_orders
WHERE percentile IN (50, 51);

-- Question 7: How do you calculate the coefficient of variation for order amounts?
-- Explanation: STDDEV_POP / AVG * 100 measures relative variability.
-- Answer:
SELECT 
    STDDEV_POP(order_amount) / AVG(order_amount) * 100 AS coefficient_of_variation
FROM orders;

-- Question 8: How can you approximate the correlation
