-- Time_Intelligence.sql
-- This script provides a comprehensive lesson on MySQL time intelligence techniques for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on date functions, rolling averages, and cohort analysis.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, time_series) are loaded.
-- Requires MySQL 8.0+ for window functions used in rolling averages and cohort analysis.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Time Intelligence
-- =====================================
-- The following queries demonstrate core time intelligence concepts: date functions, rolling averages, 
-- cohort analysis, and time-based aggregations for analyzing temporal data.

-- 1. Date extraction: Extract year, month, and day from order_date
-- Purpose: Break down order dates into components for analysis
SELECT 
    order_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    DAY(order_date) AS order_day
FROM orders;

-- 2. Date arithmetic: Calculate days since last order
-- Purpose: Compute the time difference between consecutive orders for each customer
SELECT 
    order_id,
    customer_id,
    order_date,
    DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_since_last_order
FROM orders;

-- 3. Date formatting: Format order_date as 'YYYY-MM-DD'
-- Purpose: Standardize date display for reporting
SELECT 
    order_id,
    DATE_FORMAT(order_date, '%Y-%m-%d') AS formatted_date,
    order_amount
FROM orders;

-- 4. Rolling average: Calculate 7-day moving average of order amounts
-- Purpose: Smooth order amounts over a 7-day window
SELECT 
    order_id,
    order_date,
    order_amount,
    AVG(order_amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS seven_day_moving_avg
FROM orders;

-- 5. Cohort analysis: Identify first order month for each customer
-- Purpose: Assign customers to cohorts based on their first order
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT 
    customer_id,
    DATE_FORMAT(first_order_date, '%Y-%m') AS cohort_month
FROM first_orders;

-- 6. Cohort retention: Calculate retention rate by cohort
-- Purpose: Measure how many customers return in subsequent months
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(order_date, '%Y-%m')) AS cohort_month
    FROM orders
    GROUP BY customer_id
),
order_months AS (
    SELECT 
        o.customer_id,
        f.cohort_month,
        DATE_FORMAT(o.order_date, '%Y-%m') AS order_month
    FROM orders o
    JOIN first_orders f ON o.customer_id = f.customer_id
)
SELECT 
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS active_customers
FROM order_months
GROUP BY cohort_month, order_month;

-- 7. Date truncation: Aggregate sales by month
-- Purpose: Summarize sales using DATE_TRUNC for monthly aggregation
SELECT 
    DATE_FORMAT(order_date, '%Y-%m-01') AS month_start,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m-01');

-- 8. Time interval: Calculate orders within the last 30 days
-- Purpose: Filter orders based on a recent time window
SELECT 
    order_id,
    order_date,
    order_amount
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

-- 9. Rolling sum: Calculate cumulative sales by customer
-- Purpose: Compute running total of order amounts per customer
SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    SUM(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_sales
FROM orders;

-- 10. Year-over-year growth: Compare sales by year
-- Purpose: Calculate annual sales and growth percentage
WITH yearly_sales AS (
    SELECT 
        YEAR(order_date) AS order_year,
        SUM(order_amount) AS total_sales
    FROM orders
    GROUP BY YEAR(order_date)
)
SELECT 
    order_year,
    total_sales,
    (total_sales - LAG(total_sales) OVER (ORDER BY order_year)) / LAG(total_sales) OVER (ORDER BY order_year) * 100 AS yoy_growth
FROM yearly_sales;

-- 11. Day of week analysis: Summarize sales by day of week
-- Purpose: Analyze sales patterns by day of the week
SELECT 
    DAYNAME(order_date) AS day_of_week,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY DAYNAME(order_date)
ORDER BY FIELD(DAYNAME(order_date), 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

-- 12. First order lag: Time to first purchase after registration
-- Purpose: Calculate days between customer registration and first order
SELECT 
    c.customer_id,
    c.registration_date,
    MIN(o.order_date) AS first_order_date,
    DATEDIFF(MIN(o.order_date), c.registration_date) AS days_to_first_order
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.registration_date;

-- 13. Rolling count: Count orders in a 30-day window
-- Purpose: Track order volume within a sliding 30-day window
SELECT 
    order_id,
    order_date,
    COUNT(*) OVER (
        ORDER BY order_date
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS orders_last_30_days
FROM orders;

-- 14. Cohort revenue: Calculate revenue by cohort over time
-- Purpose: Analyze total revenue by cohort month
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(order_date, '%Y-%m')) AS cohort_month
    FROM orders
    GROUP BY customer_id
)
SELECT 
    f.cohort_month,
    DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
    SUM(o.order_amount) AS total_revenue
FROM orders o
JOIN first_orders f ON o.customer_id = f.customer_id
GROUP BY f.cohort_month, DATE_FORMAT(o.order_date, '%Y-%m');

-- 15. Seasonality analysis: Identify seasonal sales patterns
-- Purpose: Aggregate sales by quarter to detect seasonal trends
SELECT 
    CONCAT(YEAR(order_date), '-Q', QUARTER(order_date)) AS quarter,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY CONCAT(YEAR(order_date), '-Q', QUARTER(order_date))
ORDER BY YEAR(order_date), QUARTER(order_date);

-- Lesson Notes:
-- - Date functions (YEAR, MONTH, DATE_FORMAT, DATEDIFF) simplify temporal analysis.
-- - Window functions (e.g., AVG, SUM, LAG) are critical for rolling calculations and require MySQL 8.0+.
-- - Cohort analysis tracks customer behavior over time, useful for retention and revenue studies.
-- - Use indexes on date columns (e.g., order_date) to optimize time-based queries.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, time_series.csv).
-- - Test queries with small date ranges to verify accuracy before scaling.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of time intelligence concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you extract year, month, and day from order_date?
-- Explanation: YEAR, MONTH, and DAY functions break down a date into components.
-- Answer:
SELECT 
    order_id,
    order_date,
    YEAR(order_date) AS order_year,
    MONTH(order_date) AS order_month,
    DAY(order_date) AS order_day
FROM orders;

-- Question 2: How can you calculate days between consecutive orders for each customer?
-- Explanation: LAG accesses the previous order_date, and DATEDIFF computes the difference.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    DATEDIFF(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)) AS days_since_last_order
FROM orders;

-- Question 3: How do you format order_date as 'YYYY-MM-DD'?
-- Explanation: DATE_FORMAT standardizes date output for consistent reporting.
-- Answer:
SELECT 
    order_id,
    DATE_FORMAT(order_date, '%Y-%m-%d') AS formatted_date,
    order_amount
FROM orders;

-- Question 4: How can you calculate a 7-day moving average of order amounts?
-- Explanation: AVG with a ROWS window computes the average over the current and prior 6 rows.
-- Answer:
SELECT 
    order_id,
    order_date,
    order_amount,
    AVG(order_amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS seven_day_moving_avg
FROM orders;

-- Question 5: How do you identify the first order month for each customer?
-- Explanation: A CTE with MIN assigns each customer to their first order month.
-- Answer:
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date
    FROM orders
    GROUP BY customer_id
)
SELECT 
    customer_id,
    DATE_FORMAT(first_order_date, '%Y-%m') AS cohort_month
FROM first_orders;

-- Question 6: How can you calculate customer retention by cohort month?
-- Explanation: A CTE identifies cohort months, and the query counts active customers per month.
-- Answer:
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(order_date, '%Y-%m')) AS cohort_month
    FROM orders
    GROUP BY customer_id
),
order_months AS (
    SELECT 
        o.customer_id,
        f.cohort_month,
        DATE_FORMAT(o.order_date, '%Y-%m') AS order_month
    FROM orders o
    JOIN first_orders f ON o.customer_id = f.customer_id
)
SELECT 
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS active_customers
FROM order_months
GROUP BY cohort_month, order_month;

-- Question 7: How do you aggregate sales by month?
-- Explanation: DATE_FORMAT with '%Y-%m-01' groups sales by the first day of each month.
-- Answer:
SELECT 
    DATE_FORMAT(order_date, '%Y-%m-01') AS month_start,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m-01');

-- Question 8: How can you filter orders from the last 30 days?
-- Explanation: DATE_SUB subtracts 30 days from the current date for filtering.
-- Answer:
SELECT 
    order_id,
    order_date,
    order_amount
FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

-- Question 9: How do you calculate cumulative sales per customer?
-- Explanation: SUM with UNBOUNDED PRECEDING accumulates sales per customer over time.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    SUM(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_sales
FROM orders;

-- Question 10: How can you calculate year-over-year sales growth?
-- Explanation: LAG accesses prior year’s sales to compute growth percentage.
-- Answer:
WITH yearly_sales AS (
    SELECT 
        YEAR(order_date) AS order_year,
        SUM(order_amount) AS total_sales
    FROM orders
    GROUP BY YEAR(order_date)
)
SELECT 
    order_year,
    total_sales,
    (total_sales - LAG(total_sales) OVER (ORDER BY order_year)) / LAG(total_sales) OVER (ORDER BY order_year) * 100 AS yoy_growth
FROM yearly_sales;

-- Question 11: How do you summarize sales by day of the week?
-- Explanation: DAYNAME groups sales by weekday, ordered for consistency.
-- Answer:
SELECT 
    DAYNAME(order_date) AS day_of_week,
    COUNT(*) AS order_count,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY DAYNAME(order_date)
ORDER BY FIELD(DAYNAME(order_date), 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');

-- Question 12: How can you calculate the time to first purchase after registration?
-- Explanation: DATEDIFF computes days between registration and first order.
-- Answer:
SELECT 
    c.customer_id,
    c.registration_date,
    MIN(o.order_date) AS first_order_date,
    DATEDIFF(MIN(o.order_date), c.registration_date) AS days_to_first_order
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.registration_date;

-- Question 13: How do you count orders in a 30-day sliding window?
-- Explanation: COUNT with a RANGE window counts orders within 30 days.
-- Answer:
SELECT 
    order_id,
    order_date,
    COUNT(*) OVER (
        ORDER BY order_date
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS orders_last_30_days
FROM orders;

-- Question 14: How can you calculate revenue by cohort over time?
-- Explanation: A CTE identifies cohort months, and the query aggregates revenue by cohort and order month.
-- Answer:
WITH first_orders AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(order_date, '%Y-%m')) AS cohort_month
    FROM orders
    GROUP BY customer_id
)
SELECT 
    f.cohort_month,
    DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
    SUM(o.order_amount) AS total_revenue
FROM orders o
JOIN first_orders f ON o.customer_id = f.customer_id
GROUP BY f.cohort_month, DATE_FORMAT(o.order_date, '%Y-%m');

-- Question 15: How do you identify seasonal sales patterns by quarter?
-- Explanation: CONCAT and QUARTER group sales by year and quarter for trend analysis.
-- Answer:
SELECT 
    CONCAT(YEAR(order_date), '-Q', QUARTER(order_date)) AS quarter,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY CONCAT(YEAR(order_date), '-Q', QUARTER(order_date))
ORDER BY YEAR(order_date), QUARTER(order_date);

-- =====================================
-- Final Notes
-- =====================================
-- - Time intelligence queries benefit from indexes on date columns (e.g., order_date, registration_date).
-- - Window functions for rolling calculations require MySQL 8.0+; verify database version.
-- - Cohort analysis is key for understanding customer behavior and retention.
-- - Always validate date ranges and formats to avoid errors in calculations.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, time_series.csv).
-- - For further learning, explore `Window_Functions.sql` for advanced window techniques or `Partitioning.sql` for optimizing large time-based datasets.
