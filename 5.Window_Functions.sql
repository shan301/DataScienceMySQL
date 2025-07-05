-- Window_Functions.sql
-- This script provides a comprehensive lesson on MySQL window functions for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Window Functions
-- =====================================
-- The following queries demonstrate core MySQL window function concepts: ROW_NUMBER, RANK, DENSE_RANK, NTILE, 
-- PARTITION BY, running totals, moving averages, and more. Window functions are supported in MySQL 8.0+.

-- 1. ROW_NUMBER: Assign a unique sequential number to each row
-- Purpose: Number each order within the orders table
SELECT 
    order_id,
    customer_id,
    order_amount,
    ROW_NUMBER() OVER (ORDER BY order_date) AS row_num
FROM orders;

-- 2. RANK: Assign ranks with gaps for ties
-- Purpose: Rank products by price, with ties receiving the same rank
SELECT 
    product_name,
    price,
    RANK() OVER (ORDER BY price DESC) AS price_rank
FROM products;

-- 3. DENSE_RANK: Assign ranks without gaps for ties
-- Purpose: Rank customers by total orders, without gaps in ranking
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    DENSE_RANK() OVER (ORDER BY COUNT(o.order_id) DESC) AS order_rank
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 4. NTILE: Divide rows into specified buckets
-- Purpose: Divide customers into 4 quartiles based on total spent
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent,
    NTILE(4) OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_quartile
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 5. PARTITION BY: Group rows for window calculations
-- Purpose: Rank orders within each customer
SELECT 
    o.order_id,
    o.customer_id,
    o.order_amount,
    RANK() OVER (PARTITION BY o.customer_id ORDER BY o.order_amount DESC) AS rank_within_customer
FROM orders o;

-- 6. Running total with SUM
-- Purpose: Calculate cumulative order amount over time
SELECT 
    order_id,
    order_date,
    order_amount,
    SUM(order_amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS running_total
FROM orders;

-- 7. Moving average with AVG
-- Purpose: Calculate 3-day moving average of order amounts
SELECT 
    order_id,
    order_date,
    order_amount,
    AVG(order_amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg
FROM orders;

-- 8. FIRST_VALUE: Get the first value in a window
-- Purpose: Show the most expensive product per category
SELECT 
    product_name,
    category,
    price,
    FIRST_VALUE(product_name) OVER (
        PARTITION BY category
        ORDER BY price DESC
    ) AS most_expensive_in_category
FROM products;

-- 9. LAST_VALUE: Get the last value in a window
-- Purpose: Identify the least expensive product per category
SELECT 
    product_name,
    category,
    price,
    LAST_VALUE(product_name) OVER (
        PARTITION BY category
        ORDER BY price ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS least_expensive_in_category
FROM products;

-- 10. LAG: Access previous row's value
-- Purpose: Compare each order's amount with the previous order for the same customer
SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    LAG(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS previous_order_amount
FROM orders;

-- 11. LEAD: Access next row's value
-- Purpose: Show the next order date for each customer
SELECT 
    order_id,
    customer_id,
    order_date,
    LEAD(order_date) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS next_order_date
FROM orders;

-- 12. Cumulative count with COUNT
-- Purpose: Count orders cumulatively by customer over time
SELECT 
    order_id,
    customer_id,
    order_date,
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_order_count
FROM orders;

-- 13. Window with PARTITION BY and RANGE
-- Purpose: Calculate total sales per city within a date range
SELECT 
    o.order_id,
    c.city,
    o.order_date,
    o.order_amount,
    SUM(o.order_amount) OVER (
        PARTITION BY c.city
        ORDER BY o.order_date
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS city_sales_last_30_days
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- 14. CUME_DIST: Calculate cumulative distribution
-- Purpose: Determine the percentile rank of product prices
SELECT 
    product_name,
    price,
    CUME_DIST() OVER (ORDER BY price) AS price_percentile
FROM products;

-- 15. PERCENT_RANK: Calculate relative rank
-- Purpose: Rank customers by total spent relative to others
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent,
    PERCENT_RANK() OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_rank
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Lesson Notes:
-- - Window functions operate over a "window" of rows defined by OVER clause.
-- - PARTITION BY divides rows into groups, similar to GROUP BY but without collapsing rows.
-- - ORDER BY in OVER clause determines the order of rows within the window.
-- - ROWS or RANGE specifies the window frame (e.g., UNBOUNDED PRECEDING, CURRENT ROW).
-- - Use window functions for analytics tasks like rankings, running totals, and comparisons without subqueries.
-- - Ensure MySQL 8.0+ is used, as window functions are not supported in earlier versions.
-- - Sample data (customers.csv, orders.csv, products.csv) must be loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of window function concepts. Each question includes an explanation and the SQL query as the answer.

-- Question 1: How can you assign a unique sequential number to each order based on order date?
-- Explanation: ROW_NUMBER() assigns a unique number to each row in the order specified by the OVER clause.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    ROW_NUMBER() OVER (ORDER BY order_date) AS row_num
FROM orders;

-- Question 2: How do you rank products by price, allowing ties to share the same rank with gaps?
-- Explanation: RANK() assigns the same rank to tied values but leaves gaps in the sequence for subsequent ranks.
-- Answer:
SELECT 
    product_name,
    price,
    RANK() OVER (ORDER BY price DESC) AS price_rank
FROM products;

-- Question 3: How do you rank customers by total orders without gaps in the ranking sequence?
-- Explanation: DENSE_RANK() assigns ranks without gaps, even for ties, unlike RANK().
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    DENSE_RANK() OVER (ORDER BY COUNT(o.order_id) DESC) AS order_rank
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Question 4: How can you divide customers into 4 groups based on their total spending?
-- Explanation: NTILE(4) divides rows into 4 equal buckets based on the ORDER BY clause.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent,
    NTILE(4) OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_quartile
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Question 5: How do you rank orders within each customer based on order amount?
-- Explanation: PARTITION BY divides the window by customer_id, and RANK() ranks orders within each partition.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount,
    RANK() OVER (PARTITION BY customer_id ORDER BY order_amount DESC) AS rank_within_customer
FROM orders;

-- Question 6: How can you calculate a running total of order amounts over time?
-- Explanation: SUM() with ROWS UNBOUNDED PRECEDING accumulates values from the start of the window to the current row.
-- Answer:
SELECT 
    order_id,
    order_date,
    order_amount,
    SUM(order_amount) OVER (ORDER BY order_date ROWS UNBOUNDED PRECEDING) AS running_total
FROM orders;

-- Question 7: How do you compute a 3-day moving average of order amounts?
-- Explanation: AVG() with ROWS BETWEEN 2 PRECEDING AND CURRENT ROW defines a window of the current and previous 2 rows.
-- Answer:
SELECT 
    order_id,
    order_date,
    order_amount,
    AVG(order_amount) OVER (
        ORDER BY order_date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg
FROM orders;

-- Question 8: How can you identify the most expensive product in each category?
-- Explanation: FIRST_VALUE() returns the first value in the window, partitioned by category and ordered by price.
-- Answer:
SELECT 
    product_name,
    category,
    price,
    FIRST_VALUE(product_name) OVER (
        PARTITION BY category
        ORDER BY price DESC
    ) AS most_expensive_in_category
FROM products;

-- Question 9: How do you find the least expensive product in each category?
-- Explanation: LAST_VALUE() with ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ensures the last value in the ordered window.
-- Answer:
SELECT 
    product_name,
    category,
    price,
    LAST_VALUE(product_name) OVER (
        PARTITION BY category
        ORDER BY price ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS least_expensive_in_category
FROM products;

-- Question 10: How can you compare each order’s amount with the previous order for the same customer?
-- Explanation: LAG() retrieves the previous row’s value within the partition, ordered by order_date.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    order_amount,
    LAG(order_amount) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS previous_order_amount
FROM orders;

-- Question 11: How do you show the next order date for each customer’s orders?
-- Explanation: LEAD() retrieves the next row’s value within the partition, ordered by order_date.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    LEAD(order_date) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
    ) AS next_order_date
FROM orders;

-- Question 12: How can you count orders cumulatively for each customer over time?
-- Explanation: COUNT(*) with ROWS UNBOUNDED PRECEDING counts all rows from the start of the partition to the current row.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_date,
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY order_date
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_order_count
FROM orders;

-- Question 13: How do you calculate total sales per city for the last 30 days for each order?
-- Explanation: SUM() with RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW defines a time-based window.
-- Answer:
SELECT 
    o.order_id,
    c.city,
    o.order_date,
    o.order_amount,
    SUM(o.order_amount) OVER (
        PARTITION BY c.city
        ORDER BY o.order_date
        RANGE BETWEEN INTERVAL 30 DAY PRECEDING AND CURRENT ROW
    ) AS city_sales_last_30_days
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id;

-- Question 14: How can you determine the percentile rank of product prices?
-- Explanation: CUME_DIST() calculates the cumulative distribution of rows, giving a value between 0 and 1.
-- Answer:
SELECT 
    product_name,
    price,
    CUME_DIST() OVER (ORDER BY price) AS price_percentile
FROM products;

-- Question 15: How do you rank customers by total spent relative to others?
-- Explanation: PERCENT_RANK() computes the relative rank of each row, returning a value between 0 and 1.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent,
    PERCENT_RANK() OVER (ORDER BY SUM(o.order_amount) DESC) AS spending_rank
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- =====================================
-- Final Notes
-- =====================================
-- - Window functions are powerful for analytical queries, reducing the need for complex subqueries.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
-- - Run these queries in a MySQL client (e.g., MySQL Workbench) to practice and verify results.
-- - Modify window specifications (e.g., PARTITION BY, ORDER BY, ROWS/RANGE) for additional practice.
-- - For further learning, explore the `Advanced_Queries.sql` script in the `advanced/` folder.
