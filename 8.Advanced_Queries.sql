-- Advanced_Queries.sql
-- This script provides a comprehensive lesson on advanced MySQL query techniques for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on subqueries, derived tables, and dynamic SQL.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: Advanced MySQL Queries
-- =====================================
-- The following queries demonstrate advanced techniques: subqueries (scalar, row, correlated), derived tables, common table expressions (CTEs), and dynamic SQL for complex data transformations and analysis.

-- 1. Scalar subquery: Find customers with above-average order amounts
-- Purpose: Identify customers whose total order amount exceeds the overall average
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING total_spent > (SELECT AVG(order_amount) FROM orders);

-- 2. Row subquery: Find orders matching the highest-priced product
-- Purpose: Identify orders for products with the maximum price
SELECT 
    order_id,
    product_id,
    order_amount
FROM orders
WHERE product_id = (
    SELECT product_id 
    FROM products 
    WHERE price = (SELECT MAX(price) FROM products)
);

-- 3. Correlated subquery: Find customers with orders in the last 30 days
-- Purpose: List customers who placed orders recently using a correlated subquery
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name
FROM customers c
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.customer_id = c.customer_id 
    AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
);

-- 4. Derived table: Calculate top 5 cities by sales
-- Purpose: Use a derived table to compute and rank city sales
SELECT 
    city,
    total_sales
FROM (
    SELECT 
        c.city,
        SUM(o.order_amount) AS total_sales
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.city
) AS city_sales
ORDER BY total_sales DESC
LIMIT 5;

-- 5. Common Table Expression (CTE): Rank products by sales
-- Purpose: Use a CTE to rank products based on total sales
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(o.order_amount) AS total_sales
    FROM products p
    JOIN orders o ON p.product_id = o.product_id
    GROUP BY p.product_id, p.product_name
)
SELECT 
    product_name,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
FROM product_sales;

-- 6. Nested subquery: Find customers with no orders
-- Purpose: Identify customers who have never placed an order
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id 
    FROM orders
);

-- 7. Dynamic SQL: Filter orders based on a dynamic condition
-- Purpose: Use PREPARE/EXECUTE to filter orders dynamically
SET @condition = 'order_amount > 500';
SET @query = CONCAT('SELECT order_id, customer_id, order_amount FROM orders WHERE ', @condition);
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 8. Correlated subquery with aggregation: Find latest order per customer
-- Purpose: Retrieve the most recent order for each customer
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_amount
FROM orders o
WHERE o.order_date = (
    SELECT MAX(order_date)
    FROM orders o2
    WHERE o2.customer_id = o.customer_id
);

-- 9. Derived table with JOIN: Calculate average order amount per category
-- Purpose: Use a derived table to compute category-level averages
SELECT 
    p.category,
    avg_orders.avg_amount
FROM products p
JOIN (
    SELECT 
        product_id,
        AVG(order_amount) AS avg_amount
    FROM orders
    GROUP BY product_id
) AS avg_orders ON p.product_id = avg_orders.product_id;

-- 10. CTE with multiple levels: Analyze customer retention
-- Purpose: Use a CTE to calculate the number of repeat customers
WITH order_counts AS (
    SELECT 
        customer_id,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
)
SELECT 
    COUNT(*) AS repeat_customers
FROM order_counts
WHERE order_count > 1;

-- 11. Subquery in SELECT: Calculate percentage of total sales
-- Purpose: Compute each order’s contribution to total sales
SELECT 
    order_id,
    order_amount,
    (order_amount / (SELECT SUM(order_amount) FROM orders) * 100) AS percent_of_total
FROM orders;

-- 12. Dynamic SQL with parameters: Create a flexible sales report
-- Purpose: Generate a sales report with dynamic grouping
SET @group_by = 'city';
SET @query = CONCAT('
    SELECT 
        c.', @group_by, ',
        SUM(o.order_amount) AS total_sales
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.', @group_by
);
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 13. Correlated subquery with EXISTS: Find products with high sales
-- Purpose: List products with at least one order above a threshold
SELECT 
    product_id,
    product_name
FROM products p
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.product_id = p.product_id 
    AND o.order_amount > 1000
);

-- 14. CTE with window function: Identify top customers per city
-- Purpose: Use a CTE with a window function to rank customers by spending within each city
WITH customer_spending AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        SUM(o.order_amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city
)
SELECT 
    customer_id,
    first_name,
    last_name,
    city,
    total_spent,
    RANK() OVER (PARTITION BY city ORDER BY total_spent DESC) AS spending_rank
FROM customer_spending
WHERE spending_rank = 1;

-- 15. Subquery with ALL: Find orders larger than all orders in a specific city
-- Purpose: Identify orders exceeding the maximum order amount in a given city
SELECT 
    order_id,
    customer_id,
    order_amount
FROM orders o
WHERE order_amount > ALL (
    SELECT o2.order_amount
    FROM orders o2
    JOIN customers c ON o2.customer_id = c.customer_id
    WHERE c.city = 'New York'
);

-- Lesson Notes:
-- - Subqueries can be scalar (single value), row (single row), or correlated (depend on outer query).
-- - Derived tables are subqueries in the FROM clause, acting as virtual tables.
-- - CTEs improve readability for complex queries and support recursive queries (not covered here).
-- - Dynamic SQL with PREPARE/EXECUTE allows flexible query construction but requires careful validation to prevent SQL injection.
-- - Ensure indexes on frequently filtered/joined columns (e.g., customer_id, order_date) for performance.
-- - Sample datasets (customers.csv, orders.csv, products.csv) must be loaded.
-- - Test dynamic SQL in a secure environment to avoid risks.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of advanced query techniques. Each question includes an explanation and the SQL query as the answer.

-- Question 1: How do you find customers whose total spending is above the average order amount?
-- Explanation: A scalar subquery in HAVING compares each customer’s total spending to the overall average.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(o.order_amount) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING total_spent > (SELECT AVG(order_amount) FROM orders);

-- Question 2: How can you find orders for the highest-priced product?
-- Explanation: A nested subquery identifies the product with the maximum price, used to filter orders.
-- Answer:
SELECT 
    order_id,
    product_id,
    order_amount
FROM orders
WHERE product_id = (
    SELECT product_id 
    FROM products 
    WHERE price = (SELECT MAX(price) FROM products)
);

-- Question 3: How do you list customers who placed orders in the last 30 days?
-- Explanation: A correlated subquery with EXISTS checks for recent orders per customer.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name
FROM customers c
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.customer_id = c.customer_id 
    AND o.order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
);

-- Question 4: How can you find the top 5 cities by total sales using a derived table?
-- Explanation: A derived table aggregates sales by city, and the outer query sorts and limits results.
-- Answer:
SELECT 
    city,
    total_sales
FROM (
    SELECT 
        c.city,
        SUM(o.order_amount) AS total_sales
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.city
) AS city_sales
ORDER BY total_sales DESC
LIMIT 5;

-- Question 5: How do you rank products by total sales using a CTE?
-- Explanation: A CTE computes sales per product, and a window function ranks them.
-- Answer:
WITH product_sales AS (
    SELECT 
        p.product_id,
        p.product_name,
        SUM(o.order_amount) AS total_sales
    FROM products p
    JOIN orders o ON p.product_id = o.product_id
    GROUP BY p.product_id, p.product_name
)
SELECT 
    product_name,
    total_sales,
    RANK() OVER (ORDER BY total_sales DESC) AS sales_rank
FROM product_sales;

-- Question 6: How can you find customers who have never placed an order?
-- Explanation: NOT IN with a subquery identifies customers absent from the orders table.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers
WHERE customer_id NOT IN (
    SELECT customer_id 
    FROM orders
);

-- Question 7: How do you filter orders dynamically based on a condition?
-- Explanation: Dynamic SQL uses CONCAT and PREPARE to build and execute a flexible query.
-- Answer:
SET @condition = 'order_amount > 500';
SET @query = CONCAT('SELECT order_id, customer_id, order_amount FROM orders WHERE ', @condition);
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Question 8: How can you find the most recent order for each customer?
-- Explanation: A correlated subquery matches each order to the maximum order date per customer.
-- Answer:
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_amount
FROM orders o
WHERE o.order_date = (
    SELECT MAX(order_date)
    FROM orders o2
    WHERE o2.customer_id = o.customer_id
);

-- Question 9: How do you calculate the average order amount per product category?
-- Explanation: A derived table computes averages per product, joined with products for category info.
-- Answer:
SELECT 
    p.category,
    avg_orders.avg_amount
FROM products p
JOIN (
    SELECT 
        product_id,
        AVG(order_amount) AS avg_amount
    FROM orders
    GROUP BY product_id
) AS avg_orders ON p.product_id = avg_orders.product_id;

-- Question 10: How can you count repeat customers using a CTE?
-- Explanation: A CTE counts orders per customer, and the outer query filters for those with multiple orders.
-- Answer:
WITH order_counts AS (
    SELECT 
        customer_id,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
)
SELECT 
    COUNT(*) AS repeat_customers
FROM order_counts
WHERE order_count > 1;

-- Question 11: How do you calculate each order’s percentage of total sales?
-- Explanation: A subquery in SELECT computes the total sales for percentage calculations.
-- Answer:
SELECT 
    order_id,
    order_amount,
    (order_amount / (SELECT SUM(order_amount) FROM orders) * 100) AS percent_of_total
FROM orders;

-- Question 12: How can you create a dynamic sales report grouped by a specified column?
-- Explanation: Dynamic SQL constructs a query with a variable GROUP BY clause.
-- Answer:
SET @group_by = 'city';
SET @query = CONCAT('
    SELECT 
        c.', @group_by, ',
        SUM(o.order_amount) AS total_sales
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.', @group_by
);
PREPARE stmt FROM @query;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Question 13: How do you find products with at least one high-value order?
-- Explanation: A correlated subquery with EXISTS checks for orders above a threshold.
-- Answer:
SELECT 
    product_id,
    product_name
FROM products p
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.product_id = p.product_id 
    AND o.order_amount > 1000
);

-- Question 14: How can you identify the top-spending customer in each city using a CTE?
-- Explanation: A CTE computes spending, and a window function ranks customers within cities.
-- Answer:
WITH customer_spending AS (
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        c.city,
        SUM(o.order_amount) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name, c.city
)
SELECT 
    customer_id,
    first_name,
    last_name,
    city,
    total_spent,
    RANK() OVER (PARTITION BY city ORDER BY total_spent DESC) AS spending_rank
FROM customer_spending
WHERE spending_rank = 1;

-- Question 15: How do you find orders larger than all orders in New York?
-- Explanation: A subquery with ALL compares order amounts to the maximum in a specific city.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount
FROM orders o
WHERE order_amount > ALL (
    SELECT o2.order_amount
    FROM orders o2
    JOIN customers c ON o2.customer_id = c.customer_id
    WHERE c.city = 'New York'
);

-- =====================================
-- Final Notes
-- =====================================
-- - Subqueries and CTEs improve query modularity but can impact performance; optimize with indexes.
-- - Dynamic SQL requires careful input validation to prevent security risks like SQL injection.
-- - Use EXPLAIN to analyze query performance, especially for correlated subqueries.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
-- - For further learning, explore `Triggers.sql` or `Index_Optimization.sql` in the `advanced/` folder.
