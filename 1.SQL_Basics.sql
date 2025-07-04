-- SQL_Basics.sql
-- This script provides a beginner-level lesson on foundational MySQL queries for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: Foundational MySQL Queries
-- =====================================
-- The following queries demonstrate core MySQL concepts: SELECT, WHERE, ORDER BY, GROUP BY, aggregates, joins, and more.

-- 1. Basic SELECT: Retrieve all columns from a table
-- Purpose: Explore the entire customers table
SELECT * 
FROM customers;

-- 2. SELECT specific columns
-- Purpose: Retrieve only first name, last name, and email for clarity
SELECT first_name, last_name, email
FROM customers;

-- 3. WHERE clause: Filter rows based on conditions
-- Purpose: Find customers from a specific city
SELECT first_name, last_name, city
FROM customers
WHERE city = 'New York';

-- 4. ORDER BY: Sort results
-- Purpose: List customers alphabetically by last name
SELECT first_name, last_name
FROM customers
ORDER BY last_name ASC;

-- 5. GROUP BY: Group rows for aggregation
-- Purpose: Count customers by city
SELECT city, COUNT(*) AS customer_count
FROM customers
GROUP BY city;

-- 6. Aggregate functions: COUNT, SUM, AVG
-- Purpose: Calculate total orders, total spent, and average order value per customer
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_spent,
    AVG(o.order_amount) AS avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 7. Combining WHERE, GROUP BY, and ORDER BY
-- Purpose: Find top 5 cities by total sales, excluding small orders
SELECT 
    c.city,
    SUM(o.order_amount) AS total_sales
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_amount > 100
GROUP BY c.city
ORDER BY total_sales DESC
LIMIT 5;

-- 8. Basic arithmetic in SELECT
-- Purpose: Calculate discount percentage for orders with discounts
SELECT 
    order_id,
    product_id,
    order_amount,
    discount_amount,
    (discount_amount / order_amount * 100) AS discount_percentage
FROM orders
WHERE discount_amount > 0;

-- 9. DISTINCT: Remove duplicate rows
-- Purpose: Get unique product categories
SELECT DISTINCT category
FROM products;

-- 10. LIMIT: Restrict the number of rows returned
-- Purpose: Get top 3 most expensive products
SELECT product_name, price
FROM products
ORDER BY price DESC
LIMIT 3;

-- 11. Combining conditions with AND/OR
-- Purpose: Find customers from specific cities with recent orders
SELECT 
    c.first_name, 
    c.last_name, 
    c.city, 
    o.order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.city IN ('New York', 'Chicago') 
  AND o.order_date >= '2024-01-01';

-- 12. Pattern matching with LIKE
-- Purpose: Find products with names containing 'phone'
SELECT product_name, price
FROM products
WHERE product_name LIKE '%phone%';

-- 13. Handling NULL values
-- Purpose: Find customers with no email address
SELECT first_name, last_name
FROM customers
WHERE email IS NULL;

-- 14. MIN and MAX aggregate functions
-- Purpose: Find the cheapest and most expensive product prices
SELECT 
    MIN(price) AS cheapest_price,
    MAX(price) AS highest_price
FROM products;

-- 15. Basic INNER JOIN
-- Purpose: List orders with customer names
SELECT 
    o.order_id,
    o.order_amount,
    c.first_name,
    c.last_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- Lesson Notes:
-- - Use * cautiously in production; explicitly list columns for clarity and performance.
-- - ASC (ascending) is default for ORDER BY; use DESC for descending.
-- - GROUP BY requires all non-aggregated columns in SELECT to be listed.
-- - LEFT JOIN includes all rows from the left table, even if no match exists.
-- - INNER JOIN only includes rows with matching records in both tables.
-- - Ensure sample data (customers.csv, orders.csv, products.csv) is loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of the concepts above. Each question includes an explanation and the SQL query as the answer.

-- Question 1: How can you retrieve all columns from the customers table?
-- Explanation: The `SELECT *` statement retrieves all columns from a specified table, useful for quick data exploration.
-- Answer:
SELECT * 
FROM customers;

-- Question 2: How do you select only the first name, last name, and email of customers?
-- Explanation: Explicitly listing columns in `SELECT` improves readability and performance compared to `SELECT *`.
-- Answer:
SELECT first_name, last_name, email
FROM customers;

-- Question 3: How can you find customers who live in New York?
-- Explanation: The `WHERE` clause filters rows based on a condition, here selecting customers where the `city` column equals 'New York'.
-- Answer:
SELECT first_name, last_name, city
FROM customers
WHERE city = 'New York';

-- Question 4: How do you list customers sorted alphabetically by their last name?
-- Explanation: The `ORDER BY` clause sorts results, with `ASC` (ascending) sorting last names from A to Z.
-- Answer:
SELECT first_name, last_name
FROM customers
ORDER BY last_name ASC;

-- Question 5: How can you count the number of customers in each city?
-- Explanation: The `GROUP BY` clause groups rows by a column, and `COUNT(*)` counts the rows in each group.
-- Answer:
SELECT city, COUNT(*) AS customer_count
FROM customers
GROUP BY city;

-- Question 6: How do you calculate the total orders, total spent, and average order value per customer?
-- Explanation: Use `COUNT`, `SUM`, and `AVG` with `GROUP BY` to aggregate data. A `LEFT JOIN` includes all customers, even those without orders.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_spent,
    AVG(o.order_amount) AS avg_order_value
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- Question 7: How do you find the top 5 cities by total sales for orders over $100?
-- Explanation: Combine `WHERE` to filter orders, `GROUP BY` to aggregate sales, `ORDER BY` to sort, and `LIMIT` to get the top 5.
-- Answer:
SELECT 
    c.city,
    SUM(o.order_amount) AS total_sales
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_amount > 100
GROUP BY c.city
ORDER BY total_sales DESC
LIMIT 5;

-- Question 8: How can you calculate the discount percentage for each order with a non-zero discount?
-- Explanation: Use arithmetic in `SELECT` to compute `(discount_amount / order_amount * 100)`. The `WHERE` clause filters orders with discounts.
-- Answer:
SELECT 
    order_id,
    product_id,
    order_amount,
    discount_amount,
    (discount_amount / order_amount * 100) AS discount_percentage
FROM orders
WHERE discount_amount > 0;

-- Question 9: How do you get a list of unique product categories?
-- Explanation: The `DISTINCT` keyword removes duplicate values from the result set, useful for listing unique categories.
-- Answer:
SELECT DISTINCT category
FROM products;

-- Question 10: How can you find the three most expensive products?
-- Explanation: Use `ORDER BY` with `DESC` to sort by price in descending order and `LIMIT` to restrict to the top 3 rows.
-- Answer:
SELECT product_name, price
FROM products
ORDER BY price DESC
LIMIT 3;

-- Question 11: How do you find customers from New York or Chicago with orders placed after January 1, 2024?
-- Explanation: Use `IN` for multiple values in `WHERE` and combine conditions with `AND`. A `JOIN` links customers to their orders.
-- Answer:
SELECT 
    c.first_name, 
    c.last_name, 
    c.city, 
    o.order_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.city IN ('New York', 'Chicago') 
  AND o.order_date >= '2024-01-01';

-- Question 12: How can you find products with "phone" in their name?
-- Explanation: The `LIKE` operator with `%` wildcards searches for a substring in a column, useful for pattern matching.
-- Answer:
SELECT product_name, price
FROM products
WHERE product_name LIKE '%phone%';

-- Question 13: How do you identify customers who have not provided an email address?
-- Explanation: Use `IS NULL` in the `WHERE` clause to find rows where the `email` column is null.
-- Answer:
SELECT first_name, last_name
FROM customers
WHERE email IS NULL;

-- Question 14: How can you find the cheapest and most expensive product prices?
-- Explanation: The `MIN` and `MAX` aggregate functions compute the smallest and largest values in a column.
-- Answer:
SELECT 
    MIN(price) AS cheapest_price,
    MAX(price) AS highest_price
FROM products;

-- Question 15: How do you list orders along with the names of the customers who placed them?
-- Explanation: An `INNER JOIN` combines rows from `orders` and `customers` where there’s a match on `customer_id`.
-- Answer:
SELECT 
    o.order_id,
    o.order_amount,
    c.first_name,
    c.last_name
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- =====================================
-- Final Notes
-- =====================================
-- - Ensure the `mysql_learning` database is set up with the sample datasets (customers.csv, orders.csv, products.csv) loaded.
-- - Run these queries in a MySQL client (e.g., MySQL Workbench) to practice and verify results.
-- - Use the questions to test your understanding, and modify conditions (e.g., cities, thresholds) for additional practice.
-- - For further learning, explore the `Data_Filtering.sql` script in the `basics/` folder.
