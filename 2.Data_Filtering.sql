-- Data_Filtering.sql
-- This script teaches MySQL filtering techniques for data exploration, essential for data analysts and scientists.
-- It covers WHERE, LIKE, IN, BETWEEN, IS NULL, and REGEXP, with examples and practice questions.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Filtering Techniques
-- =====================================
-- The following queries demonstrate how to filter data using various MySQL operators and functions.

-- 1. Basic WHERE: Filter rows with a single condition
-- Purpose: Find customers from a specific state
SELECT first_name, last_name, state
FROM customers
WHERE state = 'CA';

-- 2. LIKE: Pattern matching for text
-- Purpose: Find products with names containing 'laptop'
SELECT product_name, price
FROM products
WHERE product_name LIKE '%laptop%';

-- 3. IN: Filter rows matching a list of values
-- Purpose: Get customers from multiple cities
SELECT first_name, last_name, city
FROM customers
WHERE city IN ('New York', 'Chicago', 'Los Angeles');

-- 4. BETWEEN: Filter rows within a range
-- Purpose: Find orders with amounts between $50 and $500
SELECT order_id, order_amount, order_date
FROM orders
WHERE order_amount BETWEEN 50 AND 500;

-- 5. IS NULL: Find rows with missing values
-- Purpose: Identify customers without a phone number
SELECT first_name, last_name
FROM customers
WHERE phone IS NULL;

-- 6. REGEXP: Filter using regular expressions
-- Purpose: Find customers with email addresses ending in '.com'
SELECT first_name, last_name, email
FROM customers
WHERE email REGEXP '\.com$';

-- 7. Combining conditions with AND
-- Purpose: Find recent orders from a specific city
SELECT order_id, order_amount, order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.city = 'Chicago' AND o.order_date >= '2024-01-01';

-- 8. Combining conditions with OR
-- Purpose: Find products in specific categories
SELECT product_name, category, price
FROM products
WHERE category = 'Electronics' OR category = 'Accessories';

-- 9. NOT operator: Exclude specific values
-- Purpose: Find orders not placed in 2023
SELECT order_id, order_amount, order_date
FROM orders
WHERE YEAR(order_date) NOT IN (2023);

-- 10. Complex filtering with multiple conditions
-- Purpose: Find high-value orders from specific states placed after a date
SELECT 
    o.order_id, 
    o.order_amount, 
    c.state, 
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_amount > 1000 
  AND c.state IN ('CA', 'TX') 
  AND o.order_date >= '2024-06-01';

-- Lesson Notes:
-- - WHERE filters rows before aggregation or sorting.
-- - LIKE uses % for zero or more characters and _ for a single character.
-- - IN is useful for small lists; for large lists, consider subqueries or joins.
-- - BETWEEN is inclusive of the start and end values.
-- - REGEXP supports advanced pattern matching but can be slower than LIKE.
-- - Ensure sample data (customers.csv, orders.csv, products.csv) is loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of filtering techniques. Each includes an explanation and the SQL query as the answer.

-- Question 1: How can you find customers from California?
-- Explanation: Use `WHERE` to filter rows where the `state` column equals 'CA'.
-- Answer:
SELECT first_name, last_name, state
FROM customers
WHERE state = 'CA';

-- Question 2: How do you find products with 'laptop' in their name?
-- Explanation: The `LIKE` operator with `%` wildcards searches for a substring in the `product_name` column.
-- Answer:
SELECT product_name, price
FROM products
WHERE product_name LIKE '%laptop%';

-- Question 3: How can you get customers from New York, Chicago, or Los Angeles?
-- Explanation: The `IN` operator filters rows where `city` matches any value in the specified list.
-- Answer:
SELECT first_name, last_name, city
FROM customers
WHERE city IN ('New York', 'Chicago', 'Los Angeles');

-- Question 4: How do you find orders with amounts between $50 and $500?
-- Explanation: The `BETWEEN` operator selects rows where `order_amount` is within the inclusive range of 50 to 500.
-- Answer:
SELECT order_id, order_amount, order_date
FROM orders
WHERE order_amount BETWEEN 50 AND 500;

-- Question 5: How can you identify customers with no phone number?
-- Explanation: Use `IS NULL` to find rows where the `phone` column has no value.
-- Answer:
SELECT first_name, last_name
FROM customers
WHERE phone IS NULL;

-- Question 6: How do you find customers with email addresses ending in '.com'?
-- Explanation: The `REGEXP` operator matches patterns, here checking for emails ending with '.com'.
-- Answer:
SELECT first_name, last_name, email
FROM customers
WHERE email REGEXP '\.com$';

-- Question 7: How can you find orders from Chicago placed on or after January 1, 2024?
-- Explanation: Combine `WHERE` with `AND` and a `JOIN` to filter orders by city and date.
-- Answer:
SELECT order_id, order_amount, order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE c.city = 'Chicago' AND o.order_date >= '2024-01-01';

-- Question 8: How do you find products in the Electronics or Accessories categories?
-- Explanation: Use `OR` to filter rows where `category` matches either of two values.
-- Answer:
SELECT product_name, category, price
FROM products
WHERE category = 'Electronics' OR category = 'Accessories';

-- Question 9: How can you find orders not placed in 2023?
-- Explanation: The `NOT IN` operator excludes rows where the year of `order_date` is 2023.
-- Answer:
SELECT order_id, order_amount, order_date
FROM orders
WHERE YEAR(order_date) NOT IN (2023);

-- Question 10: How do you find high-value orders (> $1000) from California or Texas after June 1, 2024?
-- Explanation: Combine multiple conditions with `AND`, `IN`, and a `JOIN` for complex filtering.
-- Answer:
SELECT 
    o.order_id, 
    o.order_amount, 
    c.state, 
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_amount > 1000 
  AND c.state IN ('CA', 'TX') 
  AND o.order_date >= '2024-06-01';

-- Question 11: How can you find customers whose last name starts with 'S'?
-- Explanation: Use `LIKE` with a single `%` wildcard to match last names starting with 'S'.
-- Answer:
SELECT first_name, last_name
FROM customers
WHERE last_name LIKE 'S%';

-- Question 12: How do you find orders with a discount amount greater than $0 but less than $100?
-- Explanation: Combine `AND` with `>` and `<` operators to filter rows within a specific range.
-- Answer:
SELECT order_id, order_amount, discount_amount
FROM orders
WHERE discount_amount > 0 AND discount_amount < 100;

-- Question 13: How can you find products priced under $100 or over $1000?
-- Explanation: Use `OR` to filter rows where `price` meets either condition.
-- Answer:
SELECT product_name, price
FROM products
WHERE price < 100 OR price > 1000;

-- Question 14: How do you find customers with emails containing 'gmail'?
-- Explanation: Use `LIKE` to search for 'gmail' within the `email` column.
-- Answer:
SELECT first_name, last_name, email
FROM customers
WHERE email LIKE '%gmail%';

-- Question 15: How can you find sales records with missing quantities?
-- Explanation: Use `IS NULL` to identify rows in the `sales` table where the `quantity` column is null.
-- Answer:
SELECT sale_id, product_id
FROM sales
WHERE quantity IS NULL;

-- =====================================
-- Final Notes
-- =====================================
-- - Ensure the `mysql_learning` database is set up with the sample datasets (customers.csv, orders.csv, products.csv, sales.csv) loaded.
-- - Run these queries in a MySQL client (e.g., MySQL Workbench) to practice and verify results.
-- - Use the questions to test your understanding, and modify conditions (e.g., cities, ranges) for additional practice.
-- - For further learning, explore the `SQL_Basics.sql` script or other files in the `basics/` folder.
