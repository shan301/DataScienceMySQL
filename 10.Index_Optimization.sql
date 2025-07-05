-- Index_Optimization.sql
-- This script provides a comprehensive lesson on MySQL index creation and optimization for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on creating, managing, and optimizing indexes.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Index Creation and Optimization
-- =====================================
-- The following queries demonstrate core concepts: creating indexes (BTREE, HASH), analyzing query performance with EXPLAIN, 
-- managing composite indexes, and optimizing queries for large datasets.

-- 1. Create a BTREE index: Index on customer email
-- Purpose: Speed up searches on the email column
CREATE INDEX idx_customer_email ON customers(email);

-- 2. Create a composite index: Index on customer city and last_name
-- Purpose: Optimize queries filtering or sorting by city and last_name
CREATE INDEX idx_customer_city_lastname ON customers(city, last_name);

-- 3. Create a unique index: Ensure unique product names
-- Purpose: Prevent duplicate product names
CREATE UNIQUE INDEX idx_product_name ON products(product_name);

-- 4. Create an index for joins: Index on orders customer_id
-- Purpose: Improve performance of joins between orders and customers
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- 5. Use EXPLAIN: Analyze a simple SELECT query
-- Purpose: Check how MySQL executes a query on customers
EXPLAIN SELECT first_name, last_name 
FROM customers 
WHERE email = 'john.doe@example.com';

-- 6. Use EXPLAIN: Analyze a JOIN query
-- Purpose: Evaluate index usage in a customer-order join
EXPLAIN SELECT 
    c.customer_id,
    c.first_name,
    COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name;

-- 7. Create a covering index: Include columns in index
-- Purpose: Optimize queries by including frequently selected columns
CREATE INDEX idx_orders_covering ON orders(customer_id, order_amount, order_date);

-- 8. Drop an index: Remove an unused index
-- Purpose: Free up space and reduce maintenance overhead
DROP INDEX idx_customer_email ON customers;

-- 9. Create a HASH index: Optimize for equality searches
-- Purpose: Use HASH index for exact matches (MySQL InnoDB supports BTREE only; HASH shown for MyISAM)
CREATE TABLE temp_table (
    id INT PRIMARY KEY,
    category VARCHAR(50)
) ENGINE=MyISAM;
CREATE INDEX idx_category_hash ON temp_table(category) USING HASH;

-- 10. Optimize range queries: Index on order_date
-- Purpose: Speed up queries filtering by date ranges
CREATE INDEX idx_orders_date ON orders(order_date);

-- 11. Composite index for sorting: Index for ORDER BY optimization
-- Purpose: Optimize queries sorting by order_amount and order_date
CREATE INDEX idx_orders_amount_date ON orders(order_amount, order_date);

-- 12. Use EXPLAIN: Analyze a range query
-- Purpose: Check index usage for a date range query
EXPLAIN SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- 13. Partial index: Index on partial column data
-- Purpose: Create an index on the first 10 characters of product_name
CREATE INDEX idx_product_name_partial ON products(LEFT(product_name, 10));

-- 14. Index for GROUP BY: Optimize aggregation queries
-- Purpose: Speed up grouping by customer city
CREATE INDEX idx_customer_city ON customers(city);

-- 15. Monitor index usage: Check which indexes are used
-- Purpose: Query the performance schema to identify unused indexes
SELECT 
    OBJECT_SCHEMA,
    OBJECT_NAME,
    INDEX_NAME
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE INDEX_NAME IS NOT NULL
AND COUNT_STAR = 0
AND OBJECT_SCHEMA = 'mysql_learning';

-- Lesson Notes:
-- - Indexes improve query performance but increase storage and maintenance overhead.
-- - BTREE indexes (default in InnoDB) support range and equality queries; HASH indexes (MyISAM) are for equality only.
-- - Use EXPLAIN to analyze query execution plans and verify index usage.
-- - Composite indexes are effective for queries with multiple conditions or sorting.
-- - Covering indexes include all columns used in a query to avoid accessing the table.
-- - Drop unused indexes to reduce overhead; monitor usage with performance_schema.
-- - Ensure the `mysql_learning` database and sample datasets are loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of index optimization concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you create an index to speed up email searches in the customers table?
-- Explanation: A BTREE index on email improves performance for WHERE clauses with equality conditions.
-- Answer:
CREATE INDEX idx_customer_email ON customers(email);

-- Question 2: How can you create a composite index for filtering by city and sorting by last_name?
-- Explanation: A composite index on city and last_name optimizes queries with both columns in WHERE or ORDER BY.
-- Answer:
CREATE INDEX idx_customer_city_lastname ON customers(city, last_name);

-- Question 3: How do you ensure product names are unique using an index?
-- Explanation: A UNIQUE index enforces uniqueness and speeds up searches on product_name.
-- Answer:
CREATE UNIQUE INDEX idx_product_name ON products(product_name);

-- Question 4: How can you optimize joins between orders and customers?
-- Explanation: An index on orders.customer_id accelerates JOIN operations.
-- Answer:
CREATE INDEX idx_orders_customer_id ON orders(customer_id);

-- Question 5: How do you analyze the execution plan of a query filtering by email?
-- Explanation: EXPLAIN shows whether the idx_customer_email index is used.
-- Answer:
EXPLAIN SELECT first_name, last_name 
FROM customers 
WHERE email = 'john.doe@example.com';

-- Question 6: How can you evaluate index usage in a JOIN query?
-- Explanation: EXPLAIN analyzes whether indexes like idx_orders_customer_id are used in joins.
-- Answer:
EXPLAIN SELECT 
    c.customer_id,
    c.first_name,
    COUNT(o.order_id) AS order_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name;

-- Question 7: How do you create a covering index for an orders query?
-- Explanation: A covering index includes all columns in the SELECT, WHERE, and ORDER BY clauses.
-- Answer:
CREATE INDEX idx_orders_covering ON orders(customer_id, order_amount, order_date);

-- Question 8: How can you remove an unused index on the customers table?
-- Explanation: DROP INDEX removes an index to save space and reduce overhead.
-- Answer:
DROP INDEX idx_customer_email ON customers;

-- Question 9: How do you create a HASH index for exact matches (for MyISAM tables)?
-- Explanation: HASH indexes optimize equality searches but are only supported in MyISAM.
-- Answer:
CREATE TABLE temp_table (
    id INT PRIMARY KEY,
    category VARCHAR(50)
) ENGINE=MyISAM;
CREATE INDEX idx_category_hash ON temp_table(category) USING HASH;

-- Question 10: How can you optimize queries filtering by date ranges?
-- Explanation: An index on order_date speeds up range queries like BETWEEN.
-- Answer:
CREATE INDEX idx_orders_date ON orders(order_date);

-- Question 11: How do you optimize queries sorting by order_amount and order_date?
-- Explanation: A composite index on order_amount and order_date supports sorting.
-- Answer:
CREATE INDEX idx_orders_amount_date ON orders(order_amount, order_date);

-- Question 12: How can you analyze the execution plan for a date range query?
-- Explanation: EXPLAIN verifies if idx_orders_date is used for range filtering.
-- Answer:
EXPLAIN SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31';

-- Question 13: How do you create a partial index on the first 10 characters of product_name?
-- Explanation: A partial index reduces storage by indexing only part of a column.
-- Answer:
CREATE INDEX idx_product_name_partial ON products(LEFT(product_name, 10));

-- Question 14: How can you optimize queries grouping by customer city?
-- Explanation: An index on city accelerates GROUP BY operations.
-- Answer:
CREATE INDEX idx_customer_city ON customers(city);

-- Question 15: How do you identify unused indexes in the database?
-- Explanation: The performance_schema table shows indexes with no usage (COUNT_STAR = 0).
-- Answer:
SELECT 
    OBJECT_SCHEMA,
    OBJECT_NAME,
    INDEX_NAME
FROM performance_schema.table_io_waits_summary_by_index_usage
WHERE INDEX_NAME IS NOT NULL
AND COUNT_STAR = 0
AND OBJECT_SCHEMA = 'mysql_learning';

-- =====================================
-- Final Notes
-- =====================================
-- - Indexes should match query patterns (WHERE, JOIN, ORDER BY, GROUP BY) for maximum benefit.
-- - Use EXPLAIN to ensure indexes are used; look for 'Using index' in the Extra column.
-- - Avoid over-indexing to minimize storage and maintenance costs.
-- - Test index changes on a backup to avoid unintended performance impacts.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
-- - For further learning, explore `Partitioning.sql` for managing large datasets or `Advanced_Queries.sql` for complex query optimization.
