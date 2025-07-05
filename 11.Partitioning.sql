-- Partitioning.sql
-- This script provides a comprehensive lesson on MySQL table partitioning for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on RANGE, LIST, HASH, and KEY partitioning.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.
-- Requires MySQL 8.0+ with InnoDB engine for partitioning support.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Table Partitioning
-- =====================================
-- The following queries demonstrate core partitioning concepts: creating RANGE, LIST, HASH, and KEY partitions, 
-- managing partitions, and optimizing queries for large datasets.

-- 1. Create RANGE partition: Partition orders by order_date (yearly)
-- Purpose: Split orders table by year for faster date-based queries
CREATE TABLE orders_partitioned (
    order_id INT,
    customer_id INT,
    order_amount DECIMAL(10,2),
    order_date DATE,
    product_id INT,
    PRIMARY KEY (order_id, order_date)
)
PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p_2020 VALUES LESS THAN (2021),
    PARTITION p_2021 VALUES LESS THAN (2022),
    PARTITION p_2022 VALUES LESS THAN (2023),
    PARTITION p_2023 VALUES LESS THAN (2024),
    PARTITION p_2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- 2. Insert data into partitioned table: Populate orders_partitioned
-- Purpose: Copy data from orders to the partitioned table
INSERT INTO orders_partitioned
SELECT order_id, customer_id, order_amount, order_date, product_id
FROM orders;

-- 3. Query a specific partition: Retrieve orders from 2023
-- Purpose: Demonstrate partition pruning for faster queries
SELECT order_id, order_amount, order_date
FROM orders_partitioned
WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31';

-- 4. Create LIST partition: Partition customers by region
-- Purpose: Group customers by predefined regions
CREATE TABLE customers_partitioned (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(50)
)
PARTITION BY LIST COLUMNS (region) (
    PARTITION p_north VALUES IN ('North'),
    PARTITION p_south VALUES IN ('South'),
    PARTITION p_east VALUES IN ('East'),
    PARTITION p_west VALUES IN ('West'),
    PARTITION p_other VALUES IN (NULL, '')
);

-- 5. Create HASH partition: Partition products by product_id
-- Purpose: Distribute products evenly across 4 partitions
CREATE TABLE products_partitioned (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10,2),
    category VARCHAR(100)
)
PARTITION BY HASH (product_id)
PARTITIONS 4;

-- 6. Create KEY partition: Partition sales by product_id
-- Purpose: Use KEY partitioning for automatic distribution
CREATE TABLE sales_partitioned (
    sale_id INT,
    product_id INT,
    sale_amount DECIMAL(10,2),
    sale_date DATE,
    PRIMARY KEY (sale_id, product_id)
)
PARTITION BY KEY (product_id)
PARTITIONS 4;

-- 7. Analyze partition usage: Use EXPLAIN to check partition pruning
-- Purpose: Verify that MySQL uses only relevant partitions
EXPLAIN SELECT order_id, order_amount
FROM orders_partitioned
WHERE order_date = '2023-06-15';

-- 8. Add a partition: Extend orders_partitioned for future years
-- Purpose: Add a new partition for 2025
ALTER TABLE orders_partitioned
REORGANIZE PARTITION p_future INTO (
    PARTITION p_2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- 9. Drop a partition: Remove old partition (2020)
-- Purpose: Delete outdated data to save space
ALTER TABLE orders_partitioned
DROP PARTITION p_2020;

-- 10. Merge partitions: Combine 2021 and 2022 partitions
-- Purpose: Consolidate partitions for simplified management
ALTER TABLE orders_partitioned
REORGANIZE PARTITION p_2021, p_2022 INTO (
    PARTITION p_2021_2022 VALUES LESS THAN (2023)
);

-- 11. Query across partitions: Aggregate sales by year
-- Purpose: Perform aggregations across multiple partitions
SELECT 
    YEAR(order_date) AS order_year,
    SUM(order_amount) AS total_sales
FROM orders_partitioned
GROUP BY order_year;

-- 12. Rebuild partitions: Optimize partition performance
-- Purpose: Rebuild partitions to defragment and improve performance
ALTER TABLE orders_partitioned
REBUILD PARTITION p_2023, p_2024;

-- 13. Subpartitioning: Create RANGE with HASH subpartitions
-- Purpose: Combine RANGE and HASH for finer granularity
CREATE TABLE orders_subpartitioned (
    order_id INT,
    customer_id INT,
    order_amount DECIMAL(10,2),
    order_date DATE,
    PRIMARY KEY (order_id, order_date)
)
PARTITION BY RANGE (YEAR(order_date))
SUBPARTITION BY HASH (order_id) SUBPARTITIONS 2 (
    PARTITION p_2020 VALUES LESS THAN (2021) (
        SUBPARTITION sp0_2020,
        SUBPARTITION sp1_2020
    ),
    PARTITION p_2021 VALUES LESS THAN (2022) (
        SUBPARTITION sp0_2021,
        SUBPARTITION sp1_2021
    ),
    PARTITION p_future VALUES LESS THAN (MAXVALUE) (
        SUBPARTITION sp0_future,
        SUBPARTITION sp1_future
    )
);

-- 14. Check partition information: View partition details
-- Purpose: Inspect partition structure and row counts
SELECT 
    TABLE_NAME,
    PARTITION_NAME,
    PARTITION_METHOD,
    PARTITION_EXPRESSION,
    TABLE_ROWS
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = 'mysql_learning'
AND TABLE_NAME = 'orders_partitioned';

-- 15. Optimize query with partition pruning: Filter by specific partition
-- Purpose: Use explicit partition selection to improve performance
SELECT order_id, order_amount
FROM orders_partitioned PARTITION (p_2024)
WHERE order_date >= '2024-01-01';

-- Lesson Notes:
-- - Partitioning improves performance for large datasets by reducing the data scanned for queries.
-- - RANGE partitioning is ideal for time-based data; LIST for categorical data; HASH/KEY for even distribution.
-- - Partition pruning ensures only relevant partitions are scanned, visible in EXPLAIN output.
-- - Subpartitioning combines multiple strategies for complex use cases.
-- - Partition management (ADD, DROP, REORGANIZE) requires careful planning to avoid data loss.
-- - Ensure MySQL 8.0+ with InnoDB; verify sample datasets (customers.csv, orders.csv, products.csv, sales.csv) are loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of partitioning concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you create a RANGE-partitioned table for orders by year?
-- Explanation: RANGE partitioning splits data based on YEAR(order_date), with a MAXVALUE partition for future data.
---answer
CREATE TABLE orders_partitioned (
    order_id INT,
    customer_id INT,
    order_amount DECIMAL(10,2),
    order_date DATE,
    product_id INT,
    PRIMARY KEY (order_id, order_date)
)
PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p_2020 VALUES LESS THAN (2021),
    PARTITION p_2021 VALUES LESS THAN (2022),
    PARTITION p_2022 VALUES LESS THAN (2023),
    PARTITION p_2023 VALUES LESS THAN (2024),
    PARTITION p_2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Question 2: How can you populate a partitioned table with existing data?
-- Explanation: INSERT ... SELECT copies data from the original orders table to the partitioned table.
-- Answer:
INSERT INTO orders_partitioned
SELECT order_id, customer_id, order_amount, order_date, product_id
FROM orders;

-- Question 3: How do you query orders from a specific year (2023) in a partitioned table?
-- Explanation: A date range query benefits from partition pruning, scanning only the relevant partition.
-- Answer:
SELECT order_id, order_amount, order_date
FROM orders_partitioned
WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31';

-- Question 4: How can you create a LIST-partitioned table for customers by region?
-- Explanation: LIST partitioning groups rows by predefined region values.
-- Answer:
CREATE TABLE customers_partitioned (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(255),
    city VARCHAR(100),
    region VARCHAR(50)
)
PARTITION BY LIST COLUMNS (region) (
    PARTITION p_north VALUES IN ('North'),
    PARTITION p_south VALUES IN ('South'),
    PARTITION p_east VALUES IN ('East'),
    PARTITION p_west VALUES IN ('West'),
    PARTITION p_other VALUES IN (NULL, '')
);

-- Question 5: How do you create a HASH-partitioned table for products?
-- Explanation: HASH partitioning distributes rows evenly based on product_id.
-- Answer:
CREATE TABLE products_partitioned (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    price DECIMAL(10,2),
    category VARCHAR(100)
)
PARTITION BY HASH (product_id)
PARTITIONS 4;

-- Question 6: How can you create a KEY-partitioned table for sales?
-- Explanation: KEY partitioning uses a hash function on product_id for distribution.
-- Answer:
CREATE TABLE sales_partitioned (
    sale_id INT,
    product_id INT,
    sale_amount DECIMAL(10,2),
    sale_date DATE,
    PRIMARY KEY (sale_id, product_id)
)
PARTITION BY KEY (product_id)
PARTITIONS 4;

-- Question 7: How do you verify partition pruning for a query?
-- Explanation: EXPLAIN shows which partitions are accessed, confirming pruning efficiency.
-- Answer:
EXPLAIN SELECT order_id, order_amount
FROM orders_partitioned
WHERE order_date = '2023-06-15';

-- Question 8: How can you add a new partition for 2025?
-- Explanation: ALTER TABLE REORGANIZE PARTITION splits the MAXVALUE partition.
-- Answer:
ALTER TABLE orders_partitioned
REORGANIZE PARTITION p_future INTO (
    PARTITION p_2025 VALUES LESS THAN (2026),
    PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- Question 9: How do you drop an old partition (2020)?
-- Explanation: DROP PARTITION removes a specific partition and its data.
-- Answer:
ALTER TABLE orders
