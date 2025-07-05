-- Table_Design.sql
-- This script provides an intermediate-level lesson on MySQL table design for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database is created but does not require pre-loaded sample datasets.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Table Design
-- =====================================
-- The following queries demonstrate core MySQL table design concepts: creating tables, primary/foreign keys, indexes, and constraints.

-- 1. Creating a basic table
-- Purpose: Create a simple table for storing customer information
CREATE TABLE customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

-- 2. Adding a primary key
-- Purpose: Ensure customer_id is unique and not null
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    price DECIMAL(10,2)
);

-- 3. Auto-incrementing primary key
-- Purpose: Automatically generate unique IDs for orders
CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    order_amount DECIMAL(10,2)
);

-- 4. Adding a foreign key
-- Purpose: Link orders to customers with referential integrity
CREATE TABLE order_details (
    order_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    product_id INT,
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- 5. Foreign key with ON DELETE CASCADE
-- Purpose: Automatically delete order details if the referenced order is deleted
CREATE TABLE order_reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    rating INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);

-- 6. Adding a unique constraint
-- Purpose: Ensure email addresses in customers table are unique
CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

-- 7. Adding a check constraint
-- Purpose: Ensure product prices are positive
CREATE TABLE inventory (
    inventory_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    stock_quantity INT,
    CHECK (stock_quantity >= 0)
);

-- 8. Creating an index
-- Purpose: Improve query performance on customer last names
CREATE TABLE clients (
    client_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    INDEX idx_last_name (last_name)
);

-- 9. Composite primary key
-- Purpose: Use multiple columns as a unique identifier
CREATE TABLE customer_preferences (
    customer_id INT,
    product_id INT,
    preference_type VARCHAR(50),
    PRIMARY KEY (customer_id, product_id)
);

-- 10. Composite index
-- Purpose: Optimize queries filtering on multiple columns
CREATE TABLE sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    sale_date DATE,
    amount DECIMAL(10,2),
    INDEX idx_customer_date (customer_id, sale_date)
);

-- 11. Adding a default value
-- Purpose: Set a default order status
CREATE TABLE shipments (
    shipment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    status VARCHAR(20) DEFAULT 'Pending'
);

-- 12. Using NOT NULL constraint
-- Purpose: Ensure critical fields cannot be null
CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL
);

-- 13. Creating a table with multiple constraints
-- Purpose: Combine primary key, foreign key, and check constraints
CREATE TABLE product_reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    rating INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    CHECK (rating BETWEEN 1 AND 5)
);

-- 14. Dropping a table
-- Purpose: Remove a table if it exists to avoid errors
DROP TABLE IF EXISTS temp_table;

-- 15. Altering a table to add a column
-- Purpose: Add a phone number column to an existing customers table
ALTER TABLE customers
ADD phone VARCHAR(20);

-- Lesson Notes:
-- - Primary keys ensure uniqueness and are required for foreign key references.
-- - `AUTO_INCREMENT` generates sequential IDs, typically used with primary keys.
-- - Foreign keys enforce referential integrity; `ON DELETE CASCADE` removes dependent rows automatically.
-- - Indexes improve query performance but increase storage and update overhead.
-- - Use `CHECK` constraints (MySQL 8.0+) to enforce data rules; not all MySQL versions support this.
-- - Always verify table structure with `DESCRIBE table_name;` after creation.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of table design concepts. Each question includes an explanation and the SQL query as the answer.

-- Question 1: How do you create a table for storing product categories?
-- Explanation: Define a table with a primary key and a name field.
-- Answer:
CREATE TABLE product_categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50)
);

-- Question 2: How can you create a table with an auto-incrementing primary key for suppliers?
-- Explanation: Use `AUTO_INCREMENT` to generate unique supplier IDs automatically.
-- Answer:
CREATE TABLE suppliers (
    supplier_id INT AUTO_INCREMENT PRIMARY KEY,
    supplier_name VARCHAR(100)
);

-- Question 3: How do you create a table for order items with a foreign key to orders?
-- Explanation: Use `FOREIGN KEY` to link to the `orders` table for referential integrity.
-- Answer:
CREATE TABLE order_items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    quantity INT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Question 4: How can you ensure deleted orders automatically remove related reviews?
-- Explanation: Use `ON DELETE CASCADE` to delete dependent rows in the reviews table.
-- Answer:
CREATE TABLE customer_reviews (
    review_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    review_text TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);

-- Question 5: How do you create a table with a unique email constraint?
-- Explanation: Use the `UNIQUE` constraint to prevent duplicate email addresses.
-- Answer:
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(100) UNIQUE,
    username VARCHAR(50)
);

-- Question 6: How can you ensure inventory quantities are non-negative?
-- Explanation: Use a `CHECK` constraint to enforce non-negative stock quantities.
-- Answer:
CREATE TABLE stock (
    stock_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    quantity INT,
    CHECK (quantity >= 0)
);

-- Question 7: How do you create an index on a product name column?
-- Explanation: Use `INDEX` to optimize queries filtering or sorting by product name.
-- Answer:
CREATE TABLE items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    item_name VARCHAR(100),
    INDEX idx_item_name (item_name)
);

-- Question 8: How can you create a table with a composite primary key for customer orders?
-- Explanation: Combine multiple columns to form a unique identifier.
-- Answer:
CREATE TABLE customer_orders (
    customer_id INT,
    order_id INT,
    order_status VARCHAR(20),
    PRIMARY KEY (customer_id, order_id)
);

-- Question 9: How do you create a composite index on order date and amount?
-- Explanation: Use `INDEX` on multiple columns to optimize multi-column queries.
-- Answer:
CREATE TABLE transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    INDEX idx_date_amount (order_date, amount)
);

-- Question 10: How can you set a default value for a payment status column?
-- Explanation: Use `DEFAULT` to assign a default value to a column.
-- Answer:
CREATE TABLE payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    status VARCHAR(20) DEFAULT 'Pending'
);

-- Question 11: How do you create a table with a non-nullable name column?
-- Explanation: Use `NOT NULL` to ensure the column always has a value.
-- Answer:
CREATE TABLE departments (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(50) NOT NULL
);

-- Question 12: How can you combine multiple constraints in a table for product ratings?
-- Explanation: Use primary key, foreign key, and check constraints together.
-- Answer:
CREATE TABLE ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    score INT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    CHECK (score BETWEEN 1 AND 10)
);

-- Question 13: How do you safely drop a table named 'archive'?
-- Explanation: Use `DROP TABLE IF EXISTS` to avoid errors if the table doesn’t exist.
-- Answer:
DROP TABLE IF EXISTS archive;

-- Question 14: How can you add a column for addresses to an existing customers table?
-- Explanation: Use `ALTER TABLE` to modify an existing table structure.
-- Answer:
ALTER TABLE customers
ADD address VARCHAR(200);

-- Question 15: How do you create a table for customer addresses with multiple constraints?
-- Explanation: Combine primary key, foreign key, and not null constraints.
-- Answer:
CREATE TABLE customer_addresses (
    address_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    address_line VARCHAR(100) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- =====================================
-- Final Notes
-- =====================================
-- - Ensure the `mysql_learning` database is created before running these queries.
-- - Use `DESCRIBE table_name;` to verify table structure after creation.
-- - Test constraints (e.g., inserting invalid data) to understand their behavior.
-- - For further learning, explore the `Joins_and_Unions.sql` script in the `intermediate/` folder.
