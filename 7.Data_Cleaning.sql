-- Data_Cleaning.sql
-- This script provides a comprehensive lesson on MySQL data cleaning techniques for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Data Cleaning Techniques
-- =====================================
-- The following queries demonstrate core data cleaning techniques: handling duplicates, missing values, 
-- string manipulation, data validation, and standardization.

-- 1. Identify duplicates: Find duplicate customer records by email
-- Purpose: Detect duplicate customer records based on email address
SELECT email, COUNT(*) AS email_count
FROM customers
GROUP BY email
HAVING email_count > 1;

-- 2. Remove duplicates: Delete duplicate customer records, keeping the latest
-- Purpose: Retain the customer record with the highest customer_id for each email
DELETE c1 FROM customers c1
INNER JOIN customers c2
WHERE c1.email = c2.email
AND c1.customer_id < c2.customer_id;

-- 3. Handle missing values: Replace NULL emails with a default value
-- Purpose: Update NULL email addresses to a placeholder
UPDATE customers
SET email = 'unknown@example.com'
WHERE email IS NULL;

-- 4. Handle missing values with COALESCE: Retrieve first non-NULL contact method
-- Purpose: Use COALESCE to prioritize email, then phone, then a default value
SELECT 
    customer_id,
    first_name,
    last_name,
    COALESCE(email, phone, 'No contact info') AS contact_info
FROM customers;

-- 5. String trimming: Remove leading/trailing spaces from product names
-- Purpose: Clean product names by removing unnecessary whitespace
UPDATE products
SET product_name = TRIM(product_name);

-- 6. String case standardization: Convert city names to title case
-- Purpose: Standardize city names for consistency
UPDATE customers
SET city = CONCAT(UPPER(LEFT(city, 1)), LOWER(SUBSTRING(city, 2)));

-- 7. String replacement: Correct common misspellings in product names
-- Purpose: Replace 'Phne' with 'Phone' in product names
UPDATE products
SET product_name = REPLACE(product_name, 'Phne', 'Phone');

-- 8. String concatenation: Create full customer names
-- Purpose: Combine first_name and last_name for reporting
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name
FROM customers;

-- 9. Pattern matching for validation: Identify invalid email formats
-- Purpose: Find emails not matching a basic email pattern
SELECT 
    customer_id,
    email
FROM customers
WHERE email NOT LIKE '%@%.%' 
AND email IS NOT NULL;

-- 10. Numeric outlier detection: Identify orders with unusually high amounts
-- Purpose: Flag orders with amounts exceeding 3 standard deviations
SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_amount > (
    SELECT AVG(order_amount) + 3 * STDDEV(order_amount)
    FROM orders
);

-- 11. Date standardization: Convert string dates to DATE format
-- Purpose: Update a varchar order_date column to DATE format (assumes a temp column)
ALTER TABLE orders ADD temp_date DATE;
UPDATE orders
SET temp_date = STR_TO_DATE(order_date, '%m/%d/%Y')
WHERE order_date IS NOT NULL;
ALTER TABLE orders DROP COLUMN order_date;
ALTER TABLE orders CHANGE temp_date order_date DATE;

-- 12. Handling inconsistent categories: Standardize product categories
-- Purpose: Map variant category names to a standard set
UPDATE products
SET category = CASE 
    WHEN category IN ('Electronics', 'Electronic', 'Tech') THEN 'Electronics'
    WHEN category IN ('Clothing', 'Apparel') THEN 'Clothing'
    ELSE category
END;

-- 13. Removing non-alphanumeric characters: Clean phone numbers
-- Purpose: Remove non-numeric characters from phone numbers
UPDATE customers
SET phone = REGEXP_REPLACE(phone, '[^0-9]', '')
WHERE phone IS NOT NULL;

-- 14. Splitting strings: Extract area code from phone numbers
-- Purpose: Create a new column for area codes from phone numbers
ALTER TABLE customers ADD area_code VARCHAR(3);
UPDATE customers
SET area_code = LEFT(phone, 3)
WHERE phone IS NOT NULL AND LENGTH(phone) >= 3;

-- 15. Aggregating cleaned data: Summarize orders after cleaning
-- Purpose: Calculate total orders and amount per customer post-cleaning
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING total_orders > 0;

-- Lesson Notes:
-- - Always back up data before running cleaning operations (e.g., use CREATE TABLE backup AS SELECT * FROM table).
-- - Use SELECT to preview changes before applying UPDATE or DELETE.
-- - REGEXP_REPLACE and LIKE are powerful for string cleaning but may need careful testing.
-- - Handle NULLs explicitly with IS NULL or COALESCE to avoid unexpected results.
-- - Ensure sample datasets (customers.csv, orders.csv, products.csv) are loaded.
-- - Verify changes with counts or samples to ensure correctness.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of data cleaning concepts. Each question includes an explanation and the SQL query as the answer.

-- Question 1: How do you identify duplicate email addresses in the customers table?
-- Explanation: GROUP BY email and HAVING COUNT(*) > 1 finds emails appearing multiple times.
-- Answer:
SELECT email, COUNT(*) AS email_count
FROM customers
GROUP BY email
HAVING email_count > 1;

-- Question 2: How can you remove duplicate customer records, keeping the record with the highest customer_id?
-- Explanation: A self-join identifies duplicates, and DELETE removes the older (lower customer_id) records.
-- Answer:
DELETE c1 FROM customers c1
INNER JOIN customers c2
WHERE c1.email = c2.email
AND c1.customer_id < c2.customer_id;

-- Question 3: How do you replace NULL email addresses with a default value?
-- Explanation: UPDATE with WHERE IS NULL sets a placeholder for missing emails.
-- Answer:
UPDATE customers
SET email = 'unknown@example.com'
WHERE email IS NULL;

-- Question 4: How can you retrieve the first non-NULL contact method for customers?
-- Explanation: COALESCE returns the first non-NULL value from a list of columns.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name,
    COALESCE(email, phone, 'No contact info') AS contact_info
FROM customers;

-- Question 5: How do you remove leading and trailing spaces from product names?
-- Explanation: TRIM removes whitespace from both ends of a string.
-- Answer:
UPDATE products
SET product_name = TRIM(product_name);

-- Question 6: How can you standardize city names to title case?
-- Explanation: CONCAT with UPPER and LOWER formats the first letter as uppercase and the rest as lowercase.
-- Answer:
UPDATE customers
SET city = CONCAT(UPPER(LEFT(city, 1)), LOWER(SUBSTRING(city, 2)));

-- Question 7: How do you correct the misspelling 'Phne' to 'Phone' in product names?
-- Explanation: REPLACE substitutes one substring with another in a column.
-- Answer:
UPDATE products
SET product_name = REPLACE(product_name, 'Phne', 'Phone');

-- Question 8: How can you combine first_name and last_name into a full name?
-- Explanation: CONCAT combines strings with a space separator for readable names.
-- Answer:
SELECT 
    customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name
FROM customers;

-- Question 9: How do you identify customers with invalid email formats?
-- Explanation: LIKE with '%@%.%' checks for basic email structure; excludes NULLs for clarity.
-- Answer:
SELECT 
    customer_id,
    email
FROM customers
WHERE email NOT LIKE '%@%.%' 
AND email IS NOT NULL;

-- Question 10: How can you flag orders with unusually high amounts?
-- Explanation: A subquery calculates the mean plus 3 standard deviations to identify outliers.
-- Answer:
SELECT 
    order_id,
    order_amount
FROM orders
WHERE order_amount > (
    SELECT AVG(order_amount) + 3 * STDDEV(order_amount)
    FROM orders
);

-- Question 11: How do you convert a string order_date to DATE format?
-- Explanation: STR_TO_DATE converts a string to DATE; a temporary column avoids data loss.
-- Answer:
ALTER TABLE orders ADD temp_date DATE;
UPDATE orders
SET temp_date = STR_TO_DATE(order_date, '%m/%d/%Y')
WHERE order_date IS NOT NULL;
ALTER TABLE orders DROP COLUMN order_date;
ALTER TABLE orders CHANGE temp_date order_date DATE;

-- Question 12: How can you standardize inconsistent product category names?
-- Explanation: CASE maps variant names to a standard set for consistency.
-- Answer:
UPDATE products
SET category = CASE 
    WHEN category IN ('Electronics', 'Electronic', 'Tech') THEN 'Electronics'
    WHEN category IN ('Clothing', 'Apparel') THEN 'Clothing'
    ELSE category
END;

-- Question 13: How do you clean phone numbers by removing non-numeric characters?
-- Explanation: REGEXP_REPLACE removes all characters except digits.
-- Answer:
UPDATE customers
SET phone = REGEXP_REPLACE(phone, '[^0-9]', '')
WHERE phone IS NOT NULL;

-- Question 14: How can you extract the area code from phone numbers into a new column?
-- Explanation: LEFT extracts the first 3 characters; a new column stores the result.
-- Answer:
ALTER TABLE customers ADD area_code VARCHAR(3);
UPDATE customers
SET area_code = LEFT(phone, 3)
WHERE phone IS NOT NULL AND LENGTH(phone) >= 3;

-- Question 15: How do you summarize orders per customer after cleaning the data?
-- Explanation: A LEFT JOIN with GROUP BY aggregates orders, filtering for customers with orders.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.order_amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING total_orders > 0;

-- =====================================
-- Final Notes
-- =====================================
-- - Always test cleaning queries on a small dataset or with SELECT before applying updates.
-- - Use transactions (START TRANSACTION; ROLLBACK;) for reversible updates.
-- - Document cleaning steps to ensure reproducibility.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
-- - For further learning, explore `Advanced_Queries.sql` in the `advanced/` folder for combining cleaning with complex queries.
