-- JSON_Features.sql
-- This script provides a comprehensive lesson on MySQL JSON data handling and querying for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on JSON functions, extraction, and manipulation.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, customer_details) are loaded.
-- Requires MySQL 8.0+ for full JSON functionality.

-- Use the mysql_learning database
USE mysql_learning;

-- Create a table with a JSON column for customer details
CREATE TABLE IF NOT EXISTS customer_details (
    customer_id INT PRIMARY KEY,
    details JSON
);

-- Populate sample JSON data
INSERT INTO customer_details (customer_id, details)
VALUES 
    (1, '{"name": "John Doe", "age": 30, "city": "New York", "preferences": {"email_notifications": true, "categories": ["Electronics", "Books"]}}'),
    (2, '{"name": "Jane Smith", "age": 25, "city": "Los Angeles", "preferences": {"email_notifications": false, "categories": ["Clothing"]}}'),
    (3, '{"name": "Alice Johnson", "age": 35, "city": "Chicago", "preferences": {"email_notifications": true, "categories": ["Books", "Toys"]}}');

-- =====================================
-- Lesson: MySQL JSON Handling and Querying
-- =====================================
-- The following queries demonstrate core JSON concepts: creating JSON data, extracting values, 
-- querying with JSON functions, and updating JSON fields.

-- 1. Extract JSON field: Retrieve name from details
-- Purpose: Use JSON_EXTRACT to get a specific JSON field
SELECT 
    customer_id,
    JSON_EXTRACT(details, '$.name') AS customer_name
FROM customer_details;

-- 2. Extract with operator: Use ->> for unquoted JSON value
-- Purpose: Simplify extraction of unquoted strings with ->>
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details;

-- 3. Filter by JSON value: Find customers with email notifications enabled
-- Purpose: Query JSON fields using JSON_EXTRACT in WHERE clause
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details
WHERE JSON_EXTRACT(details, '$.preferences.email_notifications') = true;

-- 4. Extract nested JSON: Retrieve categories from preferences
-- Purpose: Access nested JSON arrays using JSON_EXTRACT
SELECT 
    customer_id,
    JSON_EXTRACT(details, '$.preferences.categories') AS categories
FROM customer_details;

-- 5. JSON array length: Count categories in preferences
-- Purpose: Use JSON_LENGTH to count elements in a JSON array
SELECT 
    customer_id,
    JSON_LENGTH(details, '$.preferences.categories') AS category_count
FROM customer_details;

-- 6. JSON search: Find customers interested in 'Books'
-- Purpose: Use JSON_CONTAINS to search for a value in a JSON array
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details
WHERE JSON_CONTAINS(details, '"Books"', '$.preferences.categories');

-- 7. Update JSON field: Change email notification preference
-- Purpose: Use JSON_SET to update a specific JSON field
UPDATE customer_details
SET details = JSON_SET(details, '$.preferences.email_notifications', false)
WHERE customer_id = 1;

-- 8. Add JSON field: Insert a new field into JSON
-- Purpose: Use JSON_INSERT to add a loyalty_status field
UPDATE customer_details
SET details = JSON_INSERT(details, '$.loyalty_status', 'Silver')
WHERE customer_id = 2;

-- 9. Remove JSON field: Delete a field from JSON
-- Purpose: Use JSON_REMOVE to delete the age field
UPDATE customer_details
SET details = JSON_REMOVE(details, '$.age')
WHERE customer_id = 3;

-- 10. JSON array append: Add a category to preferences
-- Purpose: Use JSON_ARRAY_APPEND to add an element to a JSON array
UPDATE customer_details
SET details = JSON_ARRAY_APPEND(details, '$.preferences.categories', 'Furniture')
WHERE customer_id = 1;

-- 11. JSON table: Convert JSON to relational format
-- Purpose: Use JSON_TABLE to extract JSON data into a table
SELECT 
    customer_id,
    jt.name,
    jt.city,
    jt.categories
FROM customer_details,
JSON_TABLE(
    details,
    '$' COLUMNS (
        name VARCHAR(50) PATH '$.name',
        city VARCHAR(100) PATH '$.city',
        categories JSON PATH '$.preferences.categories'
    )
) AS jt;

-- 12. Aggregate JSON data: Count customers by city
-- Purpose: Aggregate JSON-extracted values like a regular column
SELECT 
    details->>'$.city' AS city,
    COUNT(*) AS customer_count
FROM customer_details
GROUP BY details->>'$.city';

-- 13. JSON path query: Filter by nested JSON condition
-- Purpose: Query customers with more than 2 categories
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details
WHERE JSON_LENGTH(details, '$.preferences.categories') > 2;

-- 14. JSON merge: Combine JSON objects
-- Purpose: Use JSON_MERGE_PRESERVE to merge additional details
UPDATE customer_details
SET details = JSON_MERGE_PRESERVE(details, '{"last_purchase": "2025-06-01"}')
WHERE customer_id = 2;

-- 15. JSON validation: Check for valid JSON
-- Purpose: Use JSON_VALID to ensure data integrity
SELECT 
    customer_id,
    CASE WHEN JSON_VALID(details) THEN 'Valid' ELSE 'Invalid' END AS json_status
FROM customer_details;

-- Lesson Notes:
-- - JSON functions (JSON_EXTRACT, JSON_SET, JSON_CONTAINS) enable flexible handling of semi-structured data.
-- - Use -> and ->> operators for concise JSON access; ->> removes quotes from strings.
-- - JSON_TABLE converts JSON to relational format for complex queries.
-- - Indexes on JSON fields require generated columns for performance (not covered here).
-- - Ensure JSON data is valid using JSON_VALID to avoid errors.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv, customer_details).

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of JSON handling concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you extract the name field from a JSON column?
-- Explanation: JSON_EXTRACT retrieves the value of a specified JSON path.
-- Answer:
SELECT 
    customer_id,
    JSON_EXTRACT(details, '$.name') AS customer_name
FROM customer_details;

-- Question 2: How can you extract an unquoted JSON string value?
-- Explanation: The ->> operator extracts a JSON field as an unquoted string.
-- Answer:
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details;

-- Question 3: How do you filter customers with email notifications enabled?
-- Explanation: JSON_EXTRACT in the WHERE clause filters based on a JSON boolean field.
-- Answer:
SELECT 
    customer_id,
    details->>'$.name' AS customer_name
FROM customer_details
WHERE JSON_EXTRACT(details, '$.preferences.email_notifications') = true;

-- Question 4: How can you extract a nested JSON array of categories?
-- Explanation: JSON_EXTRACT accesses nested arrays within the JSON
