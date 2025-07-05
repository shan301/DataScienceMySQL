-- Stored_Procedures.sql
-- This script provides a comprehensive lesson on MySQL stored procedures for data analysts and scientists.
-- It includes 15 example stored procedures with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- =====================================
-- Lesson: MySQL Stored Procedures
-- =====================================
-- The following stored procedures demonstrate core concepts: creating procedures, using parameters, conditional logic, 
-- cursors, and error handling. Stored procedures encapsulate reusable SQL logic for data processing and analysis.

-- 1. Basic stored procedure: Retrieve all customers
-- Purpose: Create a simple procedure to fetch all customer records
DELIMITER //
CREATE PROCEDURE GetAllCustomers()
BEGIN
    SELECT * FROM customers;
END //
DELIMITER ;

-- 2. Procedure with IN parameter: Filter customers by city
-- Purpose: Retrieve customers from a specified city
DELIMITER //
CREATE PROCEDURE GetCustomersByCity(IN city_name VARCHAR(100))
BEGIN
    SELECT first_name, last_name, city
    FROM customers
    WHERE city = city_name;
END //
DELIMITER ;

-- 3. Procedure with OUT parameter: Count customers in a city
-- Purpose: Return the number of customers in a given city
DELIMITER //
CREATE PROCEDURE CountCustomersByCity(IN city_name VARCHAR(100), OUT customer_count INT)
BEGIN
    SELECT COUNT(*) INTO customer_count
    FROM customers
    WHERE city = city_name;
END //
DELIMITER ;

-- 4. Procedure with INOUT parameter: Update and return order amount
-- Purpose: Apply a discount to an order and return the new amount
DELIMITER //
CREATE PROCEDURE ApplyDiscount(IN order_id INT, INOUT order_amount DECIMAL(10,2), IN discount_percent DECIMAL(5,2))
BEGIN
    SET order_amount = order_amount * (1 - discount_percent / 100);
    UPDATE orders
    SET order_amount = order_amount
    WHERE orders.order_id = order_id;
END //
DELIMITER ;

-- 5. Procedure with conditional logic: Categorize customer spending
-- Purpose: Classify customers as 'High', 'Medium', or 'Low' spenders
DELIMITER //
CREATE PROCEDURE CategorizeCustomerSpending(IN customer_id INT)
BEGIN
    DECLARE total_spent DECIMAL(10,2);
    SELECT SUM(order_amount) INTO total_spent
    FROM orders
    WHERE orders.customer_id = customer_id;
    
    IF total_spent > 1000 THEN
        SELECT 'High Spender' AS spending_category;
    ELSEIF total_spent > 500 THEN
        SELECT 'Medium Spender' AS spending_category;
    ELSE
        SELECT 'Low Spender' AS spending_category;
    END IF;
END //
DELIMITER ;

-- 6. Procedure with loop: Process multiple orders
-- Purpose: Apply a fixed discount to all orders above a threshold
DELIMITER //
CREATE PROCEDURE BulkDiscountOrders(IN discount_percent DECIMAL(5,2), IN min_amount DECIMAL(10,2))
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE curr_order_id INT;
    DECLARE curr_amount DECIMAL(10,2);
    DECLARE order_cursor CURSOR FOR
        SELECT order_id, order_amount
        FROM orders
        WHERE order_amount > min_amount;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    
    OPEN order_cursor;
    read_loop: LOOP
        FETCH order_cursor INTO curr_order_id, curr_amount;
        IF done THEN
            LEAVE read_loop;
        END IF;
        UPDATE orders
        SET order_amount = curr_amount * (1 - discount_percent / 100)
        WHERE order_id = curr_order_id;
    END LOOP;
    CLOSE order_cursor;
END //
DELIMITER ;

-- 7. Procedure with error handling: Validate order insertion
-- Purpose: Insert a new order with validation
DELIMITER //
CREATE PROCEDURE InsertOrder(
    IN customer_id INT,
    IN order_amount DECIMAL(10,2),
    IN product_id INT
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error inserting order';
    END;
    
    IF NOT EXISTS (SELECT 1 FROM customers WHERE customers.customer_id = customer_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid customer ID';
    END IF;
    
    INSERT INTO orders (customer_id, order_amount, product_id, order_date)
    VALUES (customer_id, order_amount, product_id, CURDATE());
END //
DELIMITER ;

-- 8. Procedure with temporary table: Aggregate sales by category
-- Purpose: Create a temporary table to store category sales
DELIMITER //
CREATE PROCEDURE AggregateSalesByCategory()
BEGIN
    CREATE TEMPORARY TABLE temp_category_sales (
        category VARCHAR(100),
        total_sales DECIMAL(10,2)
    );
    
    INSERT INTO temp_category_sales
    SELECT p.category, SUM(o.order_amount) AS total_sales
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY p.category;
    
    SELECT * FROM temp_category_sales;
    
    DROP TEMPORARY TABLE temp_category_sales;
END //
DELIMITER ;

-- 9. Procedure with multiple result sets: Customer and order summary
-- Purpose: Return customer details and their order summary
DELIMITER //
CREATE PROCEDURE GetCustomerOrderSummary(IN customer_id INT)
BEGIN
    SELECT first_name, last_name, city
    FROM customers
    WHERE customers.customer_id = customer_id;
    
    SELECT 
        order_id,
        order_amount,
        order_date
    FROM orders
    WHERE orders.customer_id = customer_id;
END //
DELIMITER ;

-- 10. Procedure with transaction: Update order and log
-- Purpose: Update an order and log the change in a log table
DELIMITER //
CREATE PROCEDURE UpdateOrderWithLog(
    IN order_id INT,
    IN new_amount DECIMAL(10,2)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Transaction failed';
    END;
    
    START TRANSACTION;
    INSERT INTO order_log (order_id, old_amount, new_amount, change_date)
    SELECT order_id, order_amount, new_amount, NOW()
    FROM orders
    WHERE orders.order_id = order_id;
    
    UPDATE orders
    SET order_amount = new_amount
    WHERE orders.order_id = order_id;
    COMMIT;
END //
DELIMITER ;

-- 11. Procedure with dynamic SQL: Filter orders dynamically
-- Purpose: Filter orders based on a dynamic condition
DELIMITER //
CREATE PROCEDURE DynamicOrderFilter(IN condition_clause VARCHAR(200))
BEGIN
    SET @query = CONCAT('SELECT order_id, customer_id, order_amount FROM orders WHERE ', condition_clause);
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- 12. Procedure with cursor and aggregation: Calculate average order per customer
-- Purpose: Compute and store average order amounts per customer
DELIMITER //
CREATE PROCEDURE CalculateAvgOrderPerCustomer()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE curr_customer_id INT;
    DECLARE avg_order DECIMAL(10,2);
    DECLARE customer_cursor CURSOR FOR
        SELECT customer_id
        FROM customers;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    
    CREATE TEMPORARY TABLE temp_avg_orders (
        customer_id INT,
        avg_order_amount DECIMAL(10,2)
    );
    
    OPEN customer_cursor;
    read_loop: LOOP
        FETCH customer_cursor INTO curr_customer_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        SELECT AVG(order_amount) INTO avg_order
        FROM orders
        WHERE orders.customer_id = curr_customer_id;
        INSERT INTO temp_avg_orders (customer_id, avg_order_amount)
        VALUES (curr_customer_id, avg_order);
    END LOOP;
    CLOSE customer_cursor;
    
    SELECT * FROM temp_avg_orders;
    DROP TEMPORARY TABLE temp_avg_orders;
END //
DELIMITER ;

-- 13. Procedure with date handling Galleons: Analyze recent orders
-- Purpose: Summarize orders within a specified date range
DELIMITER //
CREATE PROCEDURE SummarizeRecentOrders(IN start_date DATE, IN end_date DATE)
BEGIN
    SELECT 
        customer_id,
        COUNT(*) AS order_count,
        SUM(order_amount) AS total_amount
    FROM orders
    WHERE order_date BETWEEN start_date AND end_date
    GROUP BY customer_id;
END //
DELIMITER ;

-- 14. Procedure with multiple parameters: Update product prices
-- Purpose: Update prices for products in a specific category
DELIMITER //
CREATE PROCEDURE UpdateProductPrices(
    IN category_name VARCHAR(100),
    IN price_increase_percent DECIMAL(5,2)
)
BEGIN
    UPDATE products
    SET price = price * (1 + price_increase_percent / 100)
    WHERE category = category_name;
END //
DELIMITER ;

-- 15. Procedure with output and error handling: Validate customer email
-- Purpose: Update customer email with validation
DELIMITER //
CREATE PROCEDURE UpdateCustomerEmail(
    IN customer_id INT,
    IN new_email VARCHAR(255),
    OUT result_message VARCHAR(100)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET result_message = 'Error updating email';
    END;
    
    IF new_email NOT LIKE '%@%.%' THEN
        SET result_message = 'Invalid email format';
    ELSE
        UPDATE customers
        SET email = new_email
        WHERE customers.customer_id = customer_id;
        SET result_message = 'Email updated successfully';
    END IF;
END //
DELIMITER ;

-- Lesson Notes:
-- - Stored procedures encapsulate reusable SQL logic, improving modularity and security.
-- - Use DELIMITER to define procedures with multiple statements.
-- - IN, OUT, and INOUT parameters allow flexible input and output handling.
-- - Cursors enable row-by-row processing for complex tasks.
-- - Transactions ensure data integrity for multiple operations.
-- - Dynamic SQL with PREPARE/EXECUTE enables flexible queries.
-- - Error handling with SIGNAL and HANDLER improves robustness.
-- - Ensure the `mysql_learning` database and sample datasets are loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of stored procedures. Each question includes an explanation and the SQL answer.

-- Question 1: How do you create a stored procedure to retrieve all customers?
-- Explanation: A basic stored procedure uses CREATE PROCEDURE and BEGIN/END to encapsulate a simple SELECT query.
-- Answer:
DELIMITER //
CREATE PROCEDURE GetAllCustomers()
BEGIN
    SELECT * FROM customers;
END //
DELIMITER ;

-- Question 2: How can you create a procedure to filter customers by a specific city?
-- Explanation: An IN parameter allows passing a value to filter the query dynamically.
-- Answer:
DELIMITER //
CREATE PROCEDURE GetCustomersByCity(IN city_name VARCHAR(100))
BEGIN
    SELECT first_name, last_name, city
    FROM customers
    WHERE city = city_name;
END //
DELIMITER ;

-- Question 3: How do you create a procedure to count customers in a city and return the count?
-- Explanation: An OUT parameter stores the result of a query for use outside the procedure.
-- Answer:
DELIMITER //
CREATE PROCEDURE CountCustomersByCity(IN city_name VARCHAR(100), OUT customer_count INT)
BEGIN
    SELECT COUNT(*) INTO customer_count
    FROM customers
    WHERE city = city_name;
END //
DELIMITER ;

-- Question 4: How can you create a procedure to apply a discount to an order and update its amount?
-- Explanation: An INOUT parameter allows modifying and returning a value, such as an order amount.
-- Answer:
DELIMITER //
CREATE PROCEDURE ApplyDiscount(IN order_id INT, INOUT order_amount DECIMAL(10,2), IN discount_percent DECIMAL(5,2))
BEGIN
    SET order_amount = order_amount * (1 - discount_percent / 100);
    UPDATE orders
    SET order_amount = order_amount
    WHERE orders.order_id = order_id;
END //
DELIMITER ;

-- Question 5: How do you classify a customer’s spending using conditional logic in a procedure?
-- Explanation: IF/ELSEIF statements enable conditional logic based on computed values.
-- Answer:
DELIMITER //
CREATE PROCEDURE CategorizeCustomerSpending(IN customer_id INT)
BEGIN
    DECLARE total_spent DECIMAL(10,2);
    SELECT SUM(order_amount) INTO total_spent
    FROM orders
    WHERE orders.customer_id = customer_id;
    
    IF total_spent > 1000 THEN
        SELECT 'High Spender' AS spending_category;
    ELSEIF total_spent > 500 THEN
        SELECT 'Medium Spender' AS spending_category;
    ELSE
        SELECT 'Low Spender' AS spending_category;
    END IF;
END //
DELIMITER ;

-- Question 6: How can you apply a discount to multiple orders using a cursor?
-- Explanation: A cursor iterates over rows matching a condition, allowing updates in a loop.
-- Answer:
DELIMITER //
CREATE PROCEDURE BulkDiscountOrders(IN discount_percent DECIMAL(5,2), IN min_amount DECIMAL(10,2))
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE curr_order_id INT;
    DECLARE curr_amount DECIMAL(10,2);
    DECLARE order_cursor CURSOR FOR
        SELECT order_id, order_amount
        FROM orders
        WHERE order_amount > min_amount;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    
    OPEN order_cursor;
    read_loop: LOOP
        FETCH order_cursor INTO curr_order_id, curr_amount;
        IF done THEN
            LEAVE read_loop;
        END IF;
        UPDATE orders
        SET order_amount = curr_amount * (1 - discount_percent / 100)
        WHERE order_id = curr_order_id;
    END LOOP;
    CLOSE order_cursor;
END //
DELIMITER ;

-- Question 7: How do you create a procedure to insert an order with error handling?
-- Explanation: A handler catches SQL exceptions, and a custom condition validates input.
-- Answer:
DELIMITER //
CREATE PROCEDURE InsertOrder(
    IN customer_id INT,
    IN order_amount DECIMAL(10,2),
    IN product_id INT
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error inserting order';
    END;
    
    IF NOT EXISTS (SELECT 1 FROM customers WHERE customers.customer_id = customer_id) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid customer ID';
    END IF;
    
    INSERT INTO orders (customer_id, order_amount, product_id, order_date)
    VALUES (customer_id, order_amount, product_id, CURDATE());
END //
DELIMITER ;

-- Question 8: How can you create a procedure to aggregate sales into a temporary table?
-- Explanation: A temporary table stores intermediate results for further processing or display.
-- Answer:
DELIMITER //
CREATE PROCEDURE AggregateSalesByCategory()
BEGIN
    CREATE TEMPORARY TABLE temp_category_sales (
        category VARCHAR(100),
        total_sales DECIMAL(10,2)
    );
    
    INSERT INTO temp_category_sales
    SELECT p.category, SUM(o.order_amount) AS total_sales
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    GROUP BY p.category;
    
    SELECT * FROM temp_category_sales;
    
    DROP TEMPORARY TABLE temp_category_sales;
END //
DELIMITER ;

-- Question 9: How do you create a procedure that returns multiple result sets?
-- Explanation: Multiple SELECT statements in a procedure return multiple result sets.
-- Answer:
DELIMITER //
CREATE PROCEDURE GetCustomerOrderSummary(IN customer_id INT)
BEGIN
    SELECT first_name, last_name, city
    FROM customers
    WHERE customers.customer_id = customer_id;
    
    SELECT 
        order_id,
        order_amount,
        order_date
    FROM orders
    WHERE orders.customer_id = customer_id;
END //
DELIMITER ;

-- Question 10: How can you update an order and log the change in a transaction?
-- Explanation: Transactions ensure atomicity, and a log table tracks changes.
-- Answer:
DELIMITER //
CREATE PROCEDURE UpdateOrderWithLog(
    IN order_id INT,
    IN new_amount DECIMAL(10,2)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Transaction failed';
    END;
    
    START TRANSACTION;
    INSERT INTO order_log (order_id, old_amount, new_amount, change_date)
    SELECT order_id, order_amount, new_amount, NOW()
    FROM orders
    WHERE orders.order_id = order_id;
    
    UPDATE orders
    SET order_amount = new_amount
    WHERE orders.order_id = order_id;
    COMMIT;
END //
DELIMITER ;

-- Question 11: How do you create a procedure with dynamic SQL to filter orders?
-- Explanation: Dynamic SQL uses PREPARE/EXECUTE for flexible query conditions.
-- Answer:
DELIMITER //
CREATE PROCEDURE DynamicOrderFilter(IN condition_clause VARCHAR(200))
BEGIN
    SET @query = CONCAT('SELECT order_id, customer_id, order_amount FROM orders WHERE ', condition_clause);
    PREPARE stmt FROM @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

-- Question 12: How can you compute average order amounts per customer using a cursor?
-- Explanation: A cursor loops through customers, calculating and storing averages in a temporary table.
-- Answer:
DELIMITER //
CREATE PROCEDURE CalculateAvgOrderPerCustomer()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE curr_customer_id INT;
    DECLARE avg_order DECIMAL(10,2);
    DECLARE customer_cursor CURSOR FOR
        SELECT customer_id
        FROM customers;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    
    CREATE TEMPORARY TABLE temp_avg_orders (
        customer_id INT,
        avg_order_amount DECIMAL(10,2)
    );
    
    OPEN customer_cursor;
    read_loop: LOOP
        FETCH customer_cursor INTO curr_customer_id;
        IF done THEN
            LEAVE read_loop;
        END IF;
        SELECT AVG(order_amount) INTO avg_order
        FROM orders
        WHERE orders.customer_id = curr_customer_id;
        INSERT INTO temp_avg_orders (customer_id, avg_order_amount)
        VALUES (curr_customer_id, avg_order);
    END LOOP;
    CLOSE customer_cursor;
    
    SELECT * FROM temp_avg_orders;
    DROP TEMPORARY TABLE temp_avg_orders;
END //
DELIMITER ;

-- Question 13: How do you summarize orders within a date range?
-- Explanation: Parameters allow filtering by date range, with aggregation for summary statistics.
-- Answer:
DELIMITER //
CREATE PROCEDURE SummarizeRecentOrders(IN start_date DATE, IN end_date DATE)
BEGIN
    SELECT 
        customer_id,
        COUNT(*) AS order_count,
        SUM(order_amount) AS total_amount
    FROM orders
    WHERE order_date BETWEEN start_date AND end_date
    GROUP BY customer_id;
END //
DELIMITER ;

-- Question 14: How can you update product prices for a specific category?
-- Explanation: Parameters enable targeted updates to specific rows in a table.
-- Answer:
DELIMITER //
CREATE PROCEDURE UpdateProductPrices(
    IN category_name VARCHAR(100),
    IN price_increase_percent DECIMAL(5,2)
)
BEGIN
    UPDATE products
    SET price = price * (1 + price_increase_percent / 100)
    WHERE category = category_name;
END //
DELIMITER ;

-- Question 15: How do you validate and update a customer’s email in a procedure?
-- Explanation: Conditional logic and error handling ensure valid input before updating.
-- Answer:
DELIMITER //
CREATE PROCEDURE UpdateCustomerEmail(
    IN customer_id INT,
    IN new_email VARCHAR(255),
    OUT result_message VARCHAR(100)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        SET result_message = 'Error updating email';
    END;
    
    IF new_email NOT LIKE '%@%.%' THEN
        SET result_message = 'Invalid email format';
    ELSE
        UPDATE customers
        SET email = new_email
        WHERE customers.customer_id = customer_id;
        SET result_message = 'Email updated successfully';
    END IF;
END //
DELIMITER ;

-- =====================================
-- Final Notes
-- =====================================
-- - Stored procedures improve code reuse, security, and performance for repetitive tasks.
-- - Use MySQL Workbench or another client to test and call procedures (e.g., CALL GetAllCustomers();).
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
-- - Experiment with parameters, cursors, and dynamic SQL for complex logic.
-- - For further learning, explore `Triggers.sql` in the `advanced/` folder for automated procedure execution.
