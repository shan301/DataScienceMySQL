-- Error_Handling.sql
-- This script provides a comprehensive lesson on MySQL error handling techniques for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on SIGNAL, RESIGNAL, and error management in stored procedures and triggers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, error_log) are loaded.
-- Requires MySQL 8.0+ for full error handling support.

-- Use the mysql_learning database
USE mysql_learning;

-- Create an error log table for tracking errors
CREATE TABLE IF NOT EXISTS error_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    error_code VARCHAR(10),
    error_message VARCHAR(255),
    error_time DATETIME
);

-- =====================================
-- Lesson: MySQL Error Handling
-- =====================================
-- The following examples demonstrate core error handling concepts: SIGNAL for custom errors, RESIGNAL for rethrowing errors, 
-- DECLARE HANDLER for catching errors, and logging errors in stored procedures and triggers.

-- 1. Basic SIGNAL: Raise a custom error in a stored procedure
-- Purpose: Prevent insertion of negative order amounts
DELIMITER //
CREATE PROCEDURE InsertOrderWithValidation(
    IN p_customer_id INT,
    IN p_order_amount DECIMAL(10,2),
    IN p_product_id INT
)
BEGIN
    IF p_order_amount <= 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Order amount must be positive';
    END IF;
    INSERT INTO orders (customer_id, order_amount, product_id, order_date)
    VALUES (p_customer_id, p_order_amount, p_product_id, CURDATE());
END //
DELIMITER ;

-- 2. DECLARE EXIT HANDLER: Catch SQL errors in a stored procedure
-- Purpose: Log errors during order insertion
DELIMITER //
CREATE PROCEDURE InsertOrderWithErrorLog(
    IN p_customer_id INT,
    IN p_order_amount DECIMAL(10,2),
    IN p_product_id INT
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('SQLERR', 'Failed to insert order', NOW());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Error inserting order';
    END;
    INSERT INTO orders (customer_id, order_amount, product_id, order_date)
    VALUES (p_customer_id, p_order_amount, p_product_id, CURDATE());
END //
DELIMITER ;

-- 3. RESIGNAL: Rethrow an error with a custom message
-- Purpose: Catch and modify an error in a stored procedure
DELIMITER //
CREATE PROCEDURE UpdateOrderAmount(
    IN p_order_id INT,
    IN p_new_amount DECIMAL(10,2)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        RESIGNAL SET MESSAGE_TEXT = 'Failed to update order amount';
    END;
    UPDATE orders
    SET order_amount = p_new_amount
    WHERE order_id = p_order_id;
END //
DELIMITER ;

-- 4. DECLARE CONTINUE HANDLER: Continue after logging an error
-- Purpose: Log errors but continue processing in a procedure
DELIMITER //
CREATE PROCEDURE BulkUpdateOrders(
    IN p_min_amount DECIMAL(10,2),
    IN p_increase_percent DECIMAL(5,2)
)
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('SQLWARN', 'Warning during bulk update', NOW());
    END;
    UPDATE orders
    SET order_amount = order_amount * (1 + p_increase_percent / 100)
    WHERE order_amount > p_min_amount;
END //
DELIMITER ;

-- 5. SIGNAL in trigger: Validate product price before update
-- Purpose: Prevent excessive price changes in a trigger
DELIMITER //
CREATE TRIGGER before_product_price_update
BEFORE UPDATE ON products
FOR EACH ROW
BEGIN
    IF NEW.price > OLD.price * 2 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Price increase exceeds 100%';
    END IF;
END //
DELIMITER ;

-- 6. EXIT HANDLER in trigger: Log errors during order deletion
-- Purpose: Catch and log errors in a BEFORE DELETE trigger
DELIMITER //
CREATE TRIGGER before_order_delete
BEFORE DELETE ON orders
FOR EACH ROW
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('SQLERR', 'Error deleting order', NOW());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Failed to delete order';
    END;
    IF OLD.order_amount > 1000 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Cannot delete high-value orders';
    END IF;
END //
DELIMITER ;

-- 7. RESIGNAL in trigger: Modify error in customer update
-- Purpose: Catch and rethrow errors with a custom message in a trigger
DELIMITER //
CREATE TRIGGER before_customer_update
BEFORE UPDATE ON customers
FOR EACH ROW
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        RESIGNAL SET MESSAGE_TEXT = 'Failed to update customer';
    END;
    IF NEW.email IS NOT NULL AND NEW.email NOT LIKE '%@%.%' THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Invalid email format';
    END IF;
END //
DELIMITER ;

-- 8. Custom SQLSTATE: Define specific error codes
-- Purpose: Use a custom SQLSTATE for invalid customer data
DELIMITER //
CREATE PROCEDURE InsertCustomer(
    IN p_first_name VARCHAR(50),
    IN p_last_name VARCHAR(50),
    IN p_email VARCHAR(255)
)
BEGIN
    IF p_email IS NULL OR p_email = '' THEN
        SIGNAL SQLSTATE '45001'
        SET MESSAGE_TEXT = 'Email cannot be empty';
    END IF;
    INSERT INTO customers (first_name, last_name, email)
    VALUES (p_first_name, p_last_name, p_email);
END //
DELIMITER ;

-- 9. Logging errors: Store error details in error_log
-- Purpose: Log detailed error information in a stored procedure
DELIMITER //
CREATE PROCEDURE UpdateProductPrice(
    IN p_product_id INT,
    IN p_new_price DECIMAL(10,2)
)
BEGIN
    DECLARE error_code VARCHAR(10);
    DECLARE error_message VARCHAR(255);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            error_code = RETURNED_SQLSTATE,
            error_message = MESSAGE_TEXT;
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES (error_code, error_message, NOW());
        RESIGNAL;
    END;
    UPDATE products
    SET price = p_new_price
    WHERE product_id = p_product_id;
END //
DELIMITER ;

-- 10. Conditional error handling: Validate order date
-- Purpose: Ensure order_date is not in the future
DELIMITER //
CREATE PROCEDURE InsertOrderWithDateCheck(
    IN p_customer_id INT,
    IN p_order_amount DECIMAL(10,2),
    IN p_order_date DATE
)
BEGIN
    IF p_order_date > CURDATE() THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Order date cannot be in the future';
    END IF;
    INSERT INTO orders (customer_id, order_amount, order_date)
    VALUES (p_customer_id, p_order_amount, p_order_date);
END //
DELIMITER ;

-- 11. Handling foreign key errors: Catch constraint violations
-- Purpose: Log foreign key errors during order insertion
DELIMITER //
CREATE PROCEDURE InsertOrderWithFKCheck(
    IN p_customer_id INT,
    IN p_order_amount DECIMAL(10,2)
)
BEGIN
    DECLARE EXIT HANDLER FOR 1452 -- Foreign key constraint violation
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('1452', 'Invalid customer ID', NOW());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid customer ID';
    END;
    INSERT INTO orders (customer_id, order_amount, order_date)
    VALUES (p_customer_id, p_order_amount, CURDATE());
END //
DELIMITER ;

-- 12. RESIGNAL with modified SQLSTATE: Customize error codes
-- Purpose: Catch and rethrow an error with a different SQLSTATE
DELIMITER //
CREATE PROCEDURE UpdateCustomerEmail(
    IN p_customer_id INT,
    IN p_new_email VARCHAR(255)
)
BEGIN
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        RESIGNAL SET SQLSTATE '45002', MESSAGE_TEXT = 'Error updating customer email';
    END;
    UPDATE customers
    SET email = p_new_email
    WHERE customer_id = p_customer_id;
END //
DELIMITER ;

-- 13. Multiple handlers: Handle different error types
-- Purpose: Differentiate between warnings and errors in a procedure
DELIMITER //
CREATE PROCEDURE BulkInsertOrders(
    IN p_customer_id INT,
    IN p_order_amount DECIMAL(10,2)
)
BEGIN
    DECLARE CONTINUE HANDLER FOR SQLWARNING
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('SQLWARN', 'Warning during bulk insert', NOW());
    END;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        INSERT INTO error_log (error_code, error_message, error_time)
        VALUES ('SQLERR', 'Error during bulk insert', NOW());
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Bulk insert failed';
    END;
    INSERT INTO orders (customer_id, order_amount, order_date)
    VALUES (p_customer_id, p_order_amount, CURDATE());
END //
DELIMITER ;

-- 14.
