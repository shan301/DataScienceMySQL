-- Triggers.sql
-- This script provides a comprehensive lesson on MySQL triggers for data analysts and scientists.
-- It includes 15 example triggers with explanations, followed by 15 practice questions with answers.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, order_log) are loaded.

-- Use the mysql_learning database
USE mysql_learning;

-- Create a log table for tracking changes (used by several triggers)
CREATE TABLE IF NOT EXISTS order_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT,
    action VARCHAR(50),
    old_amount DECIMAL(10,2),
    new_amount DECIMAL(10,2),
    change_date DATETIME
);

-- =====================================
-- Lesson: MySQL Triggers
-- =====================================
-- The following triggers demonstrate core concepts: BEFORE/AFTER triggers, INSERT/UPDATE/DELETE triggers, 
-- logging changes, enforcing rules, and cascading updates for automation.

-- 1. BEFORE INSERT trigger: Validate order amount
-- Purpose: Ensure order_amount is positive before insertion
DELIMITER //
CREATE TRIGGER before_order_insert
BEFORE INSERT ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_amount <= 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Order amount must be positive';
    END IF;
END //
DELIMITER ;

-- 2. AFTER INSERT trigger: Log new orders
-- Purpose: Record new orders in the order_log table
DELIMITER //
CREATE TRIGGER after_order_insert
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_log (order_id, action, new_amount, change_date)
    VALUES (NEW.order_id, 'INSERT', NEW.order_amount, NOW());
END //
DELIMITER ;

-- 3. BEFORE UPDATE trigger: Prevent negative order amounts
-- Purpose: Block updates that would set order_amount to a negative value
DELIMITER //
CREATE TRIGGER before_order_update
BEFORE UPDATE ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_amount < 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Order amount cannot be negative';
    END IF;
END //
DELIMITER ;

-- 4. AFTER UPDATE trigger: Log order amount changes
-- Purpose: Record changes to order_amount in the order_log table
DELIMITER //
CREATE TRIGGER after_order_update
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
    IF OLD.order_amount != NEW.order_amount THEN
        INSERT INTO order_log (order_id, action, old_amount, new_amount, change_date)
        VALUES (OLD.order_id, 'UPDATE', OLD.order_amount, NEW.order_amount, NOW());
    END IF;
END //
DELIMITER ;

-- 5. AFTER DELETE trigger: Log deleted orders
-- Purpose: Record deleted orders in the order_log table
DELIMITER //
CREATE TRIGGER after_order_delete
AFTER DELETE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_log (order_id, action, old_amount, change_date)
    VALUES (OLD.order_id, 'DELETE', OLD.order_amount, NOW());
END //
DELIMITER ;

-- 6. BEFORE INSERT trigger: Standardize customer email
-- Purpose: Convert email to lowercase before insertion
DELIMITER //
CREATE TRIGGER before_customer_insert
BEFORE INSERT ON customers
FOR EACH ROW
BEGIN
    SET NEW.email = LOWER(NEW.email);
END //
DELIMITER ;

-- 7. BEFORE UPDATE trigger: Prevent email changes to invalid formats
-- Purpose: Ensure updated emails contain '@' and '.'
DELIMITER //
CREATE TRIGGER before_customer_email_update
BEFORE UPDATE ON customers
FOR EACH ROW
BEGIN
    IF NEW.email IS NOT NULL AND NEW.email NOT LIKE '%@%.%' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid email format';
    END IF;
END //
DELIMITER ;

-- 8. AFTER INSERT trigger: Update customer count in a summary table
-- Purpose: Maintain a count of customers per city in a summary table
CREATE TABLE IF NOT EXISTS city_summary (
    city VARCHAR(100) PRIMARY KEY,
    customer_count INT
);
DELIMITER //
CREATE TRIGGER after_customer_insert
AFTER INSERT ON customers
FOR EACH ROW
BEGIN
    INSERT INTO city_summary (city, customer_count)
    VALUES (NEW.city, 1)
    ON DUPLICATE KEY UPDATE customer_count = customer_count + 1;
END //
DELIMITER ;

-- 9. AFTER DELETE trigger: Update customer count in summary table
-- Purpose: Decrease customer count when a customer is deleted
DELIMITER //
CREATE TRIGGER after_customer_delete
AFTER DELETE ON customers
FOR EACH ROW
BEGIN
    UPDATE city_summary
    SET customer_count = customer_count - 1
    WHERE city = OLD.city;
END //
DELIMITER ;

-- 10. BEFORE UPDATE trigger: Restrict product price changes
-- Purpose: Prevent price changes exceeding 50% of the original price
DELIMITER //
CREATE TRIGGER before_product_update
BEFORE UPDATE ON products
FOR EACH ROW
BEGIN
    IF NEW.price > OLD.price * 1.5 OR NEW.price < OLD.price * 0.5 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Price change exceeds 50%';
    END IF;
END //
DELIMITER ;

-- 11. AFTER INSERT trigger: Log product additions
-- Purpose: Record new products in a product_log table
CREATE TABLE IF NOT EXISTS product_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT,
    action VARCHAR(50),
    product_name VARCHAR(255),
    change_date DATETIME
);
DELIMITER //
CREATE TRIGGER after_product_insert
AFTER INSERT ON products
FOR EACH ROW
BEGIN
    INSERT INTO product_log (product_id, action, product_name, change_date)
    VALUES (NEW.product_id, 'INSERT', NEW.product_name, NOW());
END //
DELIMITER ;

-- 12. BEFORE INSERT trigger: Set default order date
-- Purpose: Set order_date to current date if NULL
DELIMITER //
CREATE TRIGGER before_order_date_insert
BEFORE INSERT ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_date IS NULL THEN
        SET NEW.order_date = CURDATE();
    END IF;
END //
DELIMITER ;

-- 13. AFTER UPDATE trigger: Track customer city changes
-- Purpose: Log changes to customer city in a log table
CREATE TABLE IF NOT EXISTS customer_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    action VARCHAR(50),
    old_city VARCHAR(100),
    new_city VARCHAR(100),
    change_date DATETIME
);
DELIMITER //
CREATE TRIGGER after_customer_city_update
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    IF OLD.city != NEW.city THEN
        INSERT INTO customer_log (customer_id, action, old_city, new_city, change_date)
        VALUES (OLD.customer_id, 'UPDATE', OLD.city, NEW.city, NOW());
    END IF;
END //
DELIMITER ;

-- 14. BEFORE DELETE trigger: Prevent deletion of high-value orders
-- Purpose: Block deletion of orders with amounts above 1000
DELIMITER //
CREATE TRIGGER before_order_delete
BEFORE DELETE ON orders
FOR EACH ROW
BEGIN
    IF OLD.order_amount > 1000 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Cannot delete high-value orders';
    END IF;
END //
DELIMITER ;

-- 15. AFTER INSERT trigger: Update product stock after order
-- Purpose: Reduce product stock when a new order is placed
ALTER TABLE products ADD stock INT DEFAULT 100;
DELIMITER //
CREATE TRIGGER after_order_stock_update
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    UPDATE products
    SET stock = stock - 1
    WHERE product_id = NEW.product_id
    AND stock > 0;
END //
DELIMITER ;

-- Lesson Notes:
-- - Triggers automate database actions but can impact performance; use sparingly for critical tasks.
-- - BEFORE triggers modify data or validate before changes; AFTER triggers log or propagate changes.
-- - Use SIGNAL for custom error messages to enforce rules.
-- - Ensure log tables (e.g., order_log, product_log, customer_log) are created before triggers.
-- - Test triggers with small datasets to verify behavior.
-- - Sample datasets (customers.csv, orders.csv, products.csv) must be loaded.

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of trigger concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you create a trigger to ensure order amounts are positive before insertion?
-- Explanation: A BEFORE INSERT trigger validates NEW.order_amount, raising an error if invalid.
-- Answer:
DELIMITER //
CREATE TRIGGER before_order_insert
BEFORE INSERT ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_amount <= 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Order amount must be positive';
    END IF;
END //
DELIMITER ;

-- Question 2: How can you log new orders in a log table after insertion?
-- Explanation: An AFTER INSERT trigger inserts a record into order_log with the new order details.
-- Answer:
DELIMITER //
CREATE TRIGGER after_order_insert
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_log (order_id, action, new_amount, change_date)
    VALUES (NEW.order_id, 'INSERT', NEW.order_amount, NOW());
END //
DELIMITER ;

-- Question 3: How do you prevent negative order amounts during updates?
-- Explanation: A BEFORE UPDATE trigger checks NEW.order_amount and raises an error if negative.
-- Answer:
DELIMITER //
CREATE TRIGGER before_order_update
BEFORE UPDATE ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_amount < 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Order amount cannot be negative';
    END IF;
END //
DELIMITER ;

-- Question 4: How can you log changes to order amounts after updates?
-- Explanation: An AFTER UPDATE trigger logs changes to order_log if the amount changes.
-- Answer:
DELIMITER //
CREATE TRIGGER after_order_update
AFTER UPDATE ON orders
FOR EACH ROW
BEGIN
    IF OLD.order_amount != NEW.order_amount THEN
        INSERT INTO order_log (order_id, action, old_amount, new_amount, change_date)
        VALUES (OLD.order_id, 'UPDATE', OLD.order_amount, NEW.order_amount, NOW());
    END IF;
END //
DELIMITER ;

-- Question 5: How do you log deleted orders in a log table?
-- Explanation: An AFTER DELETE trigger records the deleted order’s details in order_log.
-- Answer:
DELIMITER //
CREATE TRIGGER after_order_delete
AFTER DELETE ON orders
FOR EACH ROW
BEGIN
    INSERT INTO order_log (order_id, action, old_amount, change_date)
    VALUES (OLD.order_id, 'DELETE', OLD.order_amount, NOW());
END //
DELIMITER ;

-- Question 6: How can you standardize customer emails to lowercase before insertion?
-- Explanation: A BEFORE INSERT trigger modifies NEW.email to ensure consistency.
-- Answer:
DELIMITER //
CREATE TRIGGER before_customer_insert
BEFORE INSERT ON customers
FOR EACH ROW
BEGIN
    SET NEW.email = LOWER(NEW.email);
END //
DELIMITER ;

-- Question 7: How do you ensure updated emails are valid?
-- Explanation: A BEFORE UPDATE trigger validates NEW.email format, raising an error if invalid.
-- Answer:
DELIMITER //
CREATE TRIGGER before_customer_email_update
BEFORE UPDATE ON customers
FOR EACH ROW
BEGIN
    IF NEW.email IS NOT NULL AND NEW.email NOT LIKE '%@%.%' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid email format';
    END IF;
END //
DELIMITER ;

-- Question 8: How can you maintain a customer count per city after insertions?
-- Explanation: An AFTER INSERT trigger updates a summary table using ON DUPLICATE KEY UPDATE.
-- Answer:
DELIMITER //
CREATE TRIGGER after_customer_insert
AFTER INSERT ON customers
FOR EACH ROW
BEGIN
    INSERT INTO city_summary (city, customer_count)
    VALUES (NEW.city, 1)
    ON DUPLICATE KEY UPDATE customer_count = customer_count + 1;
END //
DELIMITER ;

-- Question 9: How do you update customer counts after deletions?
-- Explanation: An AFTER DELETE trigger decrements the customer count in the summary table.
-- Answer:
DELIMITER //
CREATE TRIGGER after_customer_delete
AFTER DELETE ON customers
FOR EACH ROW
BEGIN
    UPDATE city_summary
    SET customer_count = customer_count - 1
    WHERE city = OLD.city;
END //
DELIMITER ;

-- Question 10: How can you restrict product price changes to within 50% of the original?
-- Explanation: A BEFORE UPDATE trigger compares NEW.price to OLD.price and enforces limits.
-- Answer:
DELIMITER //
CREATE TRIGGER before_product_update
BEFORE UPDATE ON products
FOR EACH ROW
BEGIN
    IF NEW.price > OLD.price * 1.5 OR NEW.price < OLD.price * 0.5 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Price change exceeds 50%';
    END IF;
END //
DELIMITER ;

-- Question 11: How do you log new products after insertion?
-- Explanation: An AFTER INSERT trigger records new products in a product_log table.
-- Answer:
DELIMITER //
CREATE TRIGGER after_product_insert
AFTER INSERT ON products
FOR EACH ROW
BEGIN
    INSERT INTO product_log (product_id, action, product_name, change_date)
    VALUES (NEW.product_id, 'INSERT', NEW.product_name, NOW());
END //
DELIMITER ;

-- Question 12: How can you set a default order date if NULL during insertion?
-- Explanation: A BEFORE INSERT trigger sets NEW.order_date to CURDATE() if NULL.
-- Answer:
DELIMITER //
CREATE TRIGGER before_order_date_insert
BEFORE INSERT ON orders
FOR EACH ROW
BEGIN
    IF NEW.order_date IS NULL THEN
        SET NEW.order_date = CURDATE();
    END IF;
END //
DELIMITER ;

-- Question 13: How do you log changes to customer city after updates?
-- Explanation: An AFTER UPDATE trigger logs city changes to a customer_log table.
-- Answer:
DELIMITER //
CREATE TRIGGER after_customer_city_update
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    IF OLD.city != NEW.city THEN
        INSERT INTO customer_log (customer_id, action, old_city, new_city, change_date)
        VALUES (OLD.customer_id, 'UPDATE', OLD.city, NEW.city, NOW());
    END IF;
END //
DELIMITER ;

-- Question 14: How can you prevent deletion of high-value orders?
-- Explanation: A BEFORE DELETE trigger blocks deletion of orders above a threshold.
-- Answer:
DELIMITER //
CREATE TRIGGER before_order_delete
BEFORE DELETE ON orders
FOR EACH ROW
BEGIN
    IF OLD.order_amount > 1000 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Cannot delete high-value orders';
    END IF;
END //
DELIMITER ;

-- Question 15: How do you update product stock after a new order?
-- Explanation: An AFTER INSERT trigger reduces stock for the ordered product.
-- Answer:
DELIMITER //
CREATE TRIGGER after_order_stock_update
AFTER INSERT ON orders
FOR EACH ROW
BEGIN
    UPDATE products
    SET stock = stock - 1
    WHERE product_id = NEW.product_id
    AND stock > 0;
END //
DELIMITER ;

-- =====================================
-- Final Notes
-- =====================================
-- - Triggers should be used judiciously to avoid performance issues and complexity.
-- - Always test triggers with sample data to verify behavior and avoid infinite loops.
-- - Use DROP TRIGGER trigger_name to remove triggers if needed.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv) and log tables.
-- - For further learning, explore `Stored_Procedures.sql` for reusable logic or `Index_Optimization.sql` for performance tuning.
