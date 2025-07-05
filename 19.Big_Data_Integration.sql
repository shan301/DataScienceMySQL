-- Big_Data_Integration.sql
-- This script provides a comprehensive lesson on integrating MySQL with big data tools (Hadoop, Spark SQL) for data analysts and scientists.
-- It includes 15 example queries and code snippets with explanations, followed by 15 practice questions with answers, focusing on SQL queries for data transfer, preparation, and analysis in big data ecosystems.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded in MySQL 8.0+.
-- Requires Python 3.8+, pymysql, pandas, pyarrow, Hadoop 3.3+, Spark 3.4+, and MySQL Connector/J.

-- Use the mysql_learning database
USE mysql_learning;

-- Create a table for aggregated results from big data tools
CREATE TABLE IF NOT EXISTS customer_totals (
    customer_id INT PRIMARY KEY,
    total_spent DECIMAL(10,2),
    order_count INT
);

-- =====================================
-- Lesson: MySQL with Big Data Tools
-- =====================================
-- The following examples demonstrate SQL queries and Python code for integrating MySQL with Hadoop (Sqoop) and Spark SQL, focusing on data transfer, querying, and aggregation.

-- Example 1: Prepare MySQL table for Sqoop export
-- Purpose: Ensure the customers table is optimized for export to HDFS
-- Note: Run Sqoop export in terminal:
-- sqoop export --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table customers --export-dir /user/hadoop/customers --input-fields-terminated-by ','
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    city
FROM customers;

-- Example 2: Create table for Sqoop import
-- Purpose: Define a table to receive HDFS data via Sqoop
CREATE TABLE new_sales (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    sale_amount DECIMAL(10,2),
    sale_date DATE
);
-- Note: Run Sqoop import:
-- sqoop import --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table new_sales --target-dir /user/hadoop/sales_data --fields-terminated-by ','

-- Example 3: Prepare data for Hive table creation
-- Purpose: Select orders data for Sqoop to create a Hive table
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders;
-- Note: Run Sqoop with Hive import:
-- sqoop import --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table orders --hive-import --hive-table mysql_learning.orders --fields-terminated-by ','

-- Example 4: Query for Spark SQL
-- Purpose: Fetch customers for Spark SQL processing
SELECT 
    customer_id,
    first_name,
    last_name,
    city
FROM customers
WHERE city = 'New York';
-- Python code for Spark SQL:
-- from pyspark.sql import SparkSession
-- spark = SparkSession.builder.appName("MySQL_Spark").config("spark.jars", "mysql-connector-j-8.0.33.jar").getOrCreate()
-- url = "jdbc:mysql://localhost:3306/mysql_learning"
-- properties = {"user": "your_username", "password": "your_password", "driver": "com.mysql.cj.jdbc.Driver"}
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_customers.createOrReplaceTempView("customers")
-- result = spark.sql("SELECT customer_id, first_name, last_name FROM customers WHERE city = 'New York'")
-- result.show()
-- spark.stop()

-- Example 5: Aggregate data for Spark write-back
-- Purpose: Prepare aggregated data for Spark to write to customer_totals
SELECT 
    customer_id,
    SUM(order_amount) AS total_spent,
    COUNT(order_id) AS order_count
FROM orders
GROUP BY customer_id;
-- Python code to write to MySQL:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_agg = df_orders.groupBy("customer_id").sum("order_amount").withColumnRenamed("sum(order_amount)", "total_spent")
-- df_agg.write.jdbc(url, "customer_totals", mode="overwrite", properties=properties)

-- Example 6: Join tables for Spark SQL
-- Purpose: Join customers and orders for Spark processing
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;
-- Python code:
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_customers.createOrReplaceTempView("customers")
-- df_orders.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT c.customer_id, c.first_name, c.last_name, COUNT(o.order_id) AS order_count FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name, c.last_name")
-- result.show()

-- Example 7: Prepare data for pandas via Spark
-- Purpose: Select orders for Spark-to-pandas conversion
SELECT 
    customer_id,
    order_amount
FROM orders;
-- Python code:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- pandas_df = df_orders.toPandas()
-- stats = pandas_df.groupby('customer_id')['order_amount'].mean()
-- print(stats)

-- Example 8: Partitioned query for Spark
-- Purpose: Optimize large table read with partitioning
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders
WHERE order_id BETWEEN 1 AND 1000000;
-- Python code:
-- df_orders = spark.read.jdbc(url=url, table="orders", column="order_id", lowerBound=1, upperBound=1000000, numPartitions=4, properties=properties)
-- df_orders.show()

-- Example 9: Incremental load query
-- Purpose: Select recent orders for incremental Spark load
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders
WHERE order_date >= '2025-01-01';
-- Python code:
-- query = "(SELECT * FROM orders WHERE order_date >= '2025-01-01') AS recent_orders"
-- df_recent = spark.read.jdbc(url, query, properties=properties)
-- df_recent.show()

-- Example 10: Prepare data for HDFS export
-- Purpose: Select products for Spark to write to HDFS
SELECT 
    product_id,
    product_name,
    price
FROM products;
-- Python code:
-- df_products = spark.read.jdbc(url, "products", properties=properties)
-- df_products.write.csv("hdfs://localhost:9000/user/hadoop/products", mode="overwrite")

-- Example 11: Query for Hive metastore
-- Purpose: Ensure orders table is accessible for Hive
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders;
-- Python code with Hive support:
-- spark = SparkSession.builder.appName("MySQL_Hive").config("spark.jars", "mysql-connector-j-8.0.33.jar").enableHiveSupport().getOrCreate()
-- spark.sql("SELECT * FROM mysql_learning.orders").show()

-- Example 12: Aggregate sales for Spark SQL
-- Purpose: Compute total sales by customer
SELECT 
    customer_id,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY customer_id;
-- Python code:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_orders.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT customer_id, SUM(order_amount) AS total_sales FROM orders GROUP BY customer_id")
-- result.show()

-- Example 13: Test query with error handling
-- Purpose: Select customers with invalid credentials for error handling
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers;
-- Python code with error handling:
-- try:
--     spark = SparkSession.builder.appName("MySQL_Spark").config("spark.jars", "mysql-connector-j-8.0.33.jar").getOrCreate()
--     properties = {"user": "wrong_user", "password": "wrong_password", "driver": "com.mysql.cj.jdbc.Driver"}
--     df = spark.read.jdbc(url, "customers", properties=properties)
--     df.show()
-- except Exception as e:
--     print(f"Error: {e}")
-- finally:
--     spark.stop()

-- Example 14: Join MySQL and HDFS data
-- Purpose: Prepare customers for joining with HDFS orders
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers;
-- Python code:
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_orders_hdfs = spark.read.csv("hdfs://localhost:9000/user/hadoop/orders")
-- df_customers.createOrReplaceTempView("customers")
-- df_orders_hdfs.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT c.customer_id, c.first_name, COUNT(o.order_id) AS order_count FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name")
-- result.show()

-- Example 15: Optimize query for Spark
-- Purpose: Select orders with indexing for efficient Spark read
SELECT 
    order_id,
    customer_id,
    order_amount
FROM orders
WHERE order_id BETWEEN 1 AND 1000000;
-- Ensure index exists:
CREATE INDEX idx_order_id ON orders(order_id);
-- Python code:
-- df_orders = spark.read.jdbc(url=url, table="orders", column="order_id", lowerBound=1, upperBound=1000000, numPartitions=4, properties=properties)
-- df_orders.cache()
-- df_orders.groupBy("customer_id").sum("order_amount").show()

-- Lesson Notes:
-- - Sqoop facilitates bulk data transfers between MySQL and Hadoop HDFS/Hive.
-- - Spark SQL enables scalable querying of MySQL data with distributed processing.
-- - Use partitioning and indexing to optimize large dataset reads in Spark.
-- - Ensure MySQL Connector/J is included for Sqoop and Spark JDBC connectivity.
-- - Validate SQL queries in MySQL before running in big data tools.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv, sales.csv).

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of MySQL and big data integration. Each question includes an explanation and the SQL or Python answer.

-- Question 1: How do you prepare a MySQL table for Sqoop export to HDFS?
-- Explanation: Select relevant columns for Sqoop to export to HDFS.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    city
FROM customers;
-- Note: Run Sqoop export:
-- sqoop export --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table customers --export-dir /user/hadoop/customers --input-fields-terminated-by ','

-- Question 2: How can you create a table for Sqoop import from HDFS?
-- Explanation: Define a table to receive HDFS data via Sqoop.
-- Answer:
CREATE TABLE new_sales (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    sale_amount DECIMAL(10,2),
    sale_date DATE
);
-- Note: Run Sqoop import:
-- sqoop import --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table new_sales --target-dir /user/hadoop/sales_data --fields-terminated-by ','

-- Question 3: How do you prepare orders data for a Hive table?
-- Explanation: Select orders for Sqoop to create a Hive table.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders;
-- Note: Run Sqoop:
-- sqoop import --connect jdbc:mysql://localhost:3306/mysql_learning --username your_username --password your_password --table orders --hive-import --hive-table mysql_learning.orders --fields-terminated-by ','

-- Question 4: How can you query customers in Spark SQL?
-- Explanation: Select customers for Spark SQL processing.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name,
    city
FROM customers
WHERE city = 'New York';
-- Python code:
-- from pyspark.sql import SparkSession
-- spark = SparkSession.builder.appName("MySQL_Spark").config("spark.jars", "mysql-connector-j-8.0.33.jar").getOrCreate()
-- url = "jdbc:mysql://localhost:3306/mysql_learning"
-- properties = {"user": "your_username", "password": "your_password", "driver": "com.mysql.cj.jdbc.Driver"}
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_customers.createOrReplaceTempView("customers")
-- result = spark.sql("SELECT customer_id, first_name, last_name FROM customers WHERE city = 'New York'")
-- result.show()
-- spark.stop()

-- Question 5: How do you aggregate data for Spark to write to MySQL?
-- Explanation: Prepare aggregated data for Spark write-back.
-- Answer:
SELECT 
    customer_id,
    SUM(order_amount) AS total_spent,
    COUNT(order_id) AS order_count
FROM orders
GROUP BY customer_id;
-- Python code:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_agg = df_orders.groupBy("customer_id").sum("order_amount").withColumnRenamed("sum(order_amount)", "total_spent")
-- df_agg.write.jdbc(url, "customer_totals", mode="overwrite", properties=properties)

-- Question 6: How can you join customers and orders for Spark SQL?
-- Explanation: Perform a join for Spark processing.
-- Answer:
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;
-- Python code:
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_customers.createOrReplaceTempView("customers")
-- df_orders.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT c.customer_id, c.first_name, c.last_name, COUNT(o.order_id) AS order_count FROM customers c LEFT JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name, c.last_name")
-- result.show()

-- Question 7: How do you prepare orders for Spark-to-pandas conversion?
-- Explanation: Select data for Spark to load into pandas.
-- Answer:
SELECT 
    customer_id,
    order_amount
FROM orders;
-- Python code:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- pandas_df = df_orders.toPandas()
-- stats = pandas_df.groupby('customer_id')['order_amount'].mean()
-- print(stats)

-- Question 8: How can you optimize a large table read for Spark?
-- Explanation: Use partitioning with a range filter.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders
WHERE order_id BETWEEN 1 AND 1000000;
-- Python code:
-- df_orders = spark.read.jdbc(url=url, table="orders", column="order_id", lowerBound=1, upperBound=1000000, numPartitions=4, properties=properties)
-- df_orders.show()

-- Question 9: How do you select recent orders for incremental load?
-- Explanation: Filter orders by date for incremental processing.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders
WHERE order_date >= '2025-01-01';
-- Python code:
-- query = "(SELECT * FROM orders WHERE order_date >= '2025-01-01') AS recent_orders"
-- df_recent = spark.read.jdbc(url, query, properties=properties)
-- df_recent.show()

-- Question 10: How can you prepare products for HDFS export?
-- Explanation: Select products for Spark to write to HDFS.
-- Answer:
SELECT 
    product_id,
    product_name,
    price
FROM products;
-- Python code:
-- df_products = spark.read.jdbc(url, "products", properties=properties)
-- df_products.write.csv("hdfs://localhost:9000/user/hadoop/products", mode="overwrite")

-- Question 11: How do you ensure orders are accessible for Hive?
-- Explanation: Select orders for Hive table integration.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount,
    order_date
FROM orders;
-- Python code:
-- spark = SparkSession.builder.appName("MySQL_Hive").config("spark.jars", "mysql-connector-j-8.0.33.jar").enableHiveSupport().getOrCreate()
-- spark.sql("SELECT * FROM mysql_learning.orders").show()

-- Question 12: How can you aggregate sales by customer for Spark SQL?
-- Explanation: Compute total sales per customer.
-- Answer:
SELECT 
    customer_id,
    SUM(order_amount) AS total_sales
FROM orders
GROUP BY customer_id;
-- Python code:
-- df_orders = spark.read.jdbc(url, "orders", properties=properties)
-- df_orders.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT customer_id, SUM(order_amount) AS total_sales FROM orders GROUP BY customer_id")
-- result.show()

-- Question 13: How do you test a query with invalid credentials?
-- Explanation: Select customers to test error handling in Spark.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers;
-- Python code:
-- try:
--     spark = SparkSession.builder.appName("MySQL_Spark").config("spark.jars", "mysql-connector-j-8.0.33.jar").getOrCreate()
--     properties = {"user": "wrong_user", "password": "wrong_password", "driver": "com.mysql.cj.jdbc.Driver"}
--     df = spark.read.jdbc(url, "customers", properties=properties)
--     df.show()
-- except Exception as e:
--     print(f"Error: {e}")
-- finally:
--     spark.stop()

-- Question 14: How can you prepare customers for joining with HDFS data?
-- Explanation: Select customers for Spark join with HDFS orders.
-- Answer:
SELECT 
    customer_id,
    first_name,
    last_name
FROM customers;
-- Python code:
-- df_customers = spark.read.jdbc(url, "customers", properties=properties)
-- df_orders_hdfs = spark.read.csv("hdfs://localhost:9000/user/hadoop/orders")
-- df_customers.createOrReplaceTempView("customers")
-- df_orders_hdfs.createOrReplaceTempView("orders")
-- result = spark.sql("SELECT c.customer_id, c.first_name, COUNT(o.order_id) AS order_count FROM customers c JOIN orders o ON c.customer_id = o.customer_id GROUP BY c.customer_id, c.first_name")
-- result.show()

-- Question 15: How do you optimize a query for Spark performance?
-- Explanation: Use indexing and partitioning for efficient reads.
-- Answer:
SELECT 
    order_id,
    customer_id,
    order_amount
FROM orders
WHERE order_id BETWEEN 1 AND 1000000;
CREATE INDEX idx_order_id ON orders(order_id);
-- Python code:
-- df_orders = spark.read.jdbc(url=url, table="orders", column="order_id", lowerBound=1, upperBound=1000000, numPartitions=4, properties=properties)
-- df_orders.cache()
-- df_orders.groupBy("customer_id").sum("order_amount").show()

-- =====================================
-- Final Notes
-- =====================================
-- - Use Sqoop for bulk data transfers between MySQL and Hadoop HDFS/Hive.
-- - Spark SQL enables distributed querying of MySQL data; optimize with partitioning and caching.
-- - Create indexes on columns like order_id for faster Spark reads.
-- - Ensure MySQL Connector/J is included in Spark and Sqoop configurations.
-- - Test queries in MySQL before integrating with big data tools.
-- - For further learning, explore `SQL_Python_Integration.sql` for Python-MySQL basics or `Statistical_Analysis.sql` for advanced analytics.
