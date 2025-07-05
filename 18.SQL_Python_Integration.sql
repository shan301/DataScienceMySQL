# SQL_Python_Integration.py
# This script provides a comprehensive lesson on integrating MySQL queries with Python using pymysql and pandas for data analysts and scientists.
# It includes 15 example scripts with explanations, followed by 15 practice questions with answers, focusing on connecting to MySQL, executing queries, and handling data with pandas.
# Assumes the 'mysql_learning' database and sample datasets (customers, orders, products, sales) are loaded in a MySQL 8.0+ server.
# Requires Python 3.8+, pymysql, and pandas libraries installed.

import pymysql
import pandas as pd
from pymysql.err import OperationalError, ProgrammingError

# Database connection configuration
db_config = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'mysql_learning',
    'charset': 'utf8mb4'
}

# =====================================
# Lesson: MySQL and Python Integration
# =====================================
# The following scripts demonstrate core integration concepts: connecting to MySQL, executing queries, 
# fetching results with pymysql, and manipulating data with pandas.

# 1. Basic connection and query: Fetch all customers
# Purpose: Connect to MySQL and retrieve customer data
try:
    connection = pymysql.connect(**db_config)
    query = "SELECT customer_id, first_name, last_name FROM customers"
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
            print(row)
finally:
    connection.close()

# 2. Pandas DataFrame: Load customers into a DataFrame
# Purpose: Use pandas.read_sql to fetch query results directly into a DataFrame
connection = pymysql.connect(**db_config)
query = "SELECT customer_id, first_name, last_name, city FROM customers"
df_customers = pd.read_sql(query, connection)
connection.close()
print(df_customers.head())

# 3. Parameterized query: Filter customers by city
# Purpose: Use parameterized queries to prevent SQL injection
city = 'New York'
try:
    connection = pymysql.connect(**db_config)
    query = "SELECT customer_id, first_name, last_name FROM customers WHERE city = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (city,))
        results = cursor.fetchall()
        for row in results:
            print(row)
finally:
    connection.close()

# 4. Insert data: Add a new customer
# Purpose: Execute an INSERT query with error handling
try:
    connection = pymysql.connect(**db_config)
    query = """
    INSERT INTO customers (first_name, last_name, email, city)
    VALUES (%s, %s, %s, %s)
    """
    with connection.cursor() as cursor:
        cursor.execute(query, ('John', 'Smith', 'john.smith@example.com', 'Boston'))
        connection.commit()
    print("Customer inserted successfully")
except OperationalError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 5. Update data: Modify customer email
# Purpose: Execute an UPDATE query with parameterized input
try:
    connection = pymysql.connect(**db_config)
    query = "UPDATE customers SET email = %s WHERE customer_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, ('new.email@example.com', 1))
        connection.commit()
    print(f"Updated {cursor.rowcount} rows")
except OperationalError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 6. Delete data: Remove a customer
# Purpose: Execute a DELETE query with error handling
try:
    connection = pymysql.connect(**db_config)
    query = "DELETE FROM customers WHERE customer_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (1,))
        connection.commit()
    print(f"Deleted {cursor.rowcount} rows")
except OperationalError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 7. Join query with pandas: Combine customers and orders
# Purpose: Fetch joined data into a pandas DataFrame
connection = pymysql.connect(**db_config)
query = """
SELECT c.customer_id, c.first_name, c.last_name, COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
"""
df_joined = pd.read_sql(query, connection)
connection.close()
print(df_joined.head())

# 8. Aggregate query: Calculate total sales by city
# Purpose: Use pandas to further process aggregated SQL results
connection = pymysql.connect(**db_config)
query = """
SELECT c.city, SUM(o.order_amount) AS total_sales
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.city
"""
df_sales = pd.read_sql(query, connection)
connection.close()
print(df_sales.sort_values(by='total_sales', ascending=False))

# 9. Batch insert: Insert multiple customers
# Purpose: Use executemany for efficient batch insertion
try:
    connection = pymysql.connect(**db_config)
    query = """
    INSERT INTO customers (first_name, last_name, email, city)
    VALUES (%s, %s, %s, %s)
    """
    new_customers = [
        ('Alice', 'Brown', 'alice.brown@example.com', 'Chicago'),
        ('Bob', 'Wilson', 'bob.wilson@example.com', 'Seattle')
    ]
    with connection.cursor() as cursor:
        cursor.executemany(query, new_customers)
        connection.commit()
    print(f"Inserted {cursor.rowcount} customers")
except OperationalError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 10. Error handling: Catch specific SQL errors
# Purpose: Handle duplicate entry errors (SQLSTATE 1062)
try:
    connection = pymysql.connect(**db_config)
    query = """
    INSERT INTO customers (customer_id, first_name, last_name, email, city)
    VALUES (%s, %s, %s, %s, %s)
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (1, 'Duplicate', 'User', 'duplicate@example.com', 'Miami'))
        connection.commit()
except pymysql.err.IntegrityError as e:
    if e.args[0] == 1062:
        print("Error: Duplicate customer ID")
    else:
        print(f"Error: {e}")
finally:
    connection.close()

# 11. Stored procedure call: Execute a stored procedure
# Purpose: Call a stored procedure with parameters
try:
    connection = pymysql.connect(**db_config)
    with connection.cursor() as cursor:
        cursor.callproc('InsertOrderWithValidation', (1, 100.00, 1))
        connection.commit()
    print("Stored procedure executed successfully")
except ProgrammingError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 12. Pandas to MySQL: Write DataFrame to a new table
# Purpose: Use pandas to_sql to create and populate a table
connection = pymysql.connect(**db_config)
df_new = pd.DataFrame({
    'product_id': [101, 102],
    'product_name': ['Laptop', 'Phone'],
    'price': [999.99, 499.99]
})
df_new.to_sql('new_products', connection, if_exists='replace', index=False)
connection.close()
print("DataFrame written to MySQL table")

# 13. Dynamic query: Build and execute a dynamic SQL query
# Purpose: Construct a query based on user input
city_filter = 'Boston'
try:
    connection = pymysql.connect(**db_config)
    query = f"SELECT customer_id, first_name, last_name FROM customers WHERE city = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (city_filter,))
        results = cursor.fetchall()
        for row in results:
            print(row)
except OperationalError as e:
    print(f"Error: {e}")
finally:
    connection.close()

# 14. Transaction management: Commit or rollback multiple operations
# Purpose: Ensure atomicity for multiple SQL statements
try:
    connection = pymysql.connect(**db_config)
    connection.begin()
    with connection.cursor() as cursor:
        cursor.execute("INSERT INTO customers (first_name, last_name, email, city) VALUES (%s, %s, %s, %s)", 
                      ('Test', 'User', 'test@example.com', 'Denver'))
        cursor.execute("INSERT INTO orders (customer_id, order_amount, order_date) VALUES (%s, %s, %s)", 
                      (9999, 50.00, '2025-07-01'))  # Invalid customer_id
        connection.commit()
except pymysql.err.IntegrityError:
    connection.rollback()
    print("Transaction rolled back due to error")
finally:
    connection.close()

# 15. Data analysis with pandas: Compute statistics on query results
# Purpose: Use pandas to analyze sales data after fetching from MySQL
connection = pymysql.connect(**db_config)
query = """
SELECT c.city, o.order_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
"""
df_sales = pd.read_sql(query, connection)
connection.close()
stats = df_sales.groupby('city')['order_amount'].agg(['mean', 'std', 'count'])
print(stats)

# Lesson Notes:
# - pymysql provides low-level MySQL connectivity; pandas simplifies data manipulation.
# - Always use parameterized queries to prevent SQL injection.
# - Handle exceptions (OperationalError, IntegrityError, ProgrammingError) for robust scripts.
# - Use pandas.read_sql for efficient DataFrame loading; to_sql for writing data.
# - Ensure transactions are used for multiple operations to maintain data integrity.
# - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, products.csv).
# - Install pymysql (`pip install pymysql`) and pandas (`pip install pandas`) before running scripts.

# =====================================
# Practice Questions and Answers
# =====================================
