-- Spatial_Features.sql
-- This script provides a comprehensive lesson on MySQL spatial data types and functions for data analysts and scientists.
-- It includes 15 example queries with explanations, followed by 15 practice questions with answers, focusing on spatial data types (POINT, GEOMETRY), spatial functions (ST_Distance_Sphere, ST_Within), and spatial indexes.
-- Assumes the 'mysql_learning' database and sample datasets (customers, orders, stores, customer_locations) are loaded.
-- Requires MySQL 8.0+ with InnoDB for full spatial functionality.

-- Use the mysql_learning database
USE mysql_learning;

-- Create a table with a spatial column for customer locations
CREATE TABLE IF NOT EXISTS customer_locations (
    customer_id INT PRIMARY KEY,
    location POINT NOT NULL,
    SPATIAL INDEX idx_location (location)
);

-- Populate sample spatial data (latitude, longitude as POINT)
INSERT INTO customer_locations (customer_id, location)
VALUES 
    (1, ST_GeomFromText('POINT(-74.0060 40.7128)')), -- New York
    (2, ST_GeomFromText('POINT(-118.2437 34.0522)')), -- Los Angeles
    (3, ST_GeomFromText('POINT(-87.6298 41.8781)')); -- Chicago

-- Create a table for store locations
CREATE TABLE IF NOT EXISTS stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100),
    location POINT NOT NULL,
    SPATIAL INDEX idx_store_location (location)
);

-- Populate sample store data
INSERT INTO stores (store_id, store_name, location)
VALUES 
    (1, 'NY Store', ST_GeomFromText('POINT(-74.0050 40.7100)')),
    (2, 'LA Store', ST_GeomFromText('POINT(-118.2400 34.0500)')),
    (3, 'Chicago Store', ST_GeomFromText('POINT(-87.6300 41.8800)'));

-- =====================================
-- Lesson: MySQL Spatial Data Types and Functions
-- =====================================
-- The following queries demonstrate core spatial concepts: creating POINT and GEOMETRY data, 
-- using spatial functions (ST_Distance_Sphere, ST_Within, ST_Contains), and optimizing with spatial indexes.

-- 1. Create POINT data: Insert a customer location
-- Purpose: Store a geographic point using ST_GeomFromText
INSERT INTO customer_locations (customer_id, location)
VALUES (4, ST_GeomFromText('POINT(-122.3321 47.6062)')); -- Seattle

-- 2. Extract coordinates: Retrieve latitude and longitude
-- Purpose: Use ST_X and ST_Y to extract coordinates from a POINT
SELECT 
    customer_id,
    ST_X(location) AS longitude,
    ST_Y(location) AS latitude
FROM customer_locations;

-- 3. Calculate distance: Find distance between customers and a store
-- Purpose: Use ST_Distance_Sphere for spherical distance in meters
SELECT 
    c.customer_id,
    s.store_name,
    ST_Distance_Sphere(c.location, s.location) AS distance_meters
FROM customer_locations c
CROSS JOIN stores s
WHERE s.store_id = 1; -- NY Store

-- 4. Find nearby customers: Customers within 10km of a store
-- Purpose: Filter customers using ST_Distance_Sphere
SELECT 
    c.customer_id,
    ST_X(c.location) AS longitude,
    ST_Y(c.location) AS latitude
FROM customer_locations c
WHERE ST_Distance_Sphere(c.location, 
    (SELECT location FROM stores WHERE store_id = 1)) <= 10000; -- 10km

-- 5. Spatial containment: Customers within a polygon
-- Purpose: Use ST_Within to check if a POINT is inside a GEOMETRY polygon
SET @poly = ST_GeomFromText('POLYGON((-74.1 40.6, -73.9 40.6, -73.9 40.8, -74.1 40.8, -74.1 40.6))');
SELECT 
    customer_id,
    ST_X(location) AS longitude,
    ST_Y(location) AS latitude
FROM customer_locations
WHERE ST_Within(location, @poly);

-- 6. Spatial index usage: Analyze query with EXPLAIN
-- Purpose: Verify that the spatial index is used for a query
EXPLAIN SELECT 
    customer_id
FROM customer_locations
WHERE ST_Distance_Sphere(location, ST_GeomFromText('POINT(-74.0050 40.7100)')) <= 10000;

-- 7. Create GEOMETRY column: Store various spatial types
-- Purpose: Use GEOMETRY for flexible storage of points, lines, or polygons
CREATE TABLE IF NOT EXISTS areas (
    area_id INT PRIMARY KEY,
    area_name VARCHAR(100),
    boundary GEOMETRY NOT NULL,
    SPATIAL INDEX idx_boundary (boundary)
);

-- 8. Insert polygon: Store a geographic area
-- Purpose: Insert a POLYGON to represent a region
INSERT INTO areas (area_id, area_name, boundary)
VALUES (1, 'Downtown NY', ST_GeomFromText('POLYGON((-74.01 40.70, -74.00 40.70, -74.00 40.72, -74.01 40.72, -74.01 40.70))'));

-- 9. Spatial join: Find customers in a specific area
-- Purpose: Use ST_Contains to join customers with areas
SELECT 
    c.customer_id,
    a.area_name
FROM customer_locations c
JOIN areas a
WHERE ST_Contains(a.boundary, c.location);

-- 10. Buffer around point: Create a buffer zone around a store
-- Purpose: Use ST_Buffer to create a circular region (approximated)
SET @store_point = (SELECT location FROM stores WHERE store_id = 1);
SELECT 
    ST_AsText(ST_Buffer(@store_point, 0.01)) AS buffer_zone -- Approx 1km buffer
FROM DUAL;

-- 11. Validate geometry: Check if data is valid
-- Purpose: Use ST_IsValid to ensure geometry integrity
SELECT 
    area_id,
    area_name,
    ST_IsValid(boundary) AS is_valid
FROM areas;

-- 12. Spatial aggregation: Count customers per area
-- Purpose: Aggregate customers within each area using ST_Contains
SELECT 
    a.area_name,
    COUNT(c.customer_id) AS customer_count
FROM areas a
LEFT JOIN customer_locations c ON ST_Contains(a.boundary, c.location)
GROUP BY a.area_name;

-- 13. Convert JSON to spatial: Create POINT from JSON coordinates
-- Purpose: Use ST_GeomFromGeoJSON to convert JSON to spatial data
INSERT INTO customer_locations (customer_id, location)
VALUES (5, ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [-71.0589, 42.3601]}')); -- Boston

-- 14. Spatial distance with order data: Find orders near a store
-- Purpose: Join orders with customer_locations to calculate distances
SELECT 
    o.order_id,
    c.customer_id,
    ST_Distance_Sphere(c.location, (SELECT location FROM stores WHERE store_id = 1)) AS distance_meters
FROM orders o
JOIN customer_locations c ON o.customer_id = c.customer_id
WHERE ST_Distance_Sphere(c.location, (SELECT location FROM stores WHERE store_id = 1)) <= 10000;

-- 15. Export spatial data: Convert POINT to WKT format
-- Purpose: Use ST_AsText to export spatial data as text
SELECT 
    customer_id,
    ST_AsText(location) AS location_wkt
FROM customer_locations;

-- Lesson Notes:
-- - MySQL supports spatial data types like POINT, LINESTRING, POLYGON, and GEOMETRY.
-- - ST_Distance_Sphere calculates spherical distances; use for geographic coordinates.
-- - Spatial indexes (SPATIAL INDEX) are required for efficient spatial queries.
-- - ST_Within and ST_Contains are used for containment queries; ensure valid geometries with ST_IsValid.
-- - Use ST_GeomFromText or ST_GeomFromGeoJSON to create spatial data.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, stores.csv, customer_locations).

-- =====================================
-- Practice Questions and Answers
-- =====================================
-- The following 15 questions test your understanding of spatial data concepts. Each question includes an explanation and the SQL answer.

-- Question 1: How do you insert a POINT for a customer location?
-- Explanation: ST_GeomFromText creates a POINT from longitude and latitude.
-- Answer:
INSERT INTO customer_locations (customer_id, location)
VALUES (4, ST_GeomFromText('POINT(-122.3321 47.6062)'));

-- Question 2: How can you extract longitude and latitude from a POINT?
-- Explanation: ST_X and ST_Y retrieve the X (longitude) and Y (latitude) coordinates.
-- Answer:
SELECT 
    customer_id,
    ST_X(location) AS longitude,
    ST_Y(location) AS latitude
FROM customer_locations;

-- Question 3: How do you calculate the distance between customers and a store?
-- Explanation: ST_Distance_Sphere computes spherical distance in meters.
-- Answer:
SELECT 
    c.customer_id,
    s.store_name,
    ST_Distance_Sphere(c.location, s.location) AS distance_meters
FROM customer_locations c
CROSS JOIN stores s
WHERE s.store_id = 1;

-- Question 4: How can you find customers within 10km of a store?
-- Explanation: ST_Distance_Sphere filters customers within a specified radius.
-- Answer:
SELECT 
    c.customer_id,
    ST_X(c.location) AS longitude,
    ST_Y(c.location) AS latitude
FROM customer_locations c
WHERE ST_Distance_Sphere(c.location, 
    (SELECT location FROM stores WHERE store_id = 1)) <= 10000;

-- Question 5: How do you check if customers are within a polygon?
-- Explanation: ST_Within tests if a POINT lies within a POLYGON.
-- Answer:
SET @poly = ST_GeomFromText('POLYGON((-74.1 40.6, -73.9 40.6, -73.9 40.8, -74.1 40.8, -74.1 40.6))');
SELECT 
    customer_id,
    ST_X(location) AS longitude,
    ST_Y(location) AS latitude
FROM customer_locations
WHERE ST_Within(location, @poly);

-- Question 6: How can you verify spatial index usage in a query?
-- Explanation: EXPLAIN shows whether the spatial index is used for a distance query.
-- Answer:
EXPLAIN SELECT 
    customer_id
FROM customer_locations
WHERE ST_Distance_Sphere(location, ST_GeomFromText('POINT(-74.0050 40.7100)')) <= 10000;

-- Question 7: How do you create a table with a GEOMETRY column?
-- Explanation: GEOMETRY supports various spatial types with a SPATIAL INDEX.
-- Answer:
CREATE TABLE areas (
    area_id INT PRIMARY KEY,
    area_name VARCHAR(100),
    boundary GEOMETRY NOT NULL,
    SPATIAL INDEX idx_boundary (boundary)
);

-- Question 8: How can you insert a POLYGON for a geographic area?
-- Explanation: ST_GeomFromText creates a POLYGON from coordinates.
-- Answer:
INSERT INTO areas (area_id, area_name, boundary)
VALUES (1, 'Downtown NY', ST_GeomFromText('POLYGON((-74.01 40.70, -74.00 40.70, -74.00 40.72, -74.01 40.72, -74.01 40.70))'));

-- Question 9: How do you find customers within a specific area?
-- Explanation: ST_Contains joins customers with areas based on spatial containment.
-- Answer:
SELECT 
    c.customer_id,
    a.area_name
FROM customer_locations c
JOIN areas a
WHERE ST_Contains(a.boundary, c.location);

-- Question 10: How can you create a buffer zone around a store?
-- Explanation: ST_Buffer creates an approximate circular region around a POINT.
-- Answer:
SET @store_point = (SELECT location FROM stores WHERE store_id = 1);
SELECT 
    ST_AsText(ST_Buffer(@store_point, 0.01)) AS buffer_zone
FROM DUAL;

-- Question 11: How do you validate geometry data?
-- Explanation: ST_IsValid checks if a GEOMETRY object is well-formed.
-- Answer:
SELECT 
    area_id,
    area_name,
    ST_IsValid(boundary) AS is_valid
FROM areas;

-- Question 12: How can you count customers per geographic area?
-- Explanation: ST_Contains with GROUP BY aggregates customers within areas.
-- Answer:
SELECT 
    a.area_name,
    COUNT(c.customer_id) AS customer_count
FROM areas a
LEFT JOIN customer_locations c ON ST_Contains(a.boundary, c.location)
GROUP BY a.area_name;

-- Question 13: How do you create a POINT from JSON coordinates?
-- Explanation: ST_GeomFromGeoJSON converts JSON coordinates to a POINT.
-- Answer:
INSERT INTO customer_locations (customer_id, location)
VALUES (5, ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [-71.0589, 42.3601]}'));

-- Question 14: How can you find orders placed by customers near a store?
-- Explanation: Join orders with customer_locations and use ST_Distance_Sphere.
-- Answer:
SELECT 
    o.order_id,
    c.customer_id,
    ST_Distance_Sphere(c.location, (SELECT location FROM stores WHERE store_id = 1)) AS distance_meters
FROM orders o
JOIN customer_locations c ON o.customer_id = c.customer_id
WHERE ST_Distance_Sphere(c.location, (SELECT location FROM stores WHERE store_id = 1)) <= 10000;

-- Question 15: How do you export spatial data as text?
-- Explanation: ST_AsText converts a POINT to Well-Known Text (WKT) format.
-- Answer:
SELECT 
    customer_id,
    ST_AsText(location) AS location_wkt
FROM customer_locations;

-- =====================================
-- Final Notes
-- =====================================
-- - Spatial data types (POINT, GEOMETRY) enable geographic analysis in MySQL.
-- - ST_Distance_Sphere is ideal for distance calculations on Earth’s surface; ST_Buffer is approximate.
-- - Spatial indexes are critical for performance; always define SPATIAL INDEX on spatial columns.
-- - Validate geometries with ST_IsValid to avoid errors in spatial queries.
-- - Ensure the `mysql_learning` database is set up with sample datasets (customers.csv, orders.csv, stores.csv, customer_locations).
-- - For further learning, explore `JSON_Features.sql` for semi-structured data or `Index_Optimization.sql` for performance tuning.
