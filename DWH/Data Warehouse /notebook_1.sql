-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Topic 1: Database vs. Data Warehouse**
-- MAGIC *   **Database (DB):** A database is a system that handles near real-time data stored in small chunks or transactions (like credits and debits in an application). Constantly reading and writing real-time data makes databases highly busy. If you run complex analytical queries on them, performance degrades severely.
-- MAGIC *   **Data Warehouse (DW):** A data warehouse is a central repository where an organization stores all historical and current data (sales, HR, marketing). Data analysts and scientists query the DW to build reports and dashboards without disturbing the operational database. Data is pulled into the DW in bulk, typically on a scheduled basis (e.g., daily at 9 PM), rather than in real-time.
-- MAGIC
-- MAGIC **Topic 2: The ETL Process and Architecture Layers**
-- MAGIC *   **ETL (Extract, Transform, Load):** The process used to fetch data from the database and store it in the data warehouse.
-- MAGIC *   **Staging Layer:** Data is first extracted from the source database and dumped here in its **raw, as-is format** without applying transformations. Staging tables are usually truncated (wiped clean) before every new load to avoid duplicating older records, making them "transient" layers. 
-- MAGIC *   **Core Layer (Curated Data):** Transformations (e.g., adding/removing columns, filtering nulls, calculations) are applied to the staging data, and the refined results are stored in the core layer. This layer is exposed to business users and analysts.
-- MAGIC *   **Data Marts:** These are simply subsets of the data warehouse built for a specific business domain, like Finance or HR, ensuring users only access the data relevant to their department.
-- MAGIC
-- MAGIC **Topic 3: Incremental Data Loading and Change Data Capture (CDC)**
-- MAGIC *   **The Problem with Full Loads:** Reloading the entire database history into the data warehouse every single day is inefficient and wastes processing power. 
-- MAGIC *   **Incremental Loading:** You only load the new records that arrived since the last update. 
-- MAGIC *   **Change Data Capture (CDC):** This is achieved by storing a watermark, such as the maximum date of the last load. On the next run, your SQL query uses a predicate (e.g., `WHERE order_date > max_date`) to fetch only the newly added records. 
-- MAGIC
-- MAGIC **Topic 4: Data Modeling Concepts**
-- MAGIC *   Data modeling is the process of structuring your data to eliminate redundancy and save storage. It happens in three stages:
-- MAGIC     *   **Conceptual Data Model:** High-level business requirements identifying core entities (e.g., Customers, Sales, Products).
-- MAGIC     *   **Logical Data Model:** Defines how tables connect, joining keys, and attributes.
-- MAGIC     *   **Physical Data Model:** The actual creation of physical tables in the database with constraints (like primary keys).
-- MAGIC *   **Dimensional Modeling:** While traditional databases use Entity-Relationship (ER) models and normal forms (normalization) to structure data, Data Warehouses use **Dimensional Data Modeling**, breaking large tables into Fact and Dimension tables.
-- MAGIC
-- MAGIC **Topic 5: Fact Tables vs. Dimension Tables**
-- MAGIC *   **Fact Tables:** These contain the measurable facts or metrics (numeric columns like quantity, price, revenue) and the foreign keys required to connect to dimension tables. The fact table represents the most granular level of the data. 
-- MAGIC *   **Dimension Tables:** These contain the contextual, descriptive information of the data (e.g., customer names, product categories, regions, dates). You cannot store numeric metrics meant for aggregation in a dimension table.
-- MAGIC *   *Implementation Note:* Dimension tables must be created first so that you can generate their keys to insert into the fact table.
-- MAGIC
-- MAGIC **Topic 6: Star Schema vs. Snowflake Schema**
-- MAGIC *   **Star Schema:** Features one central fact table directly connected to multiple dimension tables. There is no hierarchy among the dimensions. This is used in 90% of industry scenarios because it is easier to maintain.
-- MAGIC *   **Snowflake Schema:** Features a hierarchy where a dimension table is connected to another dimension table, creating an indirect link to the fact table (e.g., a Country dimension linked to a Region dimension, which is linked to the Fact table). 
-- MAGIC
-- MAGIC **Topic 7: Surrogate Keys**
-- MAGIC *   In a data warehouse, you do not join tables using the natural "business keys" from the source database (like customer IDs that might have duplicate historical entries). Instead, you generate a **Surrogate Key**.
-- MAGIC *   A surrogate key is a pseudo-key (typically integers like 1, 2, 3) generated using SQL window functions like `ROW_NUMBER() OVER()`. These keys are placed into the fact table to link back to the exact dimension records.
-- MAGIC
-- MAGIC **Topic 8: Types of Fact Tables**
-- MAGIC *   **Transactional Fact Table:** The most common type, where one row equals one unique transaction at the most granular level.
-- MAGIC *   **Periodic / Snapshot Fact Table:** Used when one row represents an aggregation of transactions over a specific period (daily, weekly, monthly).
-- MAGIC *   **Accumulating Snapshot Fact Table:** Tracks the lifecycle or journey of a process, containing multiple date/timestamp columns (e.g., order placed, seller confirmed, dispatched, delivered).
-- MAGIC
-- MAGIC **Topic 9: Types of Dimension Tables**
-- MAGIC *   **Conformed Dimension:** A dimension shared by more than one fact table (e.g., a Product dimension connected to both an "Orders" fact table and a "Cancelled Orders" fact table).
-- MAGIC *   **Role-Playing Dimension:** A single dimension that plays multiple roles in the same fact table through different relationships (e.g., a single Date dimension used for both `Order Date` and `Cancel Date`).
-- MAGIC *   **Junk Dimension:** A dimension created to hold miscellaneous flags or low-cardinality values so they do not clutter the fact table.
-- MAGIC *   **Degenerate Dimension:** A dimension that has no contextual attributes other than its ID (e.g., an Order ID without any order name or details). It is simply stored inside the fact table.
-- MAGIC
-- MAGIC **Topic 10: Slowly Changing Dimensions (SCD)**
-- MAGIC SCD addresses how a data warehouse handles historical changes to dimension attributes (like a product changing its category).
-- MAGIC *   **SCD Type 0:** The data never changes.
-- MAGIC *   **SCD Type 1 (Upsert):** Stands for Update + Insert. It overwrites existing records with the new values and inserts entirely new records. It does not preserve history. In SQL, this is implemented using the `MERGE INTO` statement with `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED THEN INSERT`.
-- MAGIC *   **SCD Type 2:** Preserves the full history of changes by adding new columns to the dimension: `Effective Start Date`, `Effective End Date`, and an `In Use` flag. When a record changes, the old record is marked as expired, and a new active row is inserted.
-- MAGIC *   **SCD Type 3:** Preserves only the immediate previous value by adding a "Previous Value" column to the dimension table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Incremental Data Loading

-- COMMAND ----------

CREATE DATABASE sales_scd;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);


-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);


-- COMMAND ----------

SELECT * FROM sales_new.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA WAREHOUSING

-- COMMAND ----------

CREATE DATABASE orderDWH

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Staging Layer

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.stg_sales 
AS 
SELECT * FROM sales_new.orders 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Transformation

-- COMMAND ----------

CREATE VIEW orderDWH.trans_sales
AS
SELECT * FROM orderDWH.stg_sales WHERE Quantity IS NOT NULL 

-- COMMAND ----------

SELECT * FROM orderdwh.trans_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Core Layer 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimCustomers

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimCustomers 
(
  CustomerID INT,
  CustomerName STRING,
  CustomerEmail STRING,
  DimCustomersKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimCustomers
AS 
SELECT T.*,row_number() over(ORDER BY T.CustomerID) as DimCustomersKey FROM 
(
SELECT 
  DISTINCT(CustomerID) as CustomerID,
  CustomerName,
  CustomerEmail
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

SELECT * FROM orderDWH.view_DimCustomers

-- COMMAND ----------

INSERT INTO orderdwh.DimCustomers 
SELECT * FROM orderdwh.view_DimCustomers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimProducts

-- COMMAND ----------

CREATE TABLE orderDWH.DimProducts
(
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  DimProductsKey INT 
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimProducts
AS 
SELECT T.*,row_number() over(ORDER BY T.ProductID) as DimCustomersKey FROM 
(
SELECT 
  DISTINCT(ProductID) as ProductID,
  ProductName,
  ProductCategory
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimProducts 
SELECT * FROM orderdwh.view_DimProducts

-- COMMAND ----------

SELECT * FROM orderdwh.DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimRegion

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimRegion 
(
  RegionID INT,
  RegionName STRING,
  Country STRING,
  DimRegionKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimRegion
AS 
SELECT T.*,row_number() over(ORDER BY T.RegionID) as DimRegionKey FROM 
(
SELECT 
  DISTINCT(RegionID) as RegionID,
  RegionName,
  Country
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimRegion
SELECT * FROM orderdwh.view_DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### DimDate

-- COMMAND ----------

CREATE OR REPLACE TABLE orderDWH.DimDate
(
  OrderDate Date,
  DimDateKey INT
)

-- COMMAND ----------

CREATE OR REPLACE VIEW orderDWH.view_DimDate
AS 
SELECT T.*,row_number() over(ORDER BY T.OrderDate) as DimDateKey FROM 
(
SELECT 
  DISTINCT(OrderDate) as OrderDate
FROM 
  orderDWH.trans_sales
) AS T

-- COMMAND ----------

INSERT INTO orderdwh.DimDate
SELECT * FROM orderdwh.view_DimDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FACT TABLE

-- COMMAND ----------

CREATE TABLE orderDWH.FactSales
(
  OrderID INT,
  Quantity DECIMAL,
  UnitPrice DECIMAL,
  TotalAmount DECIMAL,
  DimProductsKey INT,
  DimCustomersKeyu INT,
  DimRegionKey INT,
  DimDateKey INT
)

-- COMMAND ----------

SELECT 
  F.OrderID,
  F.Quantity,
  F.UnitPrice,
  F.TotalAmount,
  DC.DimCustomersKey,
  DP.DimProductsKey,
  DR.DimRegionKey,
  DD.DimDateKey
FROM  
  orderDWH.trans_sales F 
LEFT JOIN 
  orderDWH.DimCustomers DC 
  ON F.CustomerID = DC.CustomerID
LEFT JOIN 
  orderDWH.dimproducts DP 
  ON F.ProductID = DP.ProductID
LEFT JOIN 
  orderDWH.DimRegion DR 
  ON DR.Country = F.Country
LEFT JOIN 
  orderDWH.DimDate DD 
  ON F.OrderDate = DD.OrderDate







-- COMMAND ----------

SELECT * FROM orderdwh.DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD TYPE - 1

-- COMMAND ----------

SELECT * FROM sales_scd.orders

-- COMMAND ----------

CREATE OR REPLACE VIEW sales_scd.view_DimProducts
AS
SELECT DISTINCT(ProductID) as ProductID, ProductName, ProductCategory
FROM sales_scd.orders
WHERE OrderDate > '2024-02-10'

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.DimProducts 
(
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING 
)

-- COMMAND ----------

INSERT INTO sales_scd.DimProducts
SELECT ProductID, ProductName, ProductCategory FROM sales_scd.view_DimProducts

-- COMMAND ----------

SELECT * FROM sales_scd.DimProducts

-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'Gaming Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 230, 'Airpods', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00)

-- COMMAND ----------

SELECT * FROM sales_scd.view_DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## MERGE - SCD TYPE - 1

-- COMMAND ----------

MERGE INTO sales_scd.DimProducts AS trg 
USING sales_scd.view_DimPrOducts AS src 
ON trg.ProductID = src.ProductID 
WHEN MATCHED THEN UPDATE SET * 
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT * FROM sales_scd.DimProducts

-- COMMAND ----------


