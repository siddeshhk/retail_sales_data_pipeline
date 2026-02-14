-- Databricks notebook source
-- DBTITLE 1,catalog_schema_creation
CREATE CATALOG IF NOT EXISTS retailer;
USE CATALOG retailer;
CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS retailer;
USE CATALOG retailer;
CREATE SCHEMA IF NOT EXISTS silver;
USE SCHEMA silver;

-- COMMAND ----------

--drop table if exists retailer.bronze.sales;
--drop table if exists retailer.bronze.customers;
--drop table if exists retailer.bronze.products;
drop table if exists retailer.silver.sales;
drop table if exists retailer.silver.customers;
drop table if exists retailer.silver.products

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retailer.bronze.sales (
  transaction_id INT,
  customer_id INT,
  product_id INT,
  quantity INT,
  price DOUBLE,
  date DATE,
  load_ts TIMESTAMP,
  source_file_name STRING
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retailer.bronze.customers (
  customer_id INT,
  name STRING,
  region STRING,
  load_ts TIMESTAMP,
  source_file_name STRING
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retailer.bronze.products (
  product_id INT,
  category STRING,
  brand STRING,
  load_ts TIMESTAMP,
  source_file_name STRING
);

-- COMMAND ----------

-- DBTITLE 1,sales
CREATE TABLE IF NOT EXISTS retailer.silver.sales (
  transaction_id INT,
  customer_id INT,
  product_id INT,
  quantity INT,
  price DOUBLE,
  date DATE,
  is_current BOOLEAN,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP,
  effective_from_ts TIMESTAMP,
  effective_to_ts TIMESTAMP,
  hashdiff STRING
);

-- COMMAND ----------

-- DBTITLE 1,customers
CREATE TABLE IF NOT EXISTS retailer.silver.customers (
  customer_id INT,
  name STRING,
  region STRING,
  is_current BOOLEAN,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP,
  effective_from_ts TIMESTAMP,
  effective_to_ts TIMESTAMP,
  hashdiff STRING
);

-- COMMAND ----------

-- DBTITLE 1,products
CREATE TABLE IF NOT EXISTS retailer.silver.products (
  product_id INT,
  category STRING,
  brand STRING,
  is_current BOOLEAN,
  created_ts TIMESTAMP,
  updated_ts TIMESTAMP,
  effective_from_ts TIMESTAMP,
  effective_to_ts TIMESTAMP,
  hashdiff STRING
);

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS retailer;
USE CATALOG retailer;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- COMMAND ----------

drop table retailer.gold.sales_fact

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS retailer.gold.sales_fact (
  transaction_id INT,
  customer_id INT,
  name STRING,
  region STRING,
  product_id INT,
  category STRING,
  brand STRING,
  quantity INT,
  price DOUBLE,
  date DATE,
  month INT,
  year INT,
  total_sale_value DOUBLE,
  loyalty_score DOUBLE
);