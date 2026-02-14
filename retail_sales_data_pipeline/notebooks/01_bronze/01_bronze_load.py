# Databricks notebook source
# MAGIC %run /Workspace/Users/siddeshhk.436@gmail.com/portfolio_projects/retail_sales_data_pipeline/notebooks/utilities/utility

# COMMAND ----------

# DBTITLE 1,variable declaration
sales_path = "/Volumes/retailer/bronze/volume/sales.csv"
customers_path = "/Volumes/retailer/bronze/volume/customers.csv"
products_path = "/Volumes/retailer/bronze/volume/products.csv"

# COMMAND ----------

# DBTITLE 1,bronze read
import pyspark.sql.functions as F
sales_df = safe_read_csv(sales_path).withColumn("load_ts", F.current_timestamp()).withColumn("source_file_name", F.lit(sales_path))
customers_df = safe_read_csv(customers_path).withColumn("load_ts", F.current_timestamp()).withColumn("source_file_name", F.lit(customers_path))
products_df = safe_read_csv(products_path).withColumn("load_ts", F.current_timestamp()).withColumn("source_file_name", F.lit(products_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table retailer.bronze.sales;
# MAGIC truncate table retailer.bronze.customers;
# MAGIC truncate table retailer.bronze.products;

# COMMAND ----------

# DBTITLE 1,bronze write
safe_write_delta(sales_df, "retailer.bronze.sales")
safe_write_delta(customers_df, "retailer.bronze.customers")
safe_write_delta(products_df, "retailer.bronze.products")