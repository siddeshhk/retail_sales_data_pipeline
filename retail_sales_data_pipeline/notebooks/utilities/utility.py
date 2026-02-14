# Databricks notebook source
from pyspark.sql.utils import AnalysisException
from datetime import datetime

# COMMAND ----------

def log(msg, level="INFO"):
    print(f"[{level}] {datetime.now()} - {msg}")

# COMMAND ----------

# DBTITLE 1,csv read
def safe_read_csv(path):
    log(f"Reading file: {path}")

    try:
        df = (spark.read
              .option("header", True)
              .option("inferSchema", True)
              .option("mode", "PERMISSIVE")             # keeps bad rows
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .csv(path))

        # Check empty dataframe
        if df.count() == 0:
            log(f"File is empty: {path}", "WARN")
            return None

        # Check corrupt rows
        if "_corrupt_record" in df.columns:
            bad_count = df.filter("_corrupt_record is not null").count()
            if bad_count > 0:
                log(f"Found {bad_count} corrupt rows in {path}", "WARN")

        log(f"Read success: {path} | rows={df.count()} cols={len(df.columns)}")
        return df

    except Exception as e:
        log(f"Failed reading {path}. Error: {str(e)}", "ERROR")
        return None

# COMMAND ----------

# DBTITLE 1,delta write
def safe_write_delta(df, table_name):
    if df is None:
        log(f"Skipping write for {table_name} because dataframe is None", "WARN")
        return

    log(f"Writing to Delta table: {table_name}")

    try:
        # Create database/schema if not exists
        db = ".".join(table_name.split(".")[:-1])  # retailer.bronze
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {db}")

        # If table does not exist → create
        spark.sql(f"DESCRIBE TABLE {table_name}")
        table_exists = True

    except AnalysisException:
        table_exists = False

    try:
        if table_exists:
            # Append with schema merge (handles new columns)
            (df.write
               .format("delta")
               .mode("append")
               .option("mergeSchema", "true")
               .saveAsTable(table_name))
            log(f"Append success: {table_name}")

        else:
            # Create new table
            (df.write
               .format("delta")
               .mode("overwrite")
               .saveAsTable(table_name))
            log(f"Table created: {table_name}")

    except Exception as e:
        log(f"Write failed for {table_name}. Error: {str(e)}", "ERROR")