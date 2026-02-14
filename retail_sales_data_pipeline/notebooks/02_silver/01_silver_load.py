# Databricks notebook source
sales_df=spark.read.format("delta").table("retailer.bronze.sales").select("transaction_id","customer_id","product_id","quantity","price","date")

# COMMAND ----------

# DBTITLE 1,MD5
import pyspark.sql.functions as F
source_df = sales_df
business_key = ["transaction_id"]
change_cols = ["customer_id", "product_id", "quantity", "price", "date"]
src_df = (
    source_df
    .withColumn(
        "hashdiff",
        F.md5(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                    for c in change_cols
                ]
            )
        )
    )
    .withColumn("effective_from_ts", F.current_timestamp())
    .withColumn("effective_to_ts", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .withColumn("created_ts", F.current_timestamp())
    .withColumn("updated_ts", F.current_timestamp())
)


# COMMAND ----------

src_df.createOrReplaceTempView("sales_src")

# COMMAND ----------

spark.sql("""
          MERGE INTO retailer.silver.sales AS tgt
USING sales_src AS src
ON tgt.transaction_id = src.transaction_id
AND tgt.is_current = true

WHEN MATCHED AND tgt.hashdiff <> src.hashdiff THEN
  UPDATE SET
    tgt.effective_to_ts = current_timestamp(),
    tgt.is_current = false,
    tgt.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id,
    customer_id,
    product_id,
    quantity,
    price,
    date,
    hashdiff,
    effective_from_ts,
    effective_to_ts,
    is_current,
    created_ts,
    updated_ts
  )
  VALUES (
    src.transaction_id,
    src.customer_id,
    src.product_id,
    src.quantity,
    src.price,
    src.date,
    src.hashdiff,
    current_timestamp(),
    NULL,
    true,
    current_timestamp(),
    current_timestamp()
  )
""")

# COMMAND ----------

spark.sql("""
INSERT INTO retailer.silver.sales
SELECT
    src.transaction_id,
    src.customer_id,
    src.product_id,
    src.quantity,
    src.price,
    src.date,
    true as is_current,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts,
    current_timestamp() as effective_from_ts,
    NULL as effective_to_ts,
    src.hashdiff
FROM sales_src src
LEFT JOIN retailer.silver.sales tgt
    ON tgt.transaction_id = src.transaction_id
    AND tgt.is_current = true
WHERE tgt.transaction_id IS NULL
   OR tgt.hashdiff <> src.hashdiff
          """)