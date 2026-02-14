# Databricks notebook source
customer_df=spark.read.format("delta").table("retailer.bronze.customers").select("customer_id","name","region")

# COMMAND ----------

import pyspark.sql.functions as F
source_df = customer_df
business_key = ["customer_id"]
change_cols = ["customer_id", "name", "region"]
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

src_df.createOrReplaceTempView("customer_src")

# COMMAND ----------

spark.sql("""
          MERGE INTO retailer.silver.customers AS tgt
USING customer_src AS src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = true

WHEN MATCHED AND tgt.hashdiff <> src.hashdiff THEN
  UPDATE SET
    tgt.effective_to_ts = current_timestamp(),
    tgt.is_current = false,
    tgt.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    name,
    region,
    hashdiff,
    effective_from_ts,
    effective_to_ts,
    is_current,
    created_ts,
    updated_ts
  )
  VALUES (
    src.customer_id,
    src.name,
    src.region,
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
INSERT INTO retailer.silver.customers
SELECT
    src.customer_id,
    src.name,
    src.region,
    true as is_current,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts,
    current_timestamp() as effective_from_ts,
    NULL as effective_to_ts,
    src.hashdiff
FROM customer_src src
LEFT JOIN retailer.silver.customers tgt
    ON tgt.customer_id = src.customer_id
    AND tgt.is_current = true
WHERE tgt.customer_id IS NULL
   OR tgt.hashdiff <> src.hashdiff
          """)