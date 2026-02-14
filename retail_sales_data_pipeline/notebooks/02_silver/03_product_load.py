# Databricks notebook source
product_df=spark.read.format("delta").table("retailer.bronze.products").select("product_id","category","brand")

# COMMAND ----------

import pyspark.sql.functions as F
source_df = product_df
business_key = ["product_id"]
change_cols = ["product_id", "category", "brand"]
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

src_df.createOrReplaceTempView("product_src")

# COMMAND ----------

spark.sql("""
          MERGE INTO retailer.silver.products AS tgt
USING product_src AS src
ON tgt.product_id = src.product_id
AND tgt.is_current = true

WHEN MATCHED AND tgt.hashdiff <> src.hashdiff THEN
  UPDATE SET
    tgt.effective_to_ts = current_timestamp(),
    tgt.is_current = false,
    tgt.updated_ts = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT (
    product_id,
    category,
    brand,
    hashdiff,
    effective_from_ts,
    effective_to_ts,
    is_current,
    created_ts,
    updated_ts
  )
  VALUES (
    src.product_id,
    src.category,
    src.brand,
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
INSERT INTO retailer.silver.products
SELECT
    src.product_id,
    src.category,
    src.brand,
    true as is_current,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts,
    current_timestamp() as effective_from_ts,
    NULL as effective_to_ts,
    src.hashdiff
FROM product_src src
LEFT JOIN retailer.silver.products tgt
    ON tgt.product_id = src.product_id
    AND tgt.is_current = true
WHERE tgt.product_id IS NULL
   OR tgt.hashdiff <> src.hashdiff
          """)