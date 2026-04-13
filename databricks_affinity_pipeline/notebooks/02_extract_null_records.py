# Databricks notebook source
# MAGIC %md
# MAGIC # 02. null事業概要レコードの抽出
# MAGIC
# MAGIC 企業マスタから `business_description` が null のレコードを抽出し、
# MAGIC Web検索の対象リストを作成します。

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("max_companies", "500", "最大処理件数")
dbutils.widgets.dropdown(
    "priority_order", "funding_stage",
    ["funding_stage", "founded_year", "random"],
    "優先順位"
)

MAX_COMPANIES = int(dbutils.widgets.get("max_companies"))
PRIORITY_ORDER = dbutils.widgets.get("priority_order")

print(f"Max companies to process: {MAX_COMPANIES}")
print(f"Priority order: {PRIORITY_ORDER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. null事業概要の現状把握

# COMMAND ----------

# 現在の統計
stats = spark.sql("""
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) AS null_total,
    SUM(CASE WHEN business_description IS NULL AND enrichment_status = 'pending' THEN 1 ELSE 0 END) AS pending,
    SUM(CASE WHEN enrichment_status = 'in_progress' THEN 1 ELSE 0 END) AS in_progress,
    SUM(CASE WHEN enrichment_status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN enrichment_status = 'failed' THEN 1 ELSE 0 END) AS failed
FROM affinity_demo.company_enrichment.company_master
""")

display(stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 対象レコードの抽出

# COMMAND ----------

# ステージの優先度マッピング
stage_priority = {
    "Series D+": 1, "Series C": 2, "Series B": 3, "Series A": 4,
    "Seed": 5, "Pre-Seed": 6, "IPO準備中": 1, "Bootstrap": 7,
}

# nullレコードを抽出（pending状態のもの）
null_records = spark.sql("""
SELECT
    company_id,
    company_name,
    company_name_en,
    website_url,
    industry,
    sub_industry,
    funding_stage,
    founded_year,
    headquarters
FROM affinity_demo.company_enrichment.company_master
WHERE business_description IS NULL
  AND enrichment_status = 'pending'
""")

print(f"Total null records (pending): {null_records.count()}")

# COMMAND ----------

# 優先順位に基づいてソート
if PRIORITY_ORDER == "funding_stage":

    # ファンディングステージに基づく優先度
    priority_expr = (
        F.when(F.col("funding_stage") == "IPO準備中", 1)
        .when(F.col("funding_stage") == "Series D+", 1)
        .when(F.col("funding_stage") == "Series C", 2)
        .when(F.col("funding_stage") == "Series B", 3)
        .when(F.col("funding_stage") == "Series A", 4)
        .when(F.col("funding_stage") == "Seed", 5)
        .when(F.col("funding_stage") == "Pre-Seed", 6)
        .otherwise(7)
    )
    target_df = null_records.withColumn("priority", priority_expr).orderBy("priority")

elif PRIORITY_ORDER == "founded_year":
    target_df = null_records.orderBy(F.col("founded_year").desc())

else:  # random
    target_df = null_records.orderBy(F.rand(seed=42))

# 最大件数で制限
target_df = target_df.limit(MAX_COMPANIES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 対象企業リストの保存（一時テーブル）

# COMMAND ----------

# 一時ビューとして保存（後続ノートブックで使用）
target_df.createOrReplaceTempView("target_companies")

# 永続テーブルにも保存（バッチ管理用）
import uuid
from datetime import datetime

batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

target_with_batch = target_df.withColumn("batch_id", F.lit(batch_id))
target_with_batch.write.mode("overwrite").saveAsTable(
    "affinity_demo.company_enrichment._tmp_target_companies"
)

print(f"Batch ID: {batch_id}")
print(f"Target companies: {target_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 対象企業のステータス更新

# COMMAND ----------

# 対象企業のステータスを 'in_progress' に更新
spark.sql("""
MERGE INTO affinity_demo.company_enrichment.company_master AS target
USING affinity_demo.company_enrichment._tmp_target_companies AS source
ON target.company_id = source.company_id
WHEN MATCHED THEN
    UPDATE SET
        target.enrichment_status = 'in_progress',
        target.updated_at = CURRENT_TIMESTAMP()
""")

print("Updated target companies status to 'in_progress'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 抽出結果の確認

# COMMAND ----------

display(spark.sql("""
SELECT
    industry,
    funding_stage,
    COUNT(*) AS count
FROM affinity_demo.company_enrichment._tmp_target_companies
GROUP BY industry, funding_stage
ORDER BY count DESC
LIMIT 20
"""))

# COMMAND ----------

# サンプル表示
display(spark.sql("""
SELECT company_id, company_name, website_url, industry, funding_stage
FROM affinity_demo.company_enrichment._tmp_target_companies
LIMIT 10
"""))

# COMMAND ----------

# バッチIDを次のノートブックに渡す
dbutils.notebook.exit(batch_id)
