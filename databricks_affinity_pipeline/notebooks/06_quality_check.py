# Databricks notebook source
# MAGIC %md
# MAGIC # 06. 品質チェック・レポート生成
# MAGIC
# MAGIC バッチ処理の品質をチェックし、レポートを生成します。
# MAGIC 手動修正・監査用のビューも更新します。
# MAGIC
# MAGIC ## チェック項目
# MAGIC - 要約の文字数チェック（短すぎ/長すぎ）
# MAGIC - 信頼度スコアの分布
# MAGIC - 検索結果の品質
# MAGIC - 処理成功率
# MAGIC - 異常値検出

# COMMAND ----------

import json
import uuid
from datetime import date

from pyspark.sql import Row
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("batch_id", "", "バッチID")
dbutils.widgets.text("min_description_length", "20", "最小事業概要文字数")
dbutils.widgets.text("max_description_length", "2000", "最大事業概要文字数")

BATCH_ID = dbutils.widgets.get("batch_id")
MIN_DESC_LEN = int(dbutils.widgets.get("min_description_length"))
MAX_DESC_LEN = int(dbutils.widgets.get("max_description_length"))

print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. バッチ処理統計の算出

# COMMAND ----------

# エンリッチメントログの統計
batch_stats = spark.sql(f"""
SELECT
    COUNT(*) AS total_processed,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN status = 'low_confidence' THEN 1 ELSE 0 END) AS low_confidence_count,
    ROUND(AVG(confidence_score), 4) AS avg_confidence,
    ROUND(AVG(CASE WHEN generated_summary IS NOT NULL THEN LENGTH(generated_summary) ELSE 0 END), 1) AS avg_summary_length,
    SUM(tokens_used) AS total_tokens_used,
    SUM(CASE WHEN llm_provider = 'databricks' THEN 1 ELSE 0 END) AS databricks_calls,
    SUM(CASE WHEN llm_provider = 'openai' THEN 1 ELSE 0 END) AS openai_calls
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}'
""").collect()[0]

# 検索結果の統計
search_stats = spark.sql(f"""
SELECT
    COUNT(DISTINCT company_id) AS companies_searched,
    COUNT(*) AS total_search_results,
    SUM(CASE WHEN result_snippet IS NOT NULL THEN 1 ELSE 0 END) AS results_with_snippets
FROM affinity_demo.company_enrichment.search_raw_results
WHERE batch_id = '{BATCH_ID}'
""").collect()[0]

# 企業マスタの全体統計
master_stats = spark.sql("""
SELECT
    COUNT(*) AS total_companies,
    SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) AS null_descriptions
FROM affinity_demo.company_enrichment.company_master
""").collect()[0]

print("=== Batch Processing Statistics ===")
print(f"Total processed: {batch_stats['total_processed']}")
print(f"Success: {batch_stats['success_count']}")
print(f"Failed: {batch_stats['failed_count']}")
print(f"Low confidence: {batch_stats['low_confidence_count']}")
print(f"Avg confidence: {batch_stats['avg_confidence']}")
print(f"Total tokens: {batch_stats['total_tokens_used']}")
print(f"\nCompanies still with null descriptions: {master_stats['null_descriptions']}/{master_stats['total_companies']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 品質チェック

# COMMAND ----------

# 文字数チェック
print("=== Description Length Check ===")
display(spark.sql(f"""
SELECT
    CASE
        WHEN generated_summary IS NULL THEN 'null'
        WHEN LENGTH(generated_summary) < {MIN_DESC_LEN} THEN 'too_short'
        WHEN LENGTH(generated_summary) > {MAX_DESC_LEN} THEN 'too_long'
        ELSE 'ok'
    END AS length_check,
    COUNT(*) AS count,
    ROUND(AVG(confidence_score), 3) AS avg_confidence
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}'
GROUP BY 1
ORDER BY count DESC
"""))

# COMMAND ----------

# 信頼度スコアの分布
print("=== Confidence Score Distribution ===")
display(spark.sql(f"""
SELECT
    CASE
        WHEN confidence_score >= 0.9 THEN '0.9-1.0 (Very High)'
        WHEN confidence_score >= 0.8 THEN '0.8-0.9 (High)'
        WHEN confidence_score >= 0.7 THEN '0.7-0.8 (Medium)'
        WHEN confidence_score >= 0.5 THEN '0.5-0.7 (Low)'
        WHEN confidence_score > 0 THEN '0.0-0.5 (Very Low)'
        ELSE '0.0 (No Score)'
    END AS confidence_bucket,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}'
GROUP BY 1
ORDER BY 1
"""))

# COMMAND ----------

# 業種別の品質
print("=== Quality by Industry ===")
display(spark.sql(f"""
SELECT
    cm.industry,
    COUNT(*) AS total,
    SUM(CASE WHEN el.status = 'success' THEN 1 ELSE 0 END) AS success,
    ROUND(AVG(el.confidence_score), 3) AS avg_confidence,
    ROUND(SUM(CASE WHEN el.status = 'success' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS success_rate_pct
FROM affinity_demo.company_enrichment.enrichment_log el
JOIN affinity_demo.company_enrichment.company_master cm ON el.company_id = cm.company_id
WHERE el.batch_id = '{BATCH_ID}'
GROUP BY cm.industry
ORDER BY success_rate_pct DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 品質レポートの保存

# COMMAND ----------

report_row = {
    "report_id": f"rpt_{uuid.uuid4().hex[:12]}",
    "batch_id": BATCH_ID,
    "report_date": str(date.today()),
    "total_companies": int(master_stats["total_companies"]),
    "null_descriptions": int(master_stats["null_descriptions"]),
    "processed_count": int(batch_stats["total_processed"]),
    "success_count": int(batch_stats["success_count"]),
    "failed_count": int(batch_stats["failed_count"]),
    "low_confidence_count": int(batch_stats["low_confidence_count"]),
    "avg_confidence": float(batch_stats["avg_confidence"]) if batch_stats["avg_confidence"] else 0.0,
    "avg_summary_length": float(batch_stats["avg_summary_length"]) if batch_stats["avg_summary_length"] else 0.0,
    "search_api_calls": int(search_stats["companies_searched"]),
    "llm_api_calls": int(batch_stats["total_processed"]),
    "total_tokens_used": int(batch_stats["total_tokens_used"]) if batch_stats["total_tokens_used"] else 0,
    "processing_time_sec": 0.0,  # ワークフローから渡される
}

report_df = spark.createDataFrame([Row(**report_row)])
report_df = report_df.withColumn("report_date", F.to_date("report_date"))
report_df.write.mode("append").saveAsTable(
    "affinity_demo.company_enrichment.quality_report"
)

print(f"Quality report saved: {report_row['report_id']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 手動レビュー待ちレコードの一覧

# COMMAND ----------

print("=== Records Pending Manual Review ===")
display(spark.sql(f"""
SELECT
    el.company_id,
    el.company_name,
    el.generated_summary,
    el.confidence_score,
    el.status,
    cm.industry,
    cm.website_url
FROM affinity_demo.company_enrichment.enrichment_log el
JOIN affinity_demo.company_enrichment.company_master cm ON el.company_id = cm.company_id
WHERE el.batch_id = '{BATCH_ID}'
  AND el.status = 'low_confidence'
ORDER BY el.confidence_score ASC
LIMIT 50
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 監査ビューの更新確認

# COMMAND ----------

# 監査ビューのレコード数
audit_count = spark.sql("""
SELECT COUNT(*) AS cnt
FROM affinity_demo.company_enrichment.v_audit_pending_review
""").collect()[0]["cnt"]

print(f"Records pending audit review: {audit_count}")

# COMMAND ----------

# 品質ダッシュボードビュー確認
display(spark.sql("""
SELECT *
FROM affinity_demo.company_enrichment.v_quality_dashboard
ORDER BY report_date DESC
LIMIT 5
"""))

# COMMAND ----------

# null統計ビュー確認
display(spark.sql("""
SELECT *
FROM affinity_demo.company_enrichment.v_null_summary_stats
ORDER BY null_count DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 処理完了サマリ

# COMMAND ----------

print("=" * 60)
print("  QUALITY REPORT SUMMARY")
print("=" * 60)
print(f"  Batch ID:            {BATCH_ID}")
print(f"  Report Date:         {date.today()}")
print(f"  Total Companies:     {master_stats['total_companies']}")
print(f"  Remaining Nulls:     {master_stats['null_descriptions']}")
print(f"  Processed:           {batch_stats['total_processed']}")
print(f"  Success:             {batch_stats['success_count']}")
print(f"  Failed:              {batch_stats['failed_count']}")
print(f"  Low Confidence:      {batch_stats['low_confidence_count']}")
print(f"  Avg Confidence:      {batch_stats['avg_confidence']}")
print(f"  Total Tokens:        {batch_stats['total_tokens_used']}")
print(f"  Pending Audit:       {audit_count}")
print("=" * 60)

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH_ID,
    "report_id": report_row["report_id"],
    "success_rate": round(
        batch_stats["success_count"] / max(batch_stats["total_processed"], 1) * 100, 1
    ),
}))
