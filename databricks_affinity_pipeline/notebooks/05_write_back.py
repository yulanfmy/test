# Databricks notebook source
# MAGIC %md
# MAGIC # 05. ターゲットテーブルへの書き戻し
# MAGIC
# MAGIC LLMで生成された事業概要を企業マスタテーブルに書き戻します。
# MAGIC
# MAGIC ## 書き戻しルール
# MAGIC - 信頼度が閾値以上（デフォルト0.7）の要約のみ自動書き戻し
# MAGIC - 低信頼度の要約は手動レビュー待ちとして保留
# MAGIC - 失敗レコードはステータスを 'failed' に更新

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("batch_id", "", "バッチID")
dbutils.widgets.text("confidence_threshold", "0.7", "自動書き戻しの信頼度閾値")
dbutils.widgets.dropdown("auto_approve", "true", ["true", "false"], "閾値以上を自動承認")

BATCH_ID = dbutils.widgets.get("batch_id")
CONFIDENCE_THRESHOLD = float(dbutils.widgets.get("confidence_threshold"))
AUTO_APPROVE = dbutils.widgets.get("auto_approve") == "true"

print(f"Batch ID: {BATCH_ID}")
print(f"Confidence threshold: {CONFIDENCE_THRESHOLD}")
print(f"Auto approve: {AUTO_APPROVE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 書き戻し対象の確認

# COMMAND ----------

# バッチ内の処理結果サマリ
display(spark.sql(f"""
SELECT
    status,
    COUNT(*) AS count,
    ROUND(AVG(confidence_score), 3) AS avg_confidence,
    MIN(confidence_score) AS min_confidence,
    MAX(confidence_score) AS max_confidence
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}'
GROUP BY status
ORDER BY status
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 高信頼度レコードの自動書き戻し

# COMMAND ----------

if AUTO_APPROVE:
    # 信頼度が閾値以上のレコードを企業マスタに書き戻し
    merge_result = spark.sql(f"""
    MERGE INTO affinity_demo.company_enrichment.company_master AS target
    USING (
        SELECT
            company_id,
            generated_summary,
            confidence_score
        FROM affinity_demo.company_enrichment.enrichment_log
        WHERE batch_id = '{BATCH_ID}'
          AND status = 'success'
          AND confidence_score >= {CONFIDENCE_THRESHOLD}
          AND generated_summary IS NOT NULL
          AND generated_summary != '情報不足'
    ) AS source
    ON target.company_id = source.company_id
    WHEN MATCHED THEN
        UPDATE SET
            target.business_description = source.generated_summary,
            target.enrichment_status = 'completed',
            target.updated_at = CURRENT_TIMESTAMP()
    """)

    # 書き戻し件数を確認
    completed_count = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM affinity_demo.company_enrichment.enrichment_log
    WHERE batch_id = '{BATCH_ID}'
      AND status = 'success'
      AND confidence_score >= {CONFIDENCE_THRESHOLD}
      AND generated_summary IS NOT NULL
      AND generated_summary != '情報不足'
    """).collect()[0]["cnt"]

    print(f"Auto-approved and written back: {completed_count} records")

else:
    print("Auto-approve is disabled. All records require manual review.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 低信頼度レコードの処理

# COMMAND ----------

# 低信頼度レコードのステータスを更新（手動レビュー待ち）
spark.sql(f"""
MERGE INTO affinity_demo.company_enrichment.company_master AS target
USING (
    SELECT company_id
    FROM affinity_demo.company_enrichment.enrichment_log
    WHERE batch_id = '{BATCH_ID}'
      AND status = 'low_confidence'
) AS source
ON target.company_id = source.company_id
WHEN MATCHED THEN
    UPDATE SET
        target.enrichment_status = 'pending',
        target.updated_at = CURRENT_TIMESTAMP()
""")

low_conf_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}' AND status = 'low_confidence'
""").collect()[0]["cnt"]

print(f"Low confidence (pending manual review): {low_conf_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 失敗レコードの処理

# COMMAND ----------

# 失敗レコードのステータスを更新
spark.sql(f"""
MERGE INTO affinity_demo.company_enrichment.company_master AS target
USING (
    SELECT company_id
    FROM affinity_demo.company_enrichment.enrichment_log
    WHERE batch_id = '{BATCH_ID}'
      AND status = 'failed'
) AS source
ON target.company_id = source.company_id
WHEN MATCHED THEN
    UPDATE SET
        target.enrichment_status = 'failed',
        target.updated_at = CURRENT_TIMESTAMP()
""")

failed_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}' AND status = 'failed'
""").collect()[0]["cnt"]

print(f"Failed: {failed_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 書き戻し結果の確認

# COMMAND ----------

display(spark.sql("""
SELECT
    enrichment_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct
FROM affinity_demo.company_enrichment.company_master
GROUP BY enrichment_status
ORDER BY count DESC
"""))

# COMMAND ----------

# 書き戻されたデータのサンプル
display(spark.sql(f"""
SELECT
    cm.company_id,
    cm.company_name,
    cm.business_description,
    cm.enrichment_status,
    el.confidence_score,
    cm.updated_at
FROM affinity_demo.company_enrichment.company_master cm
JOIN affinity_demo.company_enrichment.enrichment_log el
    ON cm.company_id = el.company_id
WHERE el.batch_id = '{BATCH_ID}'
  AND cm.enrichment_status = 'completed'
ORDER BY el.confidence_score DESC
LIMIT 10
"""))

# COMMAND ----------

dbutils.notebook.exit(f"Write-back completed for batch {BATCH_ID}")
