# Databricks notebook source
# MAGIC %md
# MAGIC # 08. 手動レビュー・承認ノートブック
# MAGIC
# MAGIC 低信頼度の要約を手動でレビュー・承認するためのインタラクティブノートブック。
# MAGIC
# MAGIC ## 使い方
# MAGIC 1. レビュー対象の一覧を確認
# MAGIC 2. 個別にレビューし、承認/却下/修正を行う
# MAGIC 3. 承認された要約は企業マスタに書き戻される

# COMMAND ----------


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. レビュー対象の一覧

# COMMAND ----------

# 監査ビューからレビュー対象を取得
pending_reviews = spark.sql("""
SELECT
    company_id,
    company_name,
    generated_summary,
    confidence_score,
    current_description,
    website_url,
    industry,
    funding_stage,
    generated_at
FROM affinity_demo.company_enrichment.v_audit_pending_review
ORDER BY confidence_score ASC
LIMIT 100
""")

review_count = pending_reviews.count()
print(f"Pending reviews: {review_count}")
display(pending_reviews)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 個別レビュー・承認

# COMMAND ----------

dbutils.widgets.text("review_company_id", "", "レビュー対象の企業ID")
dbutils.widgets.dropdown("review_action", "approve", ["approve", "reject", "edit"], "アクション")
dbutils.widgets.text("edited_summary", "", "修正後の事業概要（editの場合）")
dbutils.widgets.text("reviewer_name", "manual_reviewer", "レビュアー名")

REVIEW_COMPANY_ID = dbutils.widgets.get("review_company_id")
REVIEW_ACTION = dbutils.widgets.get("review_action")
EDITED_SUMMARY = dbutils.widgets.get("edited_summary")
REVIEWER_NAME = dbutils.widgets.get("reviewer_name")

# COMMAND ----------

if REVIEW_COMPANY_ID:
    # レビュー対象の詳細表示
    display(spark.sql(f"""
    SELECT
        el.company_id,
        el.company_name,
        el.generated_summary,
        el.confidence_score,
        el.llm_model,
        el.search_results_count,
        cm.website_url,
        cm.industry,
        cm.funding_stage
    FROM affinity_demo.company_enrichment.enrichment_log el
    JOIN affinity_demo.company_enrichment.company_master cm ON el.company_id = cm.company_id
    WHERE el.company_id = '{REVIEW_COMPANY_ID}'
    ORDER BY el.created_at DESC
    LIMIT 1
    """))

    # 検索結果の確認
    print("\n--- Search Results ---")
    display(spark.sql(f"""
    SELECT source_title, source_url, extracted_text, relevance_score
    FROM affinity_demo.company_enrichment.search_normalized
    WHERE company_id = '{REVIEW_COMPANY_ID}'
    ORDER BY relevance_score DESC
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. アクション実行

# COMMAND ----------

if REVIEW_COMPANY_ID and REVIEW_ACTION:
    if REVIEW_ACTION == "approve":
        # 承認: 生成された要約をそのまま企業マスタに書き戻し
        spark.sql(f"""
        UPDATE affinity_demo.company_enrichment.company_master
        SET
            business_description = (
                SELECT generated_summary
                FROM affinity_demo.company_enrichment.enrichment_log
                WHERE company_id = '{REVIEW_COMPANY_ID}'
                ORDER BY created_at DESC
                LIMIT 1
            ),
            enrichment_status = 'completed',
            updated_at = CURRENT_TIMESTAMP()
        WHERE company_id = '{REVIEW_COMPANY_ID}'
        """)

        # ログの更新
        spark.sql(f"""
        UPDATE affinity_demo.company_enrichment.enrichment_log
        SET
            approved_by = '{REVIEWER_NAME}',
            approved_at = CURRENT_TIMESTAMP(),
            status = 'success'
        WHERE company_id = '{REVIEW_COMPANY_ID}'
          AND approved_by IS NULL
        """)
        print(f"Approved: {REVIEW_COMPANY_ID}")

    elif REVIEW_ACTION == "reject":
        # 却下: ステータスをfailedに
        spark.sql(f"""
        UPDATE affinity_demo.company_enrichment.company_master
        SET
            enrichment_status = 'failed',
            updated_at = CURRENT_TIMESTAMP()
        WHERE company_id = '{REVIEW_COMPANY_ID}'
        """)
        print(f"Rejected: {REVIEW_COMPANY_ID}")

    elif REVIEW_ACTION == "edit" and EDITED_SUMMARY:
        # 修正: 手動修正した要約を書き戻し
        spark.sql(f"""
        UPDATE affinity_demo.company_enrichment.company_master
        SET
            business_description = '{EDITED_SUMMARY}',
            enrichment_status = 'completed',
            updated_at = CURRENT_TIMESTAMP()
        WHERE company_id = '{REVIEW_COMPANY_ID}'
        """)

        spark.sql(f"""
        UPDATE affinity_demo.company_enrichment.enrichment_log
        SET
            approved_by = '{REVIEWER_NAME}',
            approved_at = CURRENT_TIMESTAMP(),
            status = 'success'
        WHERE company_id = '{REVIEW_COMPANY_ID}'
          AND approved_by IS NULL
        """)
        print(f"Edited and approved: {REVIEW_COMPANY_ID}")
    else:
        print("No action taken. Please specify a valid action and company ID.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 一括承認（高信頼度のみ）

# COMMAND ----------

dbutils.widgets.text("bulk_approve_threshold", "0.75", "一括承認の信頼度閾値")
BULK_THRESHOLD = float(dbutils.widgets.get("bulk_approve_threshold"))

# プレビュー
bulk_candidates = spark.sql(f"""
SELECT COUNT(*) AS count, ROUND(AVG(confidence_score), 3) AS avg_conf
FROM affinity_demo.company_enrichment.v_audit_pending_review
WHERE confidence_score >= {BULK_THRESHOLD}
""").collect()[0]

print(f"Bulk approve candidates (confidence >= {BULK_THRESHOLD}):")
print(f"  Count: {bulk_candidates['count']}")
print(f"  Avg confidence: {bulk_candidates['avg_conf']}")

# COMMAND ----------

# 一括承認の実行（コメントアウト解除して実行）
# spark.sql(f"""
# MERGE INTO affinity_demo.company_enrichment.company_master AS target
# USING (
#     SELECT el.company_id, el.generated_summary
#     FROM affinity_demo.company_enrichment.enrichment_log el
#     WHERE el.approved_by IS NULL
#       AND el.confidence_score >= {BULK_THRESHOLD}
#       AND el.status IN ('low_confidence', 'success')
#       AND el.generated_summary IS NOT NULL
# ) AS source
# ON target.company_id = source.company_id
# WHEN MATCHED THEN
#     UPDATE SET
#         target.business_description = source.generated_summary,
#         target.enrichment_status = 'completed',
#         target.updated_at = CURRENT_TIMESTAMP()
# """)
# print("Bulk approval completed")
