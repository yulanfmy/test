# Databricks notebook source
# MAGIC %md
# MAGIC # 00. セットアップ: テーブル定義・初期設定
# MAGIC
# MAGIC Affinity事業概要補完パイプラインで使用するカタログ、スキーマ、テーブルを作成します。
# MAGIC
# MAGIC ## 作成するリソース
# MAGIC | リソース | 説明 |
# MAGIC |---------|------|
# MAGIC | `affinity_demo` カタログ | デモ用Unity Catalogカタログ |
# MAGIC | `company_enrichment` スキーマ | パイプライン用スキーマ |
# MAGIC | `company_master` テーブル | Affinityから取り込んだ企業マスタ |
# MAGIC | `search_raw_results` テーブル | Web検索の生データ |
# MAGIC | `search_normalized` テーブル | 整形・正規化された検索結果 |
# MAGIC | `enrichment_log` テーブル | LLM要約の処理ログ |
# MAGIC | `quality_report` テーブル | 品質チェックレポート |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. カタログ・スキーマの作成

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS affinity_demo")
spark.sql("USE CATALOG affinity_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS company_enrichment")
spark.sql("USE SCHEMA company_enrichment")

print("Catalog and schema created successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 企業マスタテーブルの作成

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS company_master (
    company_id          STRING      NOT NULL    COMMENT '企業ID（Affinity由来）',
    company_name        STRING      NOT NULL    COMMENT '企業名',
    company_name_en     STRING                  COMMENT '企業名（英語）',
    website_url         STRING                  COMMENT '公式サイトURL',
    industry            STRING                  COMMENT '業種',
    sub_industry        STRING                  COMMENT '業種（サブカテゴリ）',
    founded_year        INT                     COMMENT '設立年',
    headquarters        STRING                  COMMENT '本社所在地',
    employee_count      INT                     COMMENT '従業員数',
    funding_stage       STRING                  COMMENT '資金調達ステージ',
    total_funding_jpy   BIGINT                  COMMENT '累計調達額（円）',
    business_description STRING                 COMMENT '事業概要（補完対象）',
    affinity_list_id    STRING                  COMMENT 'AffinityリストID',
    affinity_updated_at TIMESTAMP               COMMENT 'Affinity最終更新日時',
    created_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP() COMMENT 'レコード作成日時',
    updated_at          TIMESTAMP               COMMENT 'レコード更新日時',
    enrichment_status   STRING      DEFAULT 'pending' COMMENT '補完ステータス: pending/in_progress/completed/failed/skipped'
)
USING DELTA
COMMENT 'Affinityから取り込んだスタートアップ企業マスタ'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print("company_master table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Web検索結果テーブル（生データ）

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS search_raw_results (
    search_id           STRING      NOT NULL    COMMENT '検索ID',
    company_id          STRING      NOT NULL    COMMENT '企業ID',
    company_name        STRING      NOT NULL    COMMENT '企業名',
    query_used          STRING      NOT NULL    COMMENT '使用した検索クエリ',
    search_provider     STRING      NOT NULL    COMMENT '検索プロバイダ（google/bing/duckduckgo）',
    result_rank         INT                     COMMENT '検索結果の順位',
    result_title        STRING                  COMMENT '検索結果タイトル',
    result_url          STRING                  COMMENT '検索結果URL',
    result_snippet      STRING                  COMMENT '検索結果スニペット',
    raw_response_json   STRING                  COMMENT 'API応答の生JSON',
    search_timestamp    TIMESTAMP   NOT NULL    COMMENT '検索実行日時',
    batch_id            STRING                  COMMENT 'バッチ処理ID'
)
USING DELTA
COMMENT 'Web検索APIから取得した生の検索結果'
PARTITIONED BY (search_provider)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print("search_raw_results table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 検索結果テーブル（正規化済み）

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS search_normalized (
    company_id          STRING      NOT NULL    COMMENT '企業ID',
    company_name        STRING      NOT NULL    COMMENT '企業名',
    source_url          STRING                  COMMENT '情報源URL',
    source_title        STRING                  COMMENT '情報源タイトル',
    extracted_text      STRING                  COMMENT '抽出されたテキスト',
    content_type        STRING                  COMMENT 'コンテンツ種別（company_page/news/press_release/other）',
    relevance_score     DOUBLE                  COMMENT '関連性スコア',
    search_provider     STRING                  COMMENT '検索プロバイダ',
    processed_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP() COMMENT '処理日時',
    batch_id            STRING                  COMMENT 'バッチ処理ID'
)
USING DELTA
COMMENT '整形・正規化されたWeb検索結果'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print("search_normalized table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. エンリッチメントログテーブル

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS enrichment_log (
    log_id              STRING      NOT NULL    COMMENT 'ログID',
    company_id          STRING      NOT NULL    COMMENT '企業ID',
    company_name        STRING      NOT NULL    COMMENT '企業名',
    generated_summary   STRING                  COMMENT 'LLMが生成した事業概要',
    confidence_score    DOUBLE                  COMMENT '信頼度スコア（0.0〜1.0）',
    llm_model           STRING                  COMMENT '使用したLLMモデル',
    llm_provider        STRING                  COMMENT 'LLMプロバイダ（databricks/openai）',
    tokens_used         INT                     COMMENT '使用トークン数',
    search_results_count INT                    COMMENT '入力した検索結果数',
    status              STRING      NOT NULL    COMMENT 'ステータス: success/failed/low_confidence',
    error_message       STRING                  COMMENT 'エラーメッセージ',
    approved_by         STRING                  COMMENT '承認者（手動レビュー時）',
    approved_at         TIMESTAMP               COMMENT '承認日時',
    created_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP() COMMENT '処理日時',
    batch_id            STRING                  COMMENT 'バッチ処理ID'
)
USING DELTA
COMMENT 'LLMによる事業概要生成の処理ログ'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.enableChangeDataFeed' = 'true'
)
""")

print("enrichment_log table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 品質レポートテーブル

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS quality_report (
    report_id           STRING      NOT NULL    COMMENT 'レポートID',
    batch_id            STRING      NOT NULL    COMMENT 'バッチ処理ID',
    report_date         DATE        NOT NULL    COMMENT 'レポート日付',
    total_companies     INT                     COMMENT '対象企業数',
    null_descriptions   INT                     COMMENT 'null事業概要数',
    processed_count     INT                     COMMENT '処理済み数',
    success_count       INT                     COMMENT '成功数',
    failed_count        INT                     COMMENT '失敗数',
    low_confidence_count INT                    COMMENT '低信頼度数',
    avg_confidence      DOUBLE                  COMMENT '平均信頼度',
    avg_summary_length  DOUBLE                  COMMENT '平均要約文字数',
    search_api_calls    INT                     COMMENT '検索API呼び出し回数',
    llm_api_calls       INT                     COMMENT 'LLM API呼び出し回数',
    total_tokens_used   BIGINT                  COMMENT '合計トークン使用量',
    processing_time_sec DOUBLE                  COMMENT '処理時間（秒）',
    created_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP() COMMENT 'レポート作成日時'
)
USING DELTA
COMMENT '週次バッチ処理の品質レポート'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
""")

print("quality_report table created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 監査用ビューの作成

# COMMAND ----------

# 手動レビュー・監査用ビュー
spark.sql("""
CREATE OR REPLACE VIEW v_audit_pending_review AS
SELECT
    el.company_id,
    el.company_name,
    el.generated_summary,
    el.confidence_score,
    el.llm_model,
    el.status,
    el.created_at AS generated_at,
    cm.business_description AS current_description,
    cm.website_url,
    cm.industry,
    cm.funding_stage
FROM enrichment_log el
JOIN company_master cm ON el.company_id = cm.company_id
WHERE el.status IN ('low_confidence', 'success')
  AND el.approved_by IS NULL
  AND el.confidence_score < 0.8
ORDER BY el.confidence_score ASC, el.created_at DESC
""")

print("v_audit_pending_review view created.")

# COMMAND ----------

# 品質ダッシュボードビュー
spark.sql("""
CREATE OR REPLACE VIEW v_quality_dashboard AS
SELECT
    qr.report_date,
    qr.batch_id,
    qr.total_companies,
    qr.null_descriptions,
    qr.processed_count,
    qr.success_count,
    qr.failed_count,
    qr.low_confidence_count,
    ROUND(qr.avg_confidence, 3) AS avg_confidence,
    ROUND(qr.avg_summary_length, 1) AS avg_summary_length,
    qr.search_api_calls,
    qr.llm_api_calls,
    qr.total_tokens_used,
    ROUND(qr.processing_time_sec, 1) AS processing_time_sec,
    ROUND(
        CASE WHEN qr.processed_count > 0
             THEN qr.success_count * 100.0 / qr.processed_count
             ELSE 0
        END, 1
    ) AS success_rate_pct
FROM quality_report qr
ORDER BY qr.report_date DESC
""")

print("v_quality_dashboard view created.")

# COMMAND ----------

# null事業概要の統計ビュー
spark.sql("""
CREATE OR REPLACE VIEW v_null_summary_stats AS
SELECT
    industry,
    funding_stage,
    COUNT(*) AS total_companies,
    SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) AS null_count,
    ROUND(
        SUM(CASE WHEN business_description IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
        1
    ) AS null_rate_pct,
    SUM(CASE WHEN enrichment_status = 'completed' THEN 1 ELSE 0 END) AS enriched_count,
    SUM(CASE WHEN enrichment_status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN enrichment_status = 'pending' THEN 1 ELSE 0 END) AS pending_count
FROM company_master
GROUP BY industry, funding_stage
ORDER BY null_count DESC
""")

print("v_null_summary_stats view created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. セットアップ完了確認

# COMMAND ----------

tables = spark.sql("SHOW TABLES IN affinity_demo.company_enrichment").collect()
print("\n=== 作成されたテーブル・ビュー一覧 ===")
for t in tables:
    print(f"  - {t.tableName} ({t.tableType if hasattr(t, 'tableType') else 'TABLE'})")

print("\nSetup completed successfully!")
