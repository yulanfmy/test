# Databricks notebook source
# MAGIC %md
# MAGIC # パイプライン設定
# MAGIC Affinity事業概要補完パイプラインの共通設定

# COMMAND ----------

# カタログ・スキーマ設定
CATALOG = "affinity_demo"
SCHEMA = "company_enrichment"

# テーブル名
COMPANY_MASTER_TABLE = f"{CATALOG}.{SCHEMA}.company_master"
SEARCH_RAW_TABLE = f"{CATALOG}.{SCHEMA}.search_raw_results"
SEARCH_NORMALIZED_TABLE = f"{CATALOG}.{SCHEMA}.search_normalized"
ENRICHMENT_LOG_TABLE = f"{CATALOG}.{SCHEMA}.enrichment_log"
QUALITY_REPORT_TABLE = f"{CATALOG}.{SCHEMA}.quality_report"

# 監査ビュー
AUDIT_VIEW = f"{CATALOG}.{SCHEMA}.v_audit_pending_review"
QUALITY_DASHBOARD_VIEW = f"{CATALOG}.{SCHEMA}.v_quality_dashboard"
NULL_SUMMARY_VIEW = f"{CATALOG}.{SCHEMA}.v_null_summary_stats"

# Web検索設定
SEARCH_API_PROVIDER = "google"  # "google" or "bing"
SEARCH_MAX_RESULTS_PER_QUERY = 5
SEARCH_BATCH_SIZE = 50
SEARCH_RATE_LIMIT_DELAY_SEC = 1.0
SEARCH_MAX_RETRIES = 3

# LLM設定
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"  # Databricks Foundation Model
LLM_FALLBACK_MODEL = "gpt-4o-mini"  # OpenAI fallback
LLM_MAX_TOKENS = 500
LLM_TEMPERATURE = 0.3
LLM_BATCH_SIZE = 20
LLM_PROVIDER = "databricks"  # "databricks" or "openai"

# 品質チェック閾値
QUALITY_MIN_DESCRIPTION_LENGTH = 20
QUALITY_MAX_DESCRIPTION_LENGTH = 2000
QUALITY_CONFIDENCE_THRESHOLD = 0.7

# 処理設定
PROCESSING_BATCH_SIZE = 100
MAX_COMPANIES_PER_RUN = 500

# スケジュール設定
SCHEDULE_CRON = "0 3 * * 1"  # 毎週月曜日 AM3:00 (JST: AM12:00)
SCHEDULE_TIMEZONE = "Asia/Tokyo"
