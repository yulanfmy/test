# Databricks notebook source
# MAGIC %md
# MAGIC # 07. オーケストレーター（週次パイプライン）
# MAGIC
# MAGIC 全ノートブックを順次実行するマスターノートブック。
# MAGIC Databricksワークフロー（ジョブ）からこのノートブックを呼び出して週次実行します。
# MAGIC
# MAGIC ## パイプライン実行順序
# MAGIC 1. null事業概要レコードの抽出
# MAGIC 2. Web検索による企業情報取得
# MAGIC 3. LLMによる事業概要の要約・生成
# MAGIC 4. ターゲットテーブルへの書き戻し
# MAGIC 5. 品質チェック・レポート生成

# COMMAND ----------

import json
import time
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("max_companies", "500", "最大処理件数")
dbutils.widgets.dropdown(
    "search_provider", "duckduckgo",
    ["google", "bing", "duckduckgo"],
    "検索プロバイダ"
)
dbutils.widgets.dropdown(
    "llm_provider", "databricks",
    ["databricks", "openai"],
    "LLMプロバイダ"
)
dbutils.widgets.text("llm_model", "databricks-meta-llama-3-1-70b-instruct", "LLMモデル名")
dbutils.widgets.text("confidence_threshold", "0.7", "信頼度閾値")
dbutils.widgets.dropdown("auto_approve", "true", ["true", "false"], "自動承認")
dbutils.widgets.dropdown(
    "priority_order", "funding_stage",
    ["funding_stage", "founded_year", "random"],
    "優先順位"
)

MAX_COMPANIES = dbutils.widgets.get("max_companies")
SEARCH_PROVIDER = dbutils.widgets.get("search_provider")
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
LLM_MODEL = dbutils.widgets.get("llm_model")
CONFIDENCE_THRESHOLD = dbutils.widgets.get("confidence_threshold")
AUTO_APPROVE = dbutils.widgets.get("auto_approve")
PRIORITY_ORDER = dbutils.widgets.get("priority_order")

print("=" * 60)
print("  AFFINITY DATA ENRICHMENT PIPELINE")
print("=" * 60)
print(f"  Start time:          {datetime.now().isoformat()}")
print(f"  Max companies:       {MAX_COMPANIES}")
print(f"  Search provider:     {SEARCH_PROVIDER}")
print(f"  LLM provider:        {LLM_PROVIDER}")
print(f"  LLM model:           {LLM_MODEL}")
print(f"  Confidence threshold:{CONFIDENCE_THRESHOLD}")
print(f"  Auto approve:        {AUTO_APPROVE}")
print(f"  Priority order:      {PRIORITY_ORDER}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. パイプライン実行

# COMMAND ----------

pipeline_start = time.time()
notebook_base = "/Workspace/databricks_affinity_pipeline/notebooks"
results = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: null事業概要レコードの抽出

# COMMAND ----------

print("\n>>> Step 1: Extracting null records...")
step1_start = time.time()

try:
    batch_id = dbutils.notebook.run(
        f"{notebook_base}/02_extract_null_records",
        timeout_seconds=600,
        arguments={
            "max_companies": MAX_COMPANIES,
            "priority_order": PRIORITY_ORDER,
        },
    )
    step1_time = time.time() - step1_start
    results["step1"] = {"status": "success", "batch_id": batch_id, "time": step1_time}
    print(f"Step 1 completed in {step1_time:.1f}s. Batch ID: {batch_id}")
except Exception as e:
    results["step1"] = {"status": "failed", "error": str(e)}
    print(f"Step 1 FAILED: {e}")
    dbutils.notebook.exit(json.dumps({"status": "failed", "step": 1, "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Web検索の実行

# COMMAND ----------

print("\n>>> Step 2: Web search...")
step2_start = time.time()

try:
    step2_result = dbutils.notebook.run(
        f"{notebook_base}/03_web_search",
        timeout_seconds=3600,
        arguments={
            "batch_id": batch_id,
            "search_provider": SEARCH_PROVIDER,
            "max_results_per_query": "5",
            "rate_limit_delay": "1.0",
            "batch_size": "50",
        },
    )
    step2_time = time.time() - step2_start
    results["step2"] = {"status": "success", "result": step2_result, "time": step2_time}
    print(f"Step 2 completed in {step2_time:.1f}s")
except Exception as e:
    results["step2"] = {"status": "failed", "error": str(e)}
    print(f"Step 2 FAILED: {e}")
    # 検索失敗でも次のステップに進む（部分的な結果がある可能性）

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: LLMによる事業概要の要約

# COMMAND ----------

print("\n>>> Step 3: LLM summarization...")
step3_start = time.time()

try:
    step3_result = dbutils.notebook.run(
        f"{notebook_base}/04_llm_summarize",
        timeout_seconds=3600,
        arguments={
            "batch_id": batch_id,
            "llm_provider": LLM_PROVIDER,
            "llm_model": LLM_MODEL,
            "max_tokens": "500",
            "temperature": "0.3",
            "confidence_threshold": CONFIDENCE_THRESHOLD,
        },
    )
    step3_time = time.time() - step3_start
    results["step3"] = {"status": "success", "result": step3_result, "time": step3_time}
    print(f"Step 3 completed in {step3_time:.1f}s")
except Exception as e:
    results["step3"] = {"status": "failed", "error": str(e)}
    print(f"Step 3 FAILED: {e}")
    dbutils.notebook.exit(json.dumps({"status": "failed", "step": 3, "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: ターゲットテーブルへの書き戻し

# COMMAND ----------

print("\n>>> Step 4: Write back to target table...")
step4_start = time.time()

try:
    step4_result = dbutils.notebook.run(
        f"{notebook_base}/05_write_back",
        timeout_seconds=600,
        arguments={
            "batch_id": batch_id,
            "confidence_threshold": CONFIDENCE_THRESHOLD,
            "auto_approve": AUTO_APPROVE,
        },
    )
    step4_time = time.time() - step4_start
    results["step4"] = {"status": "success", "result": step4_result, "time": step4_time}
    print(f"Step 4 completed in {step4_time:.1f}s")
except Exception as e:
    results["step4"] = {"status": "failed", "error": str(e)}
    print(f"Step 4 FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: 品質チェック

# COMMAND ----------

print("\n>>> Step 5: Quality check...")
step5_start = time.time()

try:
    step5_result = dbutils.notebook.run(
        f"{notebook_base}/06_quality_check",
        timeout_seconds=600,
        arguments={
            "batch_id": batch_id,
            "min_description_length": "20",
            "max_description_length": "2000",
        },
    )
    step5_time = time.time() - step5_start
    results["step5"] = {"status": "success", "result": step5_result, "time": step5_time}
    print(f"Step 5 completed in {step5_time:.1f}s")
except Exception as e:
    results["step5"] = {"status": "failed", "error": str(e)}
    print(f"Step 5 FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. パイプライン完了サマリ

# COMMAND ----------

total_time = time.time() - pipeline_start

print("\n" + "=" * 60)
print("  PIPELINE EXECUTION SUMMARY")
print("=" * 60)
print(f"  Batch ID:        {batch_id}")
print(f"  Total time:      {total_time:.1f}s ({total_time/60:.1f}min)")
print()

for step_name, step_result in results.items():
    status_icon = "OK" if step_result["status"] == "success" else "NG"
    step_time = step_result.get("time", 0)
    print(f"  {step_name}: [{status_icon}] {step_time:.1f}s")

print("=" * 60)

# 最終結果を返す
final_result = {
    "status": "success" if all(r["status"] == "success" for r in results.values()) else "partial",
    "batch_id": batch_id,
    "total_time_sec": round(total_time, 1),
    "steps": {k: v["status"] for k, v in results.items()},
}

dbutils.notebook.exit(json.dumps(final_result))
