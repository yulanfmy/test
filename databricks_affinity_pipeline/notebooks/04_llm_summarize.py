# Databricks notebook source
# MAGIC %md
# MAGIC # 04. LLMによる事業概要の要約・生成
# MAGIC
# MAGIC Web検索結果を元に、LLMを使って各企業の事業概要を日本語で生成します。
# MAGIC
# MAGIC ## サポートするLLMプロバイダ
# MAGIC - Databricks Foundation Model API (推奨)
# MAGIC - OpenAI API (フォールバック)

# COMMAND ----------

import json
import logging
import time
import uuid

import requests
from pyspark.sql import Row

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("batch_id", "", "バッチID")
dbutils.widgets.dropdown(
    "llm_provider", "databricks",
    ["databricks", "openai"],
    "LLMプロバイダ"
)
dbutils.widgets.text("llm_model", "databricks-meta-llama-3-1-70b-instruct", "LLMモデル名")
dbutils.widgets.text("max_tokens", "500", "最大トークン数")
dbutils.widgets.text("temperature", "0.3", "Temperature")
dbutils.widgets.text("confidence_threshold", "0.7", "信頼度閾値")

BATCH_ID = dbutils.widgets.get("batch_id")
LLM_PROVIDER = dbutils.widgets.get("llm_provider")
LLM_MODEL = dbutils.widgets.get("llm_model")
MAX_TOKENS = int(dbutils.widgets.get("max_tokens"))
TEMPERATURE = float(dbutils.widgets.get("temperature"))
CONFIDENCE_THRESHOLD = float(dbutils.widgets.get("confidence_threshold"))

print(f"Batch ID: {BATCH_ID}")
print(f"LLM Provider: {LLM_PROVIDER}")
print(f"LLM Model: {LLM_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. LLMクライアントの初期化

# COMMAND ----------

# プロンプト定義
SYSTEM_PROMPT = """あなたはスタートアップ企業の事業概要を要約するアシスタントです。
与えられたWeb検索結果を元に、以下の要件で簡潔な事業概要を日本語で作成してください。

要件：
- 100〜300文字程度で簡潔にまとめる
- 事業内容、提供サービス/プロダクト、対象市場/顧客を含める
- 客観的かつ正確な記述にする
- 情報が不十分な場合は、得られた情報のみで記述し、推測は含めない
- Web検索結果から事業内容が全く特定できない場合は「情報不足」と回答する"""

USER_PROMPT_TEMPLATE = """以下のWeb検索結果を元に、「{company_name}」の事業概要を作成してください。

## Web検索結果
{search_results}

## 出力
事業概要を日本語で簡潔に記述してください。"""


def format_search_results(results: list[dict]) -> str:
    """検索結果をプロンプト用にフォーマット"""
    formatted = []
    for i, r in enumerate(results, start=1):
        formatted.append(
            f"### 結果{i}\n"
            f"- タイトル: {r.get('title', 'N/A')}\n"
            f"- URL: {r.get('url', 'N/A')}\n"
            f"- スニペット: {r.get('snippet', 'N/A')}"
        )
    return "\n\n".join(formatted) if formatted else "（検索結果なし）"


def estimate_confidence(summary: str, search_results: list[dict]) -> float:
    """要約の信頼度スコアを簡易推定"""
    if not summary or summary == "情報不足":
        return 0.0
    score = 0.5
    n_results = len(search_results)
    if n_results >= 3:
        score += 0.2
    elif n_results >= 1:
        score += 0.1
    length = len(summary)
    if 50 <= length <= 500:
        score += 0.2
    elif 20 <= length < 50:
        score += 0.1
    keywords = ["提供", "サービス", "開発", "事業", "プラットフォーム", "ソリューション"]
    matched = sum(1 for kw in keywords if kw in summary)
    score += min(matched * 0.05, 0.1)
    return min(score, 1.0)

# COMMAND ----------

# LLMクライアントの設定
if LLM_PROVIDER == "databricks":
    # Databricks Foundation Model APIを使用
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", "")
    db_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    def call_llm(company_name: str, search_results: list[dict]) -> tuple[str, int]:
        """Databricks Foundation Model APIを呼び出し"""
        user_prompt = USER_PROMPT_TEMPLATE.format(
            company_name=company_name,
            search_results=format_search_results(search_results),
        )
        payload = {
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE,
        }
        endpoint = f"https://{workspace_url}/serving-endpoints/{LLM_MODEL}/invocations"
        headers = {"Authorization": f"Bearer {db_token}", "Content-Type": "application/json"}
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
        tokens = data.get("usage", {}).get("total_tokens", 0)
        return content, tokens

elif LLM_PROVIDER == "openai":
    openai_key = dbutils.secrets.get(scope="affinity-pipeline", key="openai-api-key")

    def call_llm(company_name: str, search_results: list[dict]) -> tuple[str, int]:
        """OpenAI APIを呼び出し"""
        user_prompt = USER_PROMPT_TEMPLATE.format(
            company_name=company_name,
            search_results=format_search_results(search_results),
        )
        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": MAX_TOKENS,
            "temperature": TEMPERATURE,
        }
        headers = {"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"}
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            json=payload, headers=headers, timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"].strip()
        tokens = data.get("usage", {}).get("total_tokens", 0)
        return content, tokens

print(f"LLM client initialized: {LLM_PROVIDER} / {LLM_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 対象企業と検索結果の読み込み

# COMMAND ----------

# 対象企業ごとに検索結果を集約
search_data = spark.sql(f"""
SELECT
    sn.company_id,
    sn.company_name,
    COLLECT_LIST(
        NAMED_STRUCT(
            'title', sn.source_title,
            'url', sn.source_url,
            'snippet', sn.extracted_text
        )
    ) AS search_results,
    COUNT(*) AS result_count
FROM affinity_demo.company_enrichment.search_normalized sn
WHERE sn.batch_id = '{BATCH_ID}'
GROUP BY sn.company_id, sn.company_name
ORDER BY result_count DESC
""").collect()

print(f"Companies with search results: {len(search_data)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. LLMバッチ処理の実行

# COMMAND ----------

enrichment_results = []
total = len(search_data)
total_tokens = 0
success_count = 0
failed_count = 0
low_confidence_count = 0

for i, row in enumerate(search_data):
    company_id = row["company_id"]
    company_name = row["company_name"]
    results_list = [r.asDict() for r in row["search_results"]]

    if (i + 1) % 10 == 0 or i == 0:
        print(f"\nProcessing [{i+1}/{total}]: {company_name}")

    try:
        summary, tokens = call_llm(company_name, results_list)
        confidence = estimate_confidence(summary, results_list)
        total_tokens += tokens

        if confidence >= CONFIDENCE_THRESHOLD:
            status = "success"
            success_count += 1
        else:
            status = "low_confidence"
            low_confidence_count += 1

        enrichment_results.append({
            "log_id": f"log_{uuid.uuid4().hex[:12]}",
            "company_id": company_id,
            "company_name": company_name,
            "generated_summary": summary,
            "confidence_score": confidence,
            "llm_model": LLM_MODEL,
            "llm_provider": LLM_PROVIDER,
            "tokens_used": tokens,
            "search_results_count": len(results_list),
            "status": status,
            "error_message": None,
            "approved_by": None,
            "approved_at": None,
            "batch_id": BATCH_ID,
        })

    except Exception as e:
        logger.error(f"LLM error for {company_name}: {e}")
        failed_count += 1
        enrichment_results.append({
            "log_id": f"log_{uuid.uuid4().hex[:12]}",
            "company_id": company_id,
            "company_name": company_name,
            "generated_summary": None,
            "confidence_score": 0.0,
            "llm_model": LLM_MODEL,
            "llm_provider": LLM_PROVIDER,
            "tokens_used": 0,
            "search_results_count": len(results_list),
            "status": "failed",
            "error_message": str(e),
            "approved_by": None,
            "approved_at": None,
            "batch_id": BATCH_ID,
        })

    # API負荷軽減
    time.sleep(0.5)

print("\n=== LLM Processing Summary ===")
print(f"Total processed: {total}")
print(f"Success: {success_count}")
print(f"Low confidence: {low_confidence_count}")
print(f"Failed: {failed_count}")
print(f"Total tokens used: {total_tokens}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. エンリッチメントログの保存

# COMMAND ----------

if enrichment_results:
    log_df = spark.createDataFrame([Row(**r) for r in enrichment_results])
    log_df.write.mode("append").saveAsTable(
        "affinity_demo.company_enrichment.enrichment_log"
    )
    print(f"Saved {log_df.count()} enrichment log records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 処理結果の確認

# COMMAND ----------

display(spark.sql(f"""
SELECT
    status,
    COUNT(*) AS count,
    ROUND(AVG(confidence_score), 3) AS avg_confidence,
    ROUND(AVG(tokens_used), 0) AS avg_tokens,
    ROUND(AVG(LENGTH(generated_summary)), 0) AS avg_summary_length
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}'
GROUP BY status
"""))

# COMMAND ----------

# 高信頼度の要約サンプル
display(spark.sql(f"""
SELECT company_name, generated_summary, confidence_score, status
FROM affinity_demo.company_enrichment.enrichment_log
WHERE batch_id = '{BATCH_ID}' AND status = 'success'
ORDER BY confidence_score DESC
LIMIT 10
"""))

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "batch_id": BATCH_ID,
    "total": total,
    "success": success_count,
    "low_confidence": low_confidence_count,
    "failed": failed_count,
    "total_tokens": total_tokens,
}))
