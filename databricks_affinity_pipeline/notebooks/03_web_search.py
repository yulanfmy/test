# Databricks notebook source
# MAGIC %md
# MAGIC # 03. Web検索による企業情報取得
# MAGIC
# MAGIC 対象企業に対してWeb検索APIを呼び出し、事業概要の元となる情報を収集します。
# MAGIC
# MAGIC ## サポートする検索プロバイダ
# MAGIC - Google Custom Search API
# MAGIC - Bing Web Search API
# MAGIC - DuckDuckGo (APIキー不要のフォールバック)

# COMMAND ----------

import json
import logging
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. パラメータ設定

# COMMAND ----------

dbutils.widgets.text("batch_id", "", "バッチID")
dbutils.widgets.dropdown(
    "search_provider", "duckduckgo",
    ["google", "bing", "duckduckgo"],
    "検索プロバイダ"
)
dbutils.widgets.text("max_results_per_query", "5", "1クエリあたりの最大結果数")
dbutils.widgets.text("rate_limit_delay", "1.0", "APIコール間隔（秒）")
dbutils.widgets.text("batch_size", "50", "バッチサイズ")

BATCH_ID = dbutils.widgets.get("batch_id")
SEARCH_PROVIDER = dbutils.widgets.get("search_provider")
MAX_RESULTS = int(dbutils.widgets.get("max_results_per_query"))
RATE_LIMIT_DELAY = float(dbutils.widgets.get("rate_limit_delay"))
BATCH_SIZE = int(dbutils.widgets.get("batch_size"))

print(f"Batch ID: {BATCH_ID}")
print(f"Search Provider: {SEARCH_PROVIDER}")
print(f"Max results per query: {MAX_RESULTS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 検索APIクレデンシャルの取得

# COMMAND ----------

# Databricks Secretsから認証情報を取得
search_api_key = ""
search_engine_id = ""

if SEARCH_PROVIDER == "google":
    try:
        search_api_key = dbutils.secrets.get(scope="affinity-pipeline", key="google-search-api-key")
        search_engine_id = dbutils.secrets.get(scope="affinity-pipeline", key="google-search-engine-id")
    except Exception as e:
        logger.warning(f"Google API credentials not found in secrets: {e}")
        logger.info("Falling back to DuckDuckGo")
        SEARCH_PROVIDER = "duckduckgo"
elif SEARCH_PROVIDER == "bing":
    try:
        search_api_key = dbutils.secrets.get(scope="affinity-pipeline", key="bing-search-api-key")
    except Exception as e:
        logger.warning(f"Bing API credentials not found in secrets: {e}")
        logger.info("Falling back to DuckDuckGo")
        SEARCH_PROVIDER = "duckduckgo"

print(f"Using search provider: {SEARCH_PROVIDER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 対象企業の読み込み

# COMMAND ----------

target_companies = spark.sql("""
SELECT company_id, company_name, company_name_en, website_url, industry
FROM affinity_demo.company_enrichment._tmp_target_companies
""").collect()

print(f"Target companies to search: {len(target_companies)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. 検索クエリの構築と実行

# COMMAND ----------


import requests


def build_search_query(company_name: str, website_url: str = "") -> str:
    """企業名から検索クエリを構築"""
    query = f"{company_name} 事業内容 会社概要 スタートアップ"
    return query


def search_duckduckgo(query: str, max_results: int = 5) -> list[dict]:
    """DuckDuckGo Instant Answer APIで検索"""
    url = "https://api.duckduckgo.com/"
    params = {"q": query, "format": "json", "no_html": 1, "skip_disambig": 1}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        results = []
        rank = 1
        if data.get("Abstract"):
            results.append({
                "title": data.get("Heading", ""),
                "url": data.get("AbstractURL", ""),
                "snippet": data.get("Abstract", ""),
                "rank": rank,
            })
            rank += 1
        for topic in data.get("RelatedTopics", [])[:max_results]:
            if isinstance(topic, dict) and "Text" in topic:
                results.append({
                    "title": topic.get("Text", "")[:100],
                    "url": topic.get("FirstURL", ""),
                    "snippet": topic.get("Text", ""),
                    "rank": rank,
                })
                rank += 1
        return results[:max_results]
    except Exception as e:
        logger.error(f"DuckDuckGo error: {e}")
        return []


def search_google(query: str, api_key: str, engine_id: str, max_results: int = 5) -> list[dict]:
    """Google Custom Search APIで検索"""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key, "cx": engine_id, "q": query,
        "num": min(max_results, 10), "lr": "lang_ja", "gl": "jp",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                "title": item.get("title", ""),
                "url": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "rank": i,
            }
            for i, item in enumerate(data.get("items", []), start=1)
        ]
    except Exception as e:
        logger.error(f"Google Search error: {e}")
        return []


def search_bing(query: str, api_key: str, max_results: int = 5) -> list[dict]:
    """Bing Web Search APIで検索"""
    url = "https://api.bing.microsoft.com/v7.0/search"
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    params = {"q": query, "count": min(max_results, 50), "mkt": "ja-JP"}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [
            {
                "title": p.get("name", ""),
                "url": p.get("url", ""),
                "snippet": p.get("snippet", ""),
                "rank": i,
            }
            for i, p in enumerate(data.get("webPages", {}).get("value", []), start=1)
        ]
    except Exception as e:
        logger.error(f"Bing Search error: {e}")
        return []


def search_company(company: dict) -> dict:
    """1企業分の検索を実行"""
    query = build_search_query(company["company_name"], company.get("website_url", ""))

    if SEARCH_PROVIDER == "google":
        results = search_google(query, search_api_key, search_engine_id, MAX_RESULTS)
    elif SEARCH_PROVIDER == "bing":
        results = search_bing(query, search_api_key, MAX_RESULTS)
    else:
        results = search_duckduckgo(query, MAX_RESULTS)

    return {
        "company_id": company["company_id"],
        "company_name": company["company_name"],
        "query_used": query,
        "results": results,
        "search_provider": SEARCH_PROVIDER,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. バッチ検索の実行

# COMMAND ----------

all_search_results = []
total = len(target_companies)

for i in range(0, total, BATCH_SIZE):
    batch = target_companies[i : i + BATCH_SIZE]
    batch_num = i // BATCH_SIZE + 1
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} companies) ---")

    for j, company in enumerate(batch):
        company_dict = company.asDict()
        result = search_company(company_dict)
        all_search_results.append(result)

        if (j + 1) % 10 == 0:
            print(f"  Progress: {j+1}/{len(batch)}")

        time.sleep(RATE_LIMIT_DELAY)

print(f"\nTotal search results collected: {len(all_search_results)}")
results_with_data = sum(1 for r in all_search_results if r["results"])
print(f"Companies with results: {results_with_data}")
print(f"Companies without results: {len(all_search_results) - results_with_data}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 検索結果の保存（生データテーブル）

# COMMAND ----------

# フラット化してDataFrameに変換
raw_rows = []
for sr in all_search_results:
    if sr["results"]:
        for result in sr["results"]:
            raw_rows.append({
                "search_id": f"srch_{uuid.uuid4().hex[:12]}",
                "company_id": sr["company_id"],
                "company_name": sr["company_name"],
                "query_used": sr["query_used"],
                "search_provider": sr["search_provider"],
                "result_rank": result["rank"],
                "result_title": result["title"],
                "result_url": result["url"],
                "result_snippet": result["snippet"],
                "raw_response_json": json.dumps(result, ensure_ascii=False),
                "search_timestamp": sr["timestamp"],
                "batch_id": BATCH_ID,
            })
    else:
        # 検索結果なしのレコードも記録
        raw_rows.append({
            "search_id": f"srch_{uuid.uuid4().hex[:12]}",
            "company_id": sr["company_id"],
            "company_name": sr["company_name"],
            "query_used": sr["query_used"],
            "search_provider": sr["search_provider"],
            "result_rank": None,
            "result_title": None,
            "result_url": None,
            "result_snippet": None,
            "raw_response_json": None,
            "search_timestamp": sr["timestamp"],
            "batch_id": BATCH_ID,
        })

from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp

raw_df = spark.createDataFrame([Row(**r) for r in raw_rows])
raw_df = raw_df.withColumn("search_timestamp", to_timestamp("search_timestamp"))

# Deltaテーブルに追記
raw_df.write.mode("append").saveAsTable(
    "affinity_demo.company_enrichment.search_raw_results"
)

print(f"Saved {raw_df.count()} raw search result records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. 検索結果の正規化・保存

# COMMAND ----------

def classify_content(url: str, title: str) -> str:
    """URLとタイトルからコンテンツ種別を推定"""
    url_lower = url.lower()
    title_lower = title.lower()

    if any(kw in url_lower for kw in ["about", "company", "corporate", "kaisha"]):
        return "company_page"
    elif any(kw in title_lower for kw in ["プレスリリース", "press", "ニュース", "news"]):
        return "press_release"
    elif any(kw in url_lower for kw in ["prtimes", "atpress", "valuepress"]):
        return "press_release"
    elif any(kw in url_lower for kw in ["crunchbase", "initial", "entrepedia"]):
        return "startup_db"
    else:
        return "other"


def estimate_relevance(snippet: str, company_name: str) -> float:
    """スニペットと企業名から関連性スコアを推定"""
    if not snippet:
        return 0.0
    score = 0.3
    if company_name in snippet:
        score += 0.3
    keywords = ["事業", "サービス", "提供", "開発", "プラットフォーム", "ソリューション"]
    matched = sum(1 for kw in keywords if kw in snippet)
    score += min(matched * 0.1, 0.4)
    return min(score, 1.0)


normalized_rows = []
for sr in all_search_results:
    for result in sr.get("results", []):
        content_type = classify_content(result.get("url", ""), result.get("title", ""))
        relevance = estimate_relevance(result.get("snippet", ""), sr["company_name"])

        normalized_rows.append({
            "company_id": sr["company_id"],
            "company_name": sr["company_name"],
            "source_url": result.get("url", ""),
            "source_title": result.get("title", ""),
            "extracted_text": result.get("snippet", ""),
            "content_type": content_type,
            "relevance_score": relevance,
            "search_provider": sr["search_provider"],
            "batch_id": BATCH_ID,
        })

if normalized_rows:
    norm_df = spark.createDataFrame([Row(**r) for r in normalized_rows])
    norm_df.write.mode("append").saveAsTable(
        "affinity_demo.company_enrichment.search_normalized"
    )
    print(f"Saved {norm_df.count()} normalized search records")
else:
    print("No normalized records to save")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. 検索結果サマリ

# COMMAND ----------

display(spark.sql(f"""
SELECT
    search_provider,
    COUNT(DISTINCT company_id) AS companies_searched,
    COUNT(*) AS total_results,
    ROUND(AVG(CASE WHEN result_snippet IS NOT NULL THEN 1 ELSE 0 END), 2) AS hit_rate
FROM affinity_demo.company_enrichment.search_raw_results
WHERE batch_id = '{BATCH_ID}'
GROUP BY search_provider
"""))

# COMMAND ----------

# 検索結果を次のノートブックで使用するためにJSON文字列として保存
import json

# メモリ上の結果を一時テーブルに格納
search_summary = json.dumps(
    [{"company_id": r["company_id"], "company_name": r["company_name"],
      "results": r["results"]} for r in all_search_results],
    ensure_ascii=False
)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW _search_results_json AS
SELECT '{BATCH_ID}' AS batch_id, 'search_complete' AS status
""")

dbutils.notebook.exit(json.dumps({"batch_id": BATCH_ID, "companies_searched": len(all_search_results)}))
