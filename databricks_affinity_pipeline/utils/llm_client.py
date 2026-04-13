"""
LLMクライアント
Databricks Foundation Model API または OpenAI API を使って事業概要を要約する
"""

import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """あなたはスタートアップ企業の事業概要を要約するアシスタントです。
与えられたWeb検索結果を元に、以下のフォーマットで簡潔な事業概要を日本語で作成してください。

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


@dataclass
class SummaryResult:
    """LLMによる要約結果"""
    company_id: str
    company_name: str
    summary: str
    confidence_score: float  # 0.0-1.0
    model_used: str
    provider: str
    tokens_used: int
    generated_at: str = ""
    error: Optional[str] = None

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)


def _format_search_results(results: list[dict]) -> str:
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


def _estimate_confidence(summary: str, search_results: list[dict]) -> float:
    """要約の信頼度スコアを簡易推定"""
    if not summary or summary == "情報不足":
        return 0.0

    score = 0.5  # base score

    # 検索結果の数に応じてスコア加算
    n_results = len(search_results)
    if n_results >= 3:
        score += 0.2
    elif n_results >= 1:
        score += 0.1

    # 要約の文字数チェック
    length = len(summary)
    if 50 <= length <= 500:
        score += 0.2
    elif 20 <= length < 50:
        score += 0.1

    # キーワードの存在チェック
    keywords = ["提供", "サービス", "開発", "事業", "プラットフォーム", "ソリューション"]
    matched = sum(1 for kw in keywords if kw in summary)
    score += min(matched * 0.05, 0.1)

    return min(score, 1.0)


class DatabricksLLMClient:
    """Databricks Foundation Model API クライアント"""

    def __init__(
        self,
        workspace_url: str,
        token: str,
        model: str = "databricks-meta-llama-3-1-70b-instruct",
    ):
        self.base_url = workspace_url.rstrip("/")
        self.token = token
        self.model = model
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        )

    def generate_summary(
        self,
        company_name: str,
        search_results: list[dict],
        max_tokens: int = 500,
        temperature: float = 0.3,
    ) -> tuple[str, int]:
        """事業概要を生成"""
        user_prompt = USER_PROMPT_TEMPLATE.format(
            company_name=company_name,
            search_results=_format_search_results(search_results),
        )

        payload = {
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        endpoint = (
            f"{self.base_url}/serving-endpoints/{self.model}/invocations"
        )

        resp = self.session.post(endpoint, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        content = data["choices"][0]["message"]["content"].strip()
        tokens = data.get("usage", {}).get("total_tokens", 0)
        return content, tokens


class OpenAILLMClient:
    """OpenAI API クライアント（フォールバック用）"""

    BASE_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        )

    def generate_summary(
        self,
        company_name: str,
        search_results: list[dict],
        max_tokens: int = 500,
        temperature: float = 0.3,
    ) -> tuple[str, int]:
        """事業概要を生成"""
        user_prompt = USER_PROMPT_TEMPLATE.format(
            company_name=company_name,
            search_results=_format_search_results(search_results),
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "max_tokens": max_tokens,
            "temperature": temperature,
        }

        resp = self.session.post(self.BASE_URL, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        content = data["choices"][0]["message"]["content"].strip()
        tokens = data.get("usage", {}).get("total_tokens", 0)
        return content, tokens


class SummaryOrchestrator:
    """要約オーケストレーター: LLMを使って事業概要を生成"""

    def __init__(
        self,
        provider: str = "databricks",
        databricks_workspace_url: str = "",
        databricks_token: str = "",
        databricks_model: str = "databricks-meta-llama-3-1-70b-instruct",
        openai_api_key: str = "",
        openai_model: str = "gpt-4o-mini",
        max_tokens: int = 500,
        temperature: float = 0.3,
        batch_size: int = 20,
    ):
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.batch_size = batch_size
        self.provider = provider

        if provider == "databricks":
            self.client = DatabricksLLMClient(
                workspace_url=databricks_workspace_url,
                token=databricks_token,
                model=databricks_model,
            )
            self.model_name = databricks_model
        else:
            self.client = OpenAILLMClient(
                api_key=openai_api_key, model=openai_model
            )
            self.model_name = openai_model

    def summarize_company(
        self, company_id: str, company_name: str, search_results: list[dict]
    ) -> SummaryResult:
        """1企業分の事業概要を生成"""
        try:
            summary, tokens = self.client.generate_summary(
                company_name=company_name,
                search_results=search_results,
                max_tokens=self.max_tokens,
                temperature=self.temperature,
            )
            confidence = _estimate_confidence(summary, search_results)
            return SummaryResult(
                company_id=company_id,
                company_name=company_name,
                summary=summary,
                confidence_score=confidence,
                model_used=self.model_name,
                provider=self.provider,
                tokens_used=tokens,
            )
        except Exception as e:
            logger.error(f"LLM error for {company_name}: {e}")
            return SummaryResult(
                company_id=company_id,
                company_name=company_name,
                summary="",
                confidence_score=0.0,
                model_used=self.model_name,
                provider=self.provider,
                tokens_used=0,
                error=str(e),
            )

    def summarize_batch(
        self, companies_with_results: list[dict]
    ) -> list[SummaryResult]:
        """
        複数企業のバッチ要約を実行

        Parameters
        ----------
        companies_with_results : list[dict]
            各要素は {
                "company_id": str,
                "company_name": str,
                "search_results": list[dict]
            } を持つ
        """
        all_summaries = []
        for i, item in enumerate(companies_with_results):
            logger.info(
                f"Summarizing [{i+1}/{len(companies_with_results)}]: "
                f"{item['company_name']}"
            )
            result = self.summarize_company(
                company_id=item["company_id"],
                company_name=item["company_name"],
                search_results=item.get("search_results", []),
            )
            all_summaries.append(result)
            # API負荷軽減のためスリープ
            if i < len(companies_with_results) - 1:
                time.sleep(0.5)
        return all_summaries
