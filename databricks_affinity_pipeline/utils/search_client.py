"""
Web検索クライアント
外部の検索APIを呼び出してスタートアップ企業の事業概要を取得する
"""

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """検索結果1件を表すデータクラス"""
    title: str
    url: str
    snippet: str
    source: str  # "google", "bing", "duckduckgo"
    rank: int
    retrieved_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class CompanySearchResults:
    """1企業分の検索結果をまとめるデータクラス"""
    company_id: str
    company_name: str
    query_used: str
    results: list[SearchResult]
    search_provider: str
    search_timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    error: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["results"] = [r.to_dict() for r in self.results]
        return d


class GoogleSearchClient:
    """Google Custom Search API クライアント"""

    BASE_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(self, api_key: str, search_engine_id: str):
        self.api_key = api_key
        self.search_engine_id = search_engine_id
        self.session = requests.Session()

    def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        params = {
            "key": self.api_key,
            "cx": self.search_engine_id,
            "q": query,
            "num": min(num_results, 10),
            "lr": "lang_ja",
            "gl": "jp",
        }
        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results = []
            for i, item in enumerate(data.get("items", []), start=1):
                results.append(
                    SearchResult(
                        title=item.get("title", ""),
                        url=item.get("link", ""),
                        snippet=item.get("snippet", ""),
                        source="google",
                        rank=i,
                    )
                )
            return results
        except requests.RequestException as e:
            logger.error(f"Google Search API error: {e}")
            raise


class BingSearchClient:
    """Bing Web Search API クライアント"""

    BASE_URL = "https://api.bing.microsoft.com/v7.0/search"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Ocp-Apim-Subscription-Key": api_key})

    def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        params = {
            "q": query,
            "count": min(num_results, 50),
            "mkt": "ja-JP",
            "setLang": "ja",
        }
        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results = []
            for i, page in enumerate(
                data.get("webPages", {}).get("value", []), start=1
            ):
                results.append(
                    SearchResult(
                        title=page.get("name", ""),
                        url=page.get("url", ""),
                        snippet=page.get("snippet", ""),
                        source="bing",
                        rank=i,
                    )
                )
            return results
        except requests.RequestException as e:
            logger.error(f"Bing Search API error: {e}")
            raise


class DuckDuckGoSearchClient:
    """DuckDuckGo検索クライアント（APIキー不要のフォールバック）"""

    BASE_URL = "https://api.duckduckgo.com/"

    def __init__(self):
        self.session = requests.Session()

    def search(self, query: str, num_results: int = 5) -> list[SearchResult]:
        params = {
            "q": query,
            "format": "json",
            "no_html": 1,
            "skip_disambig": 1,
        }
        try:
            resp = self.session.get(self.BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            results = []
            rank = 1
            # Abstract (main result)
            if data.get("Abstract"):
                results.append(
                    SearchResult(
                        title=data.get("Heading", ""),
                        url=data.get("AbstractURL", ""),
                        snippet=data.get("Abstract", ""),
                        source="duckduckgo",
                        rank=rank,
                    )
                )
                rank += 1
            # Related topics
            for topic in data.get("RelatedTopics", [])[:num_results]:
                if isinstance(topic, dict) and "Text" in topic:
                    results.append(
                        SearchResult(
                            title=topic.get("Text", "")[:100],
                            url=topic.get("FirstURL", ""),
                            snippet=topic.get("Text", ""),
                            source="duckduckgo",
                            rank=rank,
                        )
                    )
                    rank += 1
            return results[:num_results]
        except requests.RequestException as e:
            logger.error(f"DuckDuckGo Search error: {e}")
            raise


class SearchOrchestrator:
    """検索オーケストレーター: 複数の検索プロバイダを管理し、バッチ検索を実行"""

    def __init__(
        self,
        provider: str = "google",
        api_key: str = "",
        search_engine_id: str = "",
        max_results: int = 5,
        rate_limit_delay: float = 1.0,
        max_retries: int = 3,
    ):
        self.max_results = max_results
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self.provider_name = provider

        if provider == "google":
            self.client = GoogleSearchClient(api_key, search_engine_id)
        elif provider == "bing":
            self.client = BingSearchClient(api_key)
        else:
            self.client = DuckDuckGoSearchClient()
            self.provider_name = "duckduckgo"

    def build_query(self, company_name: str, website_url: str = "") -> str:
        """企業名とURLから検索クエリを構築"""
        query = f"{company_name} 事業内容 会社概要"
        if website_url:
            query += f" site:{website_url}"
        return query

    def search_company(
        self, company_id: str, company_name: str, website_url: str = ""
    ) -> CompanySearchResults:
        """1企業分の検索を実行"""
        query = self.build_query(company_name, website_url)
        error = None
        results = []

        for attempt in range(self.max_retries):
            try:
                results = self.client.search(query, self.max_results)
                break
            except Exception as e:
                error = str(e)
                logger.warning(
                    f"Search attempt {attempt + 1}/{self.max_retries} "
                    f"failed for {company_name}: {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.rate_limit_delay * (attempt + 1))

        return CompanySearchResults(
            company_id=company_id,
            company_name=company_name,
            query_used=query,
            results=results,
            search_provider=self.provider_name,
            error=error if not results else None,
        )

    def search_batch(
        self, companies: list[dict]
    ) -> list[CompanySearchResults]:
        """
        複数企業のバッチ検索を実行

        Parameters
        ----------
        companies : list[dict]
            各要素は {"company_id": str, "company_name": str, "website_url": str} を持つ

        Returns
        -------
        list[CompanySearchResults]
        """
        all_results = []
        for i, company in enumerate(companies):
            logger.info(
                f"Searching [{i+1}/{len(companies)}]: {company['company_name']}"
            )
            result = self.search_company(
                company_id=company["company_id"],
                company_name=company["company_name"],
                website_url=company.get("website_url", ""),
            )
            all_results.append(result)
            # レート制限対策
            if i < len(companies) - 1:
                time.sleep(self.rate_limit_delay)
        return all_results
