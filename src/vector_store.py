"""Zilliz vector store operations via REST API."""

import httpx

from src.config import COLLECTION_NAME, EMBEDDING_DIM, ZILLIZ_API_KEY, ZILLIZ_URI


class ZillizClient:
    """Thin REST client for Zilliz Cloud serverless."""

    def __init__(self, uri: str = ZILLIZ_URI, api_key: str = ZILLIZ_API_KEY) -> None:
        self.base_url = uri.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self._http = httpx.Client(base_url=self.base_url, headers=self.headers, timeout=60)

    def _post(self, path: str, payload: dict) -> dict:
        resp = self._http.post(path, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Zilliz API error: {data}")
        return data

    def list_collections(self) -> list[str]:
        data = self._post("/v2/vectordb/collections/list", {})
        return data.get("data", [])

    def has_collection(self, name: str) -> bool:
        data = self._post("/v2/vectordb/collections/has", {"collectionName": name})
        return data.get("data", {}).get("has", False)

    def create_collection(self, name: str, dim: int) -> None:
        schema = {
            "autoId": True,
            "enableDynamicField": False,
            "fields": [
                {
                    "fieldName": "id",
                    "dataType": "Int64",
                    "isPrimary": True,
                },
                {
                    "fieldName": "title",
                    "dataType": "VarChar",
                    "elementTypeParams": {"max_length": "512"},
                },
                {
                    "fieldName": "description",
                    "dataType": "VarChar",
                    "elementTypeParams": {"max_length": "4096"},
                },
                {
                    "fieldName": "embedding",
                    "dataType": "FloatVector",
                    "elementTypeParams": {"dim": str(dim)},
                },
            ],
        }
        index_params = [
            {
                "fieldName": "embedding",
                "indexName": "embedding_index",
                "metricType": "COSINE",
                "params": {"index_type": "AUTOINDEX"},
            }
        ]
        self._post(
            "/v2/vectordb/collections/create",
            {
                "collectionName": name,
                "schema": schema,
                "indexParams": index_params,
            },
        )

    def insert(self, collection_name: str, data: list[dict]) -> dict:
        return self._post(
            "/v2/vectordb/entities/insert",
            {"collectionName": collection_name, "data": data},
        )

    def search(
        self,
        collection_name: str,
        vector: list[float],
        limit: int = 5,
        output_fields: list[str] | None = None,
    ) -> list[dict]:
        payload: dict = {
            "collectionName": collection_name,
            "data": [vector],
            "limit": limit,
            "outputFields": output_fields or [],
        }
        data = self._post("/v2/vectordb/entities/search", payload)
        return data.get("data", [])


def get_zilliz_client() -> ZillizClient:
    """Return a ZillizClient connected to Zilliz Cloud."""
    return ZillizClient()


def create_collection(client: ZillizClient | None = None) -> None:
    """Create the book descriptions collection in Zilliz if it doesn't exist."""
    if client is None:
        client = get_zilliz_client()

    if client.has_collection(COLLECTION_NAME):
        print(f"Collection '{COLLECTION_NAME}' already exists, skipping creation.")
        return

    client.create_collection(COLLECTION_NAME, EMBEDDING_DIM)
    print(f"Collection '{COLLECTION_NAME}' created successfully.")


def insert_batch(
    titles: list[str],
    descriptions: list[str],
    embeddings: list[list[float]],
    client: ZillizClient | None = None,
) -> int:
    """Insert a batch of book data into the collection.

    Returns:
        Number of rows inserted.
    """
    if client is None:
        client = get_zilliz_client()

    data = [
        {"title": t, "description": d, "embedding": e}
        for t, d, e in zip(titles, descriptions, embeddings)
    ]
    result = client.insert(COLLECTION_NAME, data)
    return result.get("data", {}).get("insertCount", len(data))


def search_books(
    query_embedding: list[float],
    top_k: int = 5,
    client: ZillizClient | None = None,
) -> list[dict]:
    """Search for similar books by embedding.

    Returns:
        List of dicts with 'title', 'description', and 'score' keys.
    """
    if client is None:
        client = get_zilliz_client()

    results = client.search(
        collection_name=COLLECTION_NAME,
        vector=query_embedding,
        limit=top_k,
        output_fields=["title", "description"],
    )

    books = []
    for hit in results:
        books.append(
            {
                "title": hit.get("title", ""),
                "description": hit.get("description", ""),
                "score": hit.get("distance", 0.0),
            }
        )
    return books
