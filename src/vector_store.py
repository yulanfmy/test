"""Zilliz vector store operations."""

from pymilvus import CollectionSchema, DataType, FieldSchema, MilvusClient

from src.config import COLLECTION_NAME, EMBEDDING_DIM, ZILLIZ_API_KEY, ZILLIZ_URI


def get_milvus_client() -> MilvusClient:
    """Return a MilvusClient connected to Zilliz Cloud."""
    return MilvusClient(uri=ZILLIZ_URI, token=ZILLIZ_API_KEY)


def create_collection(client: MilvusClient | None = None) -> None:
    """Create the book descriptions collection in Zilliz if it doesn't exist.

    Schema:
        - id: int64 primary key (auto-generated)
        - title: book title (varchar, max 512 chars)
        - description: book description (varchar, max 4096 chars)
        - embedding: vector field for semantic search
    """
    if client is None:
        client = get_milvus_client()

    if client.has_collection(COLLECTION_NAME):
        print(f"Collection '{COLLECTION_NAME}' already exists, skipping creation.")
        return

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=4096),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
    ]
    schema = CollectionSchema(fields=fields, description="Book titles and descriptions")

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="embedding",
        index_type="AUTOINDEX",
        metric_type="COSINE",
    )

    client.create_collection(
        collection_name=COLLECTION_NAME,
        schema=schema,
        index_params=index_params,
    )
    print(f"Collection '{COLLECTION_NAME}' created successfully.")


def insert_batch(
    titles: list[str],
    descriptions: list[str],
    embeddings: list[list[float]],
    client: MilvusClient | None = None,
) -> int:
    """Insert a batch of book data into the collection.

    Returns:
        Number of rows inserted.
    """
    if client is None:
        client = get_milvus_client()

    data = [
        {"title": t, "description": d, "embedding": e}
        for t, d, e in zip(titles, descriptions, embeddings)
    ]
    result = client.insert(collection_name=COLLECTION_NAME, data=data)
    return result["insert_count"]


def search_books(
    query_embedding: list[float],
    top_k: int = 5,
    client: MilvusClient | None = None,
) -> list[dict]:
    """Search for similar books by embedding.

    Returns:
        List of dicts with 'title', 'description', and 'distance' keys.
    """
    if client is None:
        client = get_milvus_client()

    results = client.search(
        collection_name=COLLECTION_NAME,
        data=[query_embedding],
        limit=top_k,
        output_fields=["title", "description"],
    )

    books = []
    for hit in results[0]:
        books.append(
            {
                "title": hit["entity"]["title"],
                "description": hit["entity"]["description"],
                "score": hit["distance"],
            }
        )
    return books
