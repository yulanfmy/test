"""Embedding generation using OpenAI."""

from openai import OpenAI

from src.config import EMBEDDING_MODEL, OPENAI_API_KEY


def get_openai_client() -> OpenAI:
    """Return an OpenAI client."""
    return OpenAI(api_key=OPENAI_API_KEY)


def generate_embeddings(texts: list[str], client: OpenAI | None = None) -> list[list[float]]:
    """Generate embeddings for a list of texts using OpenAI.

    Args:
        texts: List of text strings to embed.
        client: Optional pre-existing OpenAI client.

    Returns:
        List of embedding vectors.
    """
    if client is None:
        client = get_openai_client()

    response = client.embeddings.create(
        input=texts,
        model=EMBEDDING_MODEL,
    )
    return [item.embedding for item in response.data]
