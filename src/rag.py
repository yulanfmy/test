"""RAG chain: retrieval + generation."""

from openai import OpenAI

from src.config import CHAT_MODEL, OPENAI_API_KEY
from src.embeddings import generate_embeddings
from src.vector_store import get_zilliz_client, search_books

SYSTEM_PROMPT = """\
You are a helpful book recommendation assistant. You answer questions about books \
using the provided context from a book database.

When recommending books, include the title and a brief reason based on the description. \
If the context doesn't contain relevant books, say so honestly and suggest the user \
try a different query.

Always be concise and friendly.\
"""


def build_context(books: list[dict]) -> str:
    """Format retrieved books into a context string for the LLM."""
    parts = []
    for i, book in enumerate(books, 1):
        parts.append(f"[{i}] Title: {book['title']}\nDescription: {book['description']}")
    return "\n\n".join(parts)


def chat(
    user_message: str,
    conversation_history: list[dict],
    top_k: int = 5,
) -> tuple[str, list[dict]]:
    """Run one turn of the RAG chatbot.

    Args:
        user_message: The user's latest message.
        conversation_history: List of prior {"role": ..., "content": ...} dicts.
        top_k: Number of books to retrieve for context.

    Returns:
        A tuple of (assistant_reply, retrieved_books).
    """
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
    zilliz_client = get_zilliz_client()

    # 1. Embed the user query
    query_embedding = generate_embeddings([user_message], client=openai_client)[0]

    # 2. Retrieve relevant books
    books = search_books(query_embedding, top_k=top_k, client=zilliz_client)

    # 3. Build augmented prompt
    context = build_context(books)
    augmented_user_message = (
        f"Context (retrieved books):\n{context}\n\nUser question: {user_message}"
    )

    # 4. Build messages for the LLM
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(conversation_history)
    messages.append({"role": "user", "content": augmented_user_message})

    # 5. Generate response
    response = openai_client.chat.completions.create(
        model=CHAT_MODEL,
        messages=messages,
        temperature=0.7,
        max_tokens=1024,
    )
    assistant_reply = response.choices[0].message.content or ""

    return assistant_reply, books
