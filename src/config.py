"""Configuration for the RAG book chatbot."""

import os

from dotenv import load_dotenv

load_dotenv()

# Zilliz Cloud
ZILLIZ_URI = os.getenv("ZILLIZ_URI", "")
ZILLIZ_API_KEY = os.getenv("ZILLIZ_API_KEY", "")

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Collection
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "book_descriptions")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "1536"))
CHAT_MODEL = os.getenv("CHAT_MODEL", "gpt-4o-mini")

# Ingestion
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
