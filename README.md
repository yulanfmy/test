# RAG Book Chatbot

A Retrieval-Augmented Generation (RAG) chatbot for book recommendations, powered by [Zilliz](https://zilliz.com/) vector database and OpenAI.

## Overview

This chatbot uses the [Skelebor/book_titles_and_descriptions_en_clean](https://huggingface.co/datasets/Skelebor/book_titles_and_descriptions_en_clean) dataset (train split) to answer questions about books and provide recommendations. It embeds book titles and descriptions into a Zilliz Cloud vector store and uses semantic search to retrieve relevant books before generating responses with an LLM.

## Architecture

```
User Query
    │
    ▼
┌──────────────┐
│  Streamlit   │  ◄── Chat UI
│    App       │
└──────┬───────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐
│   OpenAI     │────▶│   Zilliz     │
│  Embeddings  │     │  Vector DB   │
└──────┬───────┘     └──────┬───────┘
       │                    │
       ▼                    ▼
┌──────────────────────────────────┐
│     OpenAI Chat (GPT-4o-mini)    │
│  + Retrieved book context        │
└──────────────────────────────────┘
```

## Setup

### 1. Install dependencies

```bash
pip install -e .
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials:
# - ZILLIZ_URI and ZILLIZ_API_KEY from Zilliz Cloud
# - OPENAI_API_KEY from OpenAI
```

### 3. Ingest data

```bash
python -m src.ingest
```

This loads the HuggingFace dataset, generates embeddings via OpenAI, and stores them in Zilliz.

### 4. Run the chatbot

```bash
streamlit run src/app.py
```

## Project Structure

```
src/
├── config.py        # Environment and settings
├── embeddings.py    # OpenAI embedding generation
├── vector_store.py  # Zilliz collection management and search
├── rag.py           # RAG chain (retrieve + generate)
├── ingest.py        # Dataset ingestion pipeline
└── app.py           # Streamlit chat interface
```
