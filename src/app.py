"""Streamlit chat UI for the RAG book chatbot."""

import sys
from pathlib import Path

# Ensure the project root is on sys.path so 'src' is importable
# regardless of the working directory (e.g. Databricks Apps runtime).
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import streamlit as st

from src.config import OPENAI_API_KEY, ZILLIZ_API_KEY, ZILLIZ_URI
from src.rag import chat

st.set_page_config(page_title="Book Chatbot", page_icon="\U0001f4da", layout="centered")

st.title("\U0001f4da Book Recommendation Chatbot")
st.caption("Ask me about books! Powered by RAG with Zilliz + OpenAI.")

# --- Credential check ---
missing = []
if not ZILLIZ_URI:
    missing.append("ZILLIZ_URI")
if not ZILLIZ_API_KEY:
    missing.append("ZILLIZ_API_KEY")
if not OPENAI_API_KEY:
    missing.append("OPENAI_API_KEY")

if missing:
    st.error(
        f"Missing environment variables: {', '.join(missing)}. "
        "Copy `.env.example` to `.env` and fill in your credentials."
    )
    st.stop()

# --- Session state ---
if "messages" not in st.session_state:
    st.session_state.messages = []
if "history" not in st.session_state:
    st.session_state.history = []

# --- Render chat history ---
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("books"):
            with st.expander("Retrieved books"):
                for book in msg["books"]:
                    st.markdown(
                        f"**{book['title']}** (score: {book['score']:.3f})\n\n"
                        f"{book['description'][:200]}..."
                    )

# --- Chat input ---
if user_input := st.chat_input("Ask about a book..."):
    # Show user message
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("Searching books..."):
            reply, books = chat(
                user_message=user_input,
                conversation_history=st.session_state.history,
            )
        st.markdown(reply)
        if books:
            with st.expander("Retrieved books"):
                for book in books:
                    st.markdown(
                        f"**{book['title']}** (score: {book['score']:.3f})\n\n"
                        f"{book['description'][:200]}..."
                    )

    # Update state
    st.session_state.messages.append({"role": "assistant", "content": reply, "books": books})
    st.session_state.history.append({"role": "user", "content": user_input})
    st.session_state.history.append({"role": "assistant", "content": reply})

    # Keep conversation history manageable (last 10 turns)
    if len(st.session_state.history) > 20:
        st.session_state.history = st.session_state.history[-20:]
