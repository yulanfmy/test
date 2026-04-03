"""Ingest the HuggingFace book dataset into Zilliz."""

from datasets import load_dataset

from src.config import BATCH_SIZE
from src.embeddings import generate_embeddings, get_openai_client
from src.vector_store import create_collection, get_milvus_client, insert_batch


def main() -> None:
    """Load the train split, generate embeddings, and insert into Zilliz."""
    print("Loading dataset: Skelebor/book_titles_and_descriptions_en_clean (train split)...")
    dataset = load_dataset("Skelebor/book_titles_and_descriptions_en_clean", split="train")
    print(f"Loaded {len(dataset)} records.")

    openai_client = get_openai_client()
    milvus_client = get_milvus_client()

    print("Creating collection (if needed)...")
    create_collection(client=milvus_client)

    total_inserted = 0

    for start in range(0, len(dataset), BATCH_SIZE):
        end = min(start + BATCH_SIZE, len(dataset))
        batch = dataset[start:end]

        titles = batch["title"]
        descriptions = batch["description"]

        # Combine title + description for richer embeddings
        texts_to_embed = [f"Title: {t}\nDescription: {d}" for t, d in zip(titles, descriptions)]

        # Truncate very long texts to stay within token limits
        texts_to_embed = [text[:8000] for text in texts_to_embed]

        embeddings = generate_embeddings(texts_to_embed, client=openai_client)

        count = insert_batch(titles, descriptions, embeddings, client=milvus_client)
        total_inserted += count

        print(f"  Inserted batch {start}-{end} ({count} rows, total: {total_inserted})")

    print(f"Ingestion complete. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    main()
