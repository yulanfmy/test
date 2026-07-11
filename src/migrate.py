"""Migrate the book collection from a source Zilliz instance to the target."""

import os

from src.config import COLLECTION_NAME, EMBEDDING_DIM
from src.vector_store import ZillizClient

DEFAULT_MIGRATE_BATCH_SIZE = 1000


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def main() -> None:
    """Copy the source collection into the target collection."""
    source_uri = _get_env("SOURCE_ZILLIZ_URI")
    source_api_key = _get_env("SOURCE_ZILLIZ_API_KEY")
    target_uri = _get_env("ZILLIZ_URI")
    target_api_key = _get_env("ZILLIZ_API_KEY")
    source_collection = _get_env("SOURCE_COLLECTION_NAME", COLLECTION_NAME)
    target_collection = _get_env("COLLECTION_NAME", COLLECTION_NAME)
    batch_size = int(_get_env("MIGRATE_BATCH_SIZE", str(DEFAULT_MIGRATE_BATCH_SIZE)))
    drop_target = _get_env("DROP_TARGET", "false").lower() in ("1", "true", "yes")
    resume_id = _get_env("MIGRATE_RESUME_ID")

    if not source_uri or not source_api_key:
        raise RuntimeError("SOURCE_ZILLIZ_URI and SOURCE_ZILLIZ_API_KEY must be set")
    if not target_uri or not target_api_key:
        raise RuntimeError("ZILLIZ_URI and ZILLIZ_API_KEY must be set")

    source = ZillizClient(uri=source_uri, api_key=source_api_key, timeout=180)
    target = ZillizClient(uri=target_uri, api_key=target_api_key, timeout=180)

    if not source.has_collection(source_collection):
        raise RuntimeError(f"Source collection '{source_collection}' not found")

    if target.has_collection(target_collection):
        if resume_id:
            print(f"Resuming migration into target collection '{target_collection}'...")
        elif drop_target:
            print(f"Dropping target collection '{target_collection}'...")
            target.drop_collection(target_collection)
        else:
            raise RuntimeError(
                f"Target collection '{target_collection}' already exists. "
                "Set DROP_TARGET=1 to overwrite or MIGRATE_RESUME_ID to resume."
            )

    if not target.has_collection(target_collection):
        print(f"Creating target collection '{target_collection}' (dim={EMBEDDING_DIM})...")
        target.create_collection(target_collection, EMBEDDING_DIM)

    if resume_id:
        print(
            f"Resuming migration from {source_collection} to {target_collection} "
            f"starting after source id {resume_id} (batch={batch_size})..."
        )
    else:
        print(
            f"Starting migration from {source_collection} to {target_collection} "
            f"(batch={batch_size})..."
        )

    migrated = 0
    if resume_id:
        previous_count = target.query(
            collection_name=target_collection,
            filter="id >= 0",
            output_fields=["count(*)"],
            limit=0,
        )[0]["count(*)"]
        migrated = previous_count
    last_id = int(resume_id) if resume_id else -1
    output_fields = ["id", "title", "description", "embedding"]

    while True:
        filter_expr = f"id > {last_id}" if last_id >= 0 else "id >= 0"
        rows = source.query(
            collection_name=source_collection,
            filter=filter_expr,
            output_fields=output_fields,
            limit=batch_size,
            offset=0,
            order_by=["id:asc"],
        )
        if not rows:
            break

        records = [
            {
                "title": row["title"],
                "description": row["description"],
                "embedding": row["embedding"],
            }
            for row in rows
        ]
        result = target.insert(target_collection, records)
        inserted = result.get("data", {}).get("insertCount", len(records))
        migrated += inserted
        last_id = max(row["id"] for row in rows)

        print(f"  Migrated {migrated} rows (last id: {last_id})")
        if len(rows) < batch_size:
            break

    print(f"Migration complete. Total rows migrated: {migrated}")


if __name__ == "__main__":
    main()
