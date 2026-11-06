import logging
import uuid
from typing import Any, Dict, Iterable, Mapping, Set

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteStreamStatus,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    TraceType,
    Type,
)
from pymongo import IndexModel, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from destination_mongodb_python.writer import MongoDBWriter

DEFAULT_MIN_RECORDS_RATIO = 0.5

# Fields of an index description that are not creation options.
_INDEX_NON_OPTION_FIELDS = {"v", "key", "ns"}


class SwapAbortedError(RuntimeError):
    """The new data for a stream was not trusted, so the existing collection was kept."""


def get_client(config: Mapping[str, Any]) -> MongoClient:
    connection_string = config.get("connection_string")
    return MongoClient(connection_string)


def copy_indexes(source: Collection, target: Collection) -> None:
    """Create on target every index of source, except the default _id index."""
    models = []
    for index in source.list_indexes():
        if index["name"] == "_id_":
            continue
        options = {k: v for k, v in index.items() if k not in _INDEX_NON_OPTION_FIELDS}
        models.append(IndexModel(list(index["key"].items()), **options))
    if models:
        logging.info(f"Creating {len(models)} indexes on {target.name} before the swap")
        target.create_indexes(models)


def swap_collection(db: Database, original_name: str, temp_name: str, min_records_ratio: float) -> None:
    """Replace original_name with temp_name only when temp_name holds trustworthy data.

    The original collection is never dropped on its own: the replacement is a single
    rename with dropTarget, done after the indexes of the original exist on the new data.
    """
    existing = set(db.list_collection_names())
    if temp_name not in existing:
        raise SwapAbortedError(
            f"No records were written for stream {original_name}; keeping collection {original_name} untouched"
        )

    new_count = db[temp_name].count_documents({})
    if new_count == 0:
        raise SwapAbortedError(f"Stream {original_name} produced 0 records; keeping collection {original_name} untouched")

    if original_name in existing:
        old_count = db[original_name].estimated_document_count()
        if min_records_ratio > 0 and old_count > 0 and new_count < old_count * min_records_ratio:
            raise SwapAbortedError(
                f"Stream {original_name} produced {new_count} records, below {min_records_ratio:.0%} of the "
                f"{old_count} currently in {original_name}; keeping collection {original_name} untouched"
            )
        copy_indexes(db[original_name], db[temp_name])

    logging.info(f"Renaming collection {temp_name} to {original_name} ({new_count} records)")
    db[temp_name].rename(original_name, dropTarget=True)


class DestinationMongoDB(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        client = get_client(config=config)
        database = config.get("database")
        db = client[database]
        min_records_ratio = config.get("min_records_ratio", DEFAULT_MIN_RECORDS_RATIO)

        # Create mapping between original and temporary collection names
        stream_mapping: Dict[str, str] = {}
        temp_collections: Set[str] = set()
        incomplete_streams: Set[str] = set()

        try:
            for configured_stream in configured_catalog.streams:
                stream_name = configured_stream.stream.name
                if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    # Create unique temporary collection name
                    temp_stream_name = f"{stream_name}_temp_{uuid.uuid4().hex[:8]}"
                    stream_mapping[stream_name] = temp_stream_name
                    temp_collections.add(temp_stream_name)
                else:
                    stream_mapping[stream_name] = stream_name

            writer = MongoDBWriter(client, database, config.get("batch_size"))
            for message in input_messages:
                if message.type == Type.STATE:
                    writer.flush()
                    yield message
                elif message.type == Type.RECORD:
                    record = message.record
                    # Use the mapped collection name (temp or original)
                    target_collection = stream_mapping[record.stream]
                    writer.queue_write_operation(target_collection, record.data)
                elif (
                    message.type == Type.TRACE
                    and message.trace.type == TraceType.STREAM_STATUS
                    and message.trace.stream_status.status == AirbyteStreamStatus.INCOMPLETE
                ):
                    incomplete_streams.add(message.trace.stream_status.stream_descriptor.name)
                else:
                    continue
            writer.flush()

            # Swap every overwrite stream that has trustworthy data; keep the others untouched
            aborted = []
            for original_name, temp_name in stream_mapping.items():
                if temp_name == original_name:
                    continue
                try:
                    if original_name in incomplete_streams:
                        raise SwapAbortedError(
                            f"Source reported stream {original_name} as incomplete; keeping collection {original_name} untouched"
                        )
                    swap_collection(db, original_name, temp_name, min_records_ratio)
                except SwapAbortedError as e:
                    logging.error(str(e))
                    aborted.append(str(e))
            if aborted:
                raise SwapAbortedError("; ".join(aborted))

        finally:
            # Clean up any remaining temporary collections
            for temp_collection in temp_collections:
                try:
                    if temp_collection in db.list_collection_names():
                        logging.info(f"Cleaning up temporary collection {temp_collection}")
                        db[temp_collection].drop()
                except Exception as e:
                    logging.warning(f"Error cleaning up temporary collection {temp_collection}: {str(e)}")

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        logger.debug("MongoDB Destination Config Check")
        try:
            client = get_client(config=config)
            database = config.get("database")
            
            # Test connection by listing databases
            client.list_database_names()
            
            # Test write access by creating a test document
            test_collection = client[database]["_airbyte_test"]
            test_collection.insert_one({"test": "connection"})
            
            # Clean up test collection
            test_collection.drop()

            status = AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            status = AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")

        return status
