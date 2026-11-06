import logging
import uuid
from typing import Any, Dict, Iterable, Mapping, Set

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type,
)
from pymongo import MongoClient

from destination_mongodb_python.writer import MongoDBWriter


def get_client(config: Mapping[str, Any]) -> MongoClient:
    connection_string = config.get("connection_string")
    return MongoClient(connection_string)


class DestinationMongoDB(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:
        client = get_client(config=config)
        database = config.get("database")

        # Create mapping between original and temporary collection names
        stream_mapping: Dict[str, str] = {}
        temp_collections: Set[str] = set()

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
                else:
                    continue
            writer.flush()

            # After successful write, rename the collections
            for original_name, temp_name in stream_mapping.items():
                if temp_name != original_name:  # Only rename if it's a temporary collection
                    try:
                        logging.info(f"Renaming collection {temp_name} to {original_name}")
                        # Drop the original collection if it exists
                        if original_name in client[database].list_collection_names():
                            client[database][original_name].drop()
                        # Rename the temporary collection to the original name
                        client[database][temp_name].rename(original_name)
                    except Exception as e:
                        logging.error(f"Error renaming collection {temp_name} to {original_name}: {str(e)}")
                        raise

        finally:
            # Clean up any remaining temporary collections
            for temp_collection in temp_collections:
                try:
                    if temp_collection in client[database].list_collection_names():
                        logging.info(f"Cleaning up temporary collection {temp_collection}")
                        client[database][temp_collection].drop()
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
