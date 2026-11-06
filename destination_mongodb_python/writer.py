#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from collections import defaultdict
from collections.abc import Mapping
from logging import getLogger
from uuid import uuid4

from pymongo import MongoClient


logger = getLogger("airbyte")


class MongoDBWriter:
    def __init__(self, client: MongoClient, database: str, batch_size: int = 1000):
        self.client = client
        self.database = database
        self.batch_size = batch_size or 1000
        self.write_buffer: list[tuple[str, Mapping]] = []

    def queue_write_operation(self, stream_name: str, data: Mapping):
        # Add _id field if not present to ensure uniqueness
        if "_id" not in data:
            data["_id"] = str(uuid4())
        
        self.write_buffer.append((stream_name, data))
        if len(self.write_buffer) == self.batch_size:
            self.flush()

    def flush(self):
        buffer_size = len(self.write_buffer)
        if buffer_size == 0:
            return
        logger.info(f"flushing {buffer_size} records")

        grouped_by_stream: defaultdict[str, list[Mapping]] = defaultdict(list)
        for stream, data in self.write_buffer:
            grouped_by_stream[stream].append(data)

        for stream, data in grouped_by_stream.items():
            collection = self.client[self.database][stream]
            try:
                # Use bulk_write for better performance
                collection.insert_many(data, ordered=False)
            except Exception as e:
                logger.error(f"Error writing to collection {stream}: {str(e)}")
                raise

        self.write_buffer.clear()
