import os
import uuid

import pytest
from pymongo import MongoClient


@pytest.fixture(scope="session")
def mongo_uri() -> str:
    uri = os.environ.get("MONGO_TEST_URI")
    if not uri:
        pytest.fail("MONGO_TEST_URI must point to a MongoDB instance used only for tests")
    MongoClient(uri, serverSelectionTimeoutMS=5000).admin.command("ping")
    return uri


@pytest.fixture
def db_name(mongo_uri):
    name = f"test_{uuid.uuid4().hex[:12]}"
    yield name
    MongoClient(mongo_uri).drop_database(name)


@pytest.fixture
def db(mongo_uri, db_name):
    return MongoClient(mongo_uri)[db_name]


@pytest.fixture
def config(mongo_uri, db_name):
    return {"connection_string": mongo_uri, "database": db_name, "batch_size": 2}
