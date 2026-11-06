import json
from pathlib import Path

import pytest
from airbyte_cdk.models import (
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    AirbyteStreamStatus,
    AirbyteStreamStatusTraceMessage,
    AirbyteTraceMessage,
    ConfiguredAirbyteCatalog,
    ConfiguredAirbyteStream,
    DestinationSyncMode,
    StreamDescriptor,
    SyncMode,
    TraceType,
    Type,
)

from destination_mongodb_python import DestinationMongoDB
from destination_mongodb_python.destination import SwapAbortedError


def catalog(*streams, mode=DestinationSyncMode.overwrite):
    return ConfiguredAirbyteCatalog(
        streams=[
            ConfiguredAirbyteStream(
                stream=AirbyteStream(name=name, json_schema={}, supported_sync_modes=[SyncMode.full_refresh]),
                sync_mode=SyncMode.full_refresh,
                destination_sync_mode=mode,
            )
            for name in streams
        ]
    )


def record(stream, data):
    return AirbyteMessage(type=Type.RECORD, record=AirbyteRecordMessage(stream=stream, data=data, emitted_at=0))


def stream_status(stream, status):
    return AirbyteMessage(
        type=Type.TRACE,
        trace=AirbyteTraceMessage(
            type=TraceType.STREAM_STATUS,
            emitted_at=0,
            stream_status=AirbyteStreamStatusTraceMessage(
                stream_descriptor=StreamDescriptor(name=stream), status=status
            ),
        ),
    )


def run(config, configured_catalog, messages):
    return list(DestinationMongoDB().write(config, configured_catalog, iter(messages)))


def seed(db, name, count):
    db[name].insert_many([{"cnpj": f"{i:014d}", "old": True} for i in range(count)])


def temp_collections(db):
    return [n for n in db.list_collection_names() if "_temp_" in n]


def test_overwrite_replaces_data_and_keeps_secondary_indexes(db, config):
    seed(db, "empresas", 4)
    db["empresas"].create_index("cnpj", unique=True)
    db["empresas"].create_index([("socios.cpf_socio", 1), ("razao_social", 1)])

    run(config, catalog("empresas"), [record("empresas", {"cnpj": f"{i:014d}", "new": True}) for i in range(5)])

    assert db["empresas"].count_documents({"new": True}) == 5
    assert db["empresas"].count_documents({"old": True}) == 0
    keys = {tuple(i["key"].items()): i.get("unique", False) for i in db["empresas"].list_indexes()}
    assert keys[(("cnpj", 1),)] is True
    assert (("socios.cpf_socio", 1), ("razao_social", 1)) in keys
    assert temp_collections(db) == []


def test_overwrite_keeps_text_index(db, config):
    db["orgaos"].insert_many([{"nome_ua": "Secretaria de Saude"}, {"nome_ua": "Secretaria de Fazenda"}])
    db["orgaos"].create_index([("nome_ua", "text")], default_language="portuguese")

    run(config, catalog("orgaos"), [record("orgaos", {"nome_ua": "Secretaria de Educacao"}), record("orgaos", {"nome_ua": "Secretaria de Obras"})])

    text_index = next(i for i in db["orgaos"].list_indexes() if "_fts" in i["key"])
    assert text_index["weights"] == {"nome_ua": 1}
    assert text_index["default_language"] == "portuguese"
    assert db["orgaos"].count_documents({"$text": {"$search": "educacao"}}) == 1


def test_no_records_keeps_existing_collection(db, config):
    seed(db, "empresas", 3)

    with pytest.raises(SwapAbortedError):
        run(config, catalog("empresas"), [])

    assert db["empresas"].count_documents({"old": True}) == 3
    assert temp_collections(db) == []


def test_new_data_below_ratio_keeps_existing_collection(db, config):
    seed(db, "empresas", 10)

    with pytest.raises(SwapAbortedError):
        run(config, catalog("empresas"), [record("empresas", {"cnpj": "1"}), record("empresas", {"cnpj": "2"})])

    assert db["empresas"].count_documents({"old": True}) == 10
    assert temp_collections(db) == []


def test_ratio_zero_allows_smaller_replacement(db, config):
    seed(db, "empresas", 10)

    run({**config, "min_records_ratio": 0}, catalog("empresas"), [record("empresas", {"cnpj": "1"})])

    assert db["empresas"].count_documents({}) == 1


def test_incomplete_stream_keeps_existing_collection(db, config):
    seed(db, "empresas", 2)
    messages = [record("empresas", {"cnpj": f"{i}"}) for i in range(3)]
    messages.append(stream_status("empresas", AirbyteStreamStatus.INCOMPLETE))

    with pytest.raises(SwapAbortedError):
        run(config, catalog("empresas"), messages)

    assert db["empresas"].count_documents({"old": True}) == 2
    assert temp_collections(db) == []


def test_failed_stream_does_not_block_the_others(db, config):
    seed(db, "orgaos", 2)
    seed(db, "cnae", 2)

    with pytest.raises(SwapAbortedError):
        run(config, catalog("orgaos", "cnae"), [record("cnae", {"id": i}) for i in range(3)])

    assert db["orgaos"].count_documents({"old": True}) == 2
    assert db["cnae"].count_documents({}) == 3
    assert db["cnae"].count_documents({"old": True}) == 0
    assert temp_collections(db) == []


def test_overwrite_creates_missing_collection(db, config):
    run(config, catalog("pet"), [record("pet", {"id": 1}), record("pet", {"id": 2}), record("pet", {"id": 3})])

    assert db["pet"].count_documents({}) == 3


def test_append_writes_into_existing_collection(db, config):
    seed(db, "eventos", 2)

    run(config, catalog("eventos", mode=DestinationSyncMode.append), [record("eventos", {"id": 1})])

    assert db["eventos"].count_documents({}) == 3


def test_spec_marks_connection_string_as_secret():
    spec = json.loads((Path(__file__).parents[1] / "destination_mongodb_python" / "spec.json").read_text())

    assert spec["connectionSpecification"]["properties"]["connection_string"]["airbyte_secret"] is True
