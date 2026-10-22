"""
Unit tests for DELETERECORD routing through target_tools / StreamTracker.

These verify that a DELETERECORD Singer message is buffered and dispatched to the
target's delete_records(), that a stream's deletes are flushed before its record
batches (so a delete + re-insert of the same key keeps the new rows), that deletes for un-synced
streams are still routed, and that a malformed DELETERECORD raises.

No live Postgres needed: a fake target records the calls it receives.
"""
import json

import pytest

from target_postgres import target_tools


class FakeTarget:
    IDENTIFIER_FIELD_LENGTH = 63
    DELETE_BATCH_SIZE = 1000

    def __init__(self):
        self.calls = []

    def write_batch(self, stream_buffer):
        self.calls.append(("write_batch", stream_buffer.stream, stream_buffer.count))
        return None

    def activate_version(self, stream_buffer, version):
        self.calls.append(("activate_version", stream_buffer.stream, version))
        return None

    def delete_records(self, stream, records):
        self.calls.append(("delete_records", stream, [dict(r) for r in records]))
        return len(records)


SCHEMA = {
    "type": "SCHEMA",
    "stream": "accounts",
    "schema": {"type": "object", "properties": {"id": {"type": ["integer"]}}},
    "key_properties": ["id"],
}

CONFIG = {"disable_collection": True}


def _line(d):
    return json.dumps(d)


def _delete_calls(target):
    return [c for c in target.calls if c[0] == "delete_records"]


# WHY: core contract — a DELETERECORD reaches the target as delete_records(stream, [pk]).
def test_delete_record_routed_to_delete_records():
    target = FakeTarget()
    lines = [
        _line(SCHEMA),
        _line({"type": "RECORD", "stream": "accounts", "record": {"id": 1}}),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 2}}),
    ]

    target_tools.stream_to_target(lines, target, config=CONFIG)

    assert _delete_calls(target) == [("delete_records", "accounts", [{"id": 2}])]


# WHY: deletes flush before record batches (PQ-4461), so a delete followed by a
#      re-insert of the same key in one run keeps the re-inserted row.
def test_deletes_flushed_before_records():
    target = FakeTarget()
    lines = [
        _line(SCHEMA),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 1}}),
        _line({"type": "RECORD", "stream": "accounts", "record": {"id": 1}}),
    ]

    target_tools.stream_to_target(lines, target, config=CONFIG)

    kinds = [c[0] for c in target.calls]
    assert kinds.index("delete_records") < kinds.index("write_batch")


# WHY: when a stream's delete buffer fills before its record buffer, the stream's
#      buffered records are written in the same flush, so the flush watermark (and
#      the STATE it releases) never covers records that were not written yet.
def test_full_delete_buffer_also_flushes_that_streams_records():
    target = FakeTarget()
    target.DELETE_BATCH_SIZE = 2
    lines = [
        _line(SCHEMA),
        _line({"type": "RECORD", "stream": "accounts", "record": {"id": 10}}),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 1}}),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 2}}),
    ]

    target_tools.stream_to_target(lines, target, config=CONFIG)

    assert target.calls[:2] == [
        ("delete_records", "accounts", [{"id": 1}, {"id": 2}]),
        ("write_batch", "accounts", 1),
    ]


# WHY: a delete can target a table that had no SCHEMA/RECORD this run (e.g. a
#      dedicated delete-sync run) and must still be routed.
def test_delete_for_unsynced_stream_is_routed():
    target = FakeTarget()
    lines = [
        _line({"type": "DELETERECORD", "stream": "ghost", "record": {"id": 9}}),
    ]

    target_tools.stream_to_target(lines, target, config=CONFIG)

    assert ("delete_records", "ghost", [{"id": 9}]) in target.calls


# WHY: multiple deletes for a stream are batched into a single delete_records call.
def test_multiple_deletes_batched_per_stream():
    target = FakeTarget()
    lines = [
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 1}}),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 2}}),
        _line({"type": "DELETERECORD", "stream": "accounts", "record": {"id": 3}}),
    ]

    target_tools.stream_to_target(lines, target, config=CONFIG)

    assert _delete_calls(target) == [
        ("delete_records", "accounts", [{"id": 1}, {"id": 2}, {"id": 3}])
    ]


# WHY: a DELETERECORD without a `record` is malformed and must raise.
def test_delete_record_missing_record_raises():
    target = FakeTarget()
    lines = [_line({"type": "DELETERECORD", "stream": "accounts"})]

    with pytest.raises(Exception):
        target_tools.stream_to_target(lines, target, config=CONFIG)


# WHY: a DELETERECORD without a `stream` is malformed and must raise.
def test_delete_record_missing_stream_raises():
    target = FakeTarget()
    lines = [_line({"type": "DELETERECORD", "record": {"id": 1}})]

    with pytest.raises(Exception):
        target_tools.stream_to_target(lines, target, config=CONFIG)
