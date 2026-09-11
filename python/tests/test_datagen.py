from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "python" / "python"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from lance_context import InvalidRequestError  # noqa: E402
from lance_context.api import DatagenStore, DatagenStreamWriter  # noqa: E402

_CONTEXT = {"run_id": "run-1", "writer_epoch": "writer-1"}


def _leaf_position(name: str, index: int) -> dict[str, object]:
    return {
        "step_name": name,
        "step_kind": "leaf",
        "index": index,
        "enclosing": None,
        "selector": None,
    }


def _set_field(name: str, value: object, field_type: str = "str") -> dict[str, object]:
    return {
        "name": name,
        "field_type": field_type,
        "codec_version": 1,
        "op": "set",
        "value": {"kind": field_type, "value": value},
    }


def test_open_stream_writer_appends_and_folds(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))

    writer = store.open_stream(
        "5", run_id="run-1", writer_epoch="writer-1", query_tags={"lang": "en"}
    )
    assert isinstance(writer, DatagenStreamWriter)
    assert writer.item_id == "5"
    assert writer.attempt == 0

    checkpoint = writer.step_completed(
        _leaf_position("gen", 0), [_set_field("draft", "v1")]
    )
    store.append_checkpoint(checkpoint)
    store.append([writer.item_terminal("completed")])

    folded = store.fold_item("5")
    assert folded is not None
    assert folded["status"] == "completed"
    assert folded["fields"]["draft"] == {
        "mode": "set",
        "field_type": "str",
        "codec_version": 1,
        "value": {"kind": "str", "value": "v1"},
    }
    assert folded["query_tags"] == {"lang": "en"}


def test_resume_stream_bumps_attempt(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))
    writer = store.open_stream("5", run_id="run-1", writer_epoch="writer-1")
    store.append_checkpoint(
        writer.step_completed(_leaf_position("gen", 0), [_set_field("draft", "v1")])
    )

    resumed = store.resume_stream("5", run_id="run-1", writer_epoch="writer-2")
    assert resumed is not None
    assert resumed.attempt == 1

    store.append_checkpoint(
        resumed.step_completed(_leaf_position("gen", 0), [_set_field("draft", "v2")])
    )
    store.append([resumed.item_terminal("completed")])

    folded = store.fold_item("5")
    assert folded is not None
    assert folded["last_attempt"] == 1
    assert folded["fields"]["draft"] == {
        "mode": "set",
        "field_type": "str",
        "codec_version": 1,
        "value": {"kind": "str", "value": "v2"},
    }


def test_resume_stream_none_when_never_started(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))
    assert store.resume_stream("9", run_id="run-1", writer_epoch="writer-1") is None


def test_item_tree_links_parent_and_child(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))

    root = store.open_stream("9", run_id="run-1", writer_epoch="writer-1")
    store.append([root.item_terminal("completed")])

    child = store.open_stream(
        "9/expand:0", run_id="run-1", writer_epoch="writer-1", parent_item_id="9"
    )
    store.append([child.item_terminal("completed")])

    tree = store.item_tree("9")
    assert tree["roots"] == ["9"]
    root_node = tree["nodes"]["9"]
    assert root_node["item"]["status"] == "completed"
    assert root_node["children"] == ["9/expand:0"]
    child_node = tree["nodes"]["9/expand:0"]
    assert child_node["item"]["parent_item_id"] == "9"
    assert child_node["children"] == []


def test_load_blob_by_field_name(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))
    writer = store.open_stream("5", run_id="run-1", writer_epoch="writer-1")

    blob_field = {
        "name": "screenshot",
        "field_type": "blob",
        "codec_version": 1,
        "op": "set",
        "value": {"kind": "blob", "bytes": b"payload", "size": 7},
    }
    checkpoint = writer.step_completed(_leaf_position("shot", 0), [blob_field])
    store.append_checkpoint(checkpoint)
    store.append([writer.item_terminal("completed")])

    folded = store.fold_item("5")
    assert folded is not None
    assert store.load_blob(folded, "screenshot") == b"payload"
    # A non-blob / absent field resolves to None.
    assert store.load_blob(folded, "missing") is None


def test_item_failed_is_failure_lens_not_terminal(tmp_path: Path) -> None:
    store = DatagenStore.open(str(tmp_path / "log"))
    writer = store.open_stream("5", run_id="run-1", writer_epoch="writer-1")
    store.append(
        [writer.item_failed(_leaf_position("gen", 0), "ValueError", error_dump="boom")]
    )

    failures = store.item_failures("5")
    assert len(failures) == 1
    assert failures[0]["error_type"] == "ValueError"

    # FAILED does not terminate the item.
    folded = store.fold_item("5")
    assert folded is not None
    assert folded["status"] == "running"


@pytest.fixture(params=["embedded", "remote"])
def history_store(request: pytest.FixtureRequest, tmp_path: Path) -> DatagenStore:
    """Exercise the same public read contract through both backends."""
    if request.param == "embedded":
        return DatagenStore.open(str(tmp_path / "log"))
    return DatagenStore.connect_or_create(
        request.getfixturevalue("server"), "datagen-history"
    )


def _wait_for_events(
    store: DatagenStore, item_id: str, count: int
) -> list[dict[str, Any]]:
    deadline = time.monotonic() + 15
    while True:
        events = store.item_events(item_id)
        if len(events) == count:
            return events
        if time.monotonic() >= deadline:
            raise AssertionError(f"expected {count} events, got {events!r}")
        time.sleep(0.1)


def test_raw_history_retains_overwritten_values_and_groups_descendants(
    history_store: DatagenStore,
) -> None:
    store = history_store
    # Exercise fan-out slashes and URL delimiters without changing their identity.
    root_id = "root ?#%'"
    child_id = f"{root_id}/expand:0"
    root = store.open_stream(root_id, **_CONTEXT)
    first = root.step_completed(_leaf_position("gen", 0), [_set_field("draft", "v1")])
    store.append_checkpoint(first)
    child = store.open_stream(child_id, parent_item_id=root_id, **_CONTEXT)
    child_checkpoint = child.step_completed(
        _leaf_position("child", 0), [_set_field("draft", "child")]
    )
    store.append_checkpoint(child_checkpoint)
    last = root.step_completed(_leaf_position("edit", 1), [_set_field("draft", "v2")])
    store.append_checkpoint(last)
    store.append_checkpoint(
        last
    )  # An ambiguous-response retry must not duplicate rows.
    store.open_stream("unrelated", **_CONTEXT)

    root_events = _wait_for_events(store, root_id, 5)
    child_events = _wait_for_events(store, child_id, 3)
    assert [row["event_id"] for row in root_events[1:]] == [
        row["event_id"] for row in [*first, *last]
    ]
    assert [row["value"]["value"] for row in root_events if row["field_name"]] == [
        "v1",
        "v2",
    ]
    assert [row["event_id"] for row in child_events[1:]] == [
        row["event_id"] for row in child_checkpoint
    ]
    assert all(row["item_id"] == child_id for row in child_events)
    assert all(row["parent_item_id"] == root_id for row in child_events)
    assert store.root_events(root_id) == root_events + child_events
    assert store.item_events("missing") == []
    assert store.root_events("missing") == []
    for row in root_events[1:]:
        if row["field_name"]:
            assert row["field_type"] == "str"
            assert row["codec_version"] == 1


def test_fold_preserves_winning_codec_and_append_metadata(
    history_store: DatagenStore,
) -> None:
    store = history_store
    writer = store.open_stream("root", **_CONTEXT)
    first = {
        **_set_field("when", "2021-02-03"),
        "field_type": "date",
        "codec_version": 3,
    }
    appended = {**first, "name": "dates", "op": "append"}
    store.append_checkpoint(
        writer.step_completed(_leaf_position("first", 0), [first, appended])
    )
    _wait_for_events(store, "root", 4)
    assert store.fold_item("root")["fields"]["when"]["field_type"] == "date"
    last = {**first, "field_type": "path", "codec_version": 4, "value": first["value"]}
    store.append_checkpoint(
        writer.step_completed(_leaf_position("last", 1), [last, appended])
    )
    _wait_for_events(store, "root", 7)
    folded = store.fold_item("root")
    assert folded is not None
    assert folded["fields"]["when"] == {
        "mode": "set",
        "field_type": "path",
        "codec_version": 4,
        "value": first["value"],
    }
    assert folded["fields"]["dates"] == {
        "mode": "append",
        "field_type": "date",
        "codec_version": 3,
        "values": [first["value"], first["value"]],
    }
    tree = store.item_tree("root")
    assert tree["nodes"]["root"]["item"]["fields"] == folded["fields"]
    resumed = store.resume_stream("root", run_id="run-2", writer_epoch="writer-2")
    assert resumed is not None
    assert resumed.attempt == 1


@pytest.mark.parametrize(("next_type", "next_version"), [("path", 1), ("date", 2)])
def test_fold_rejects_mixed_append_codecs_but_raw_history_remains_readable(
    history_store: DatagenStore,
    next_type: str,
    next_version: int,
) -> None:
    store = history_store
    writer = store.open_stream("root", **_CONTEXT)
    first = {
        **_set_field("dates", "2021-02-03"),
        "op": "append",
        "field_type": "date",
    }
    store.append_checkpoint(writer.step_completed(_leaf_position("first", 0), [first]))
    store.append_checkpoint(
        writer.step_completed(
            _leaf_position("next", 1),
            [{**first, "field_type": next_type, "codec_version": next_version}],
        )
    )
    events = _wait_for_events(store, "root", 5)
    assert len(store.root_events("root")) == len(events)
    with pytest.raises(InvalidRequestError, match="changes append codec"):
        store.fold_item("root")
    with pytest.raises(InvalidRequestError, match="changes append codec"):
        store.resume_stream("root", run_id="run-2", writer_epoch="writer-2")


def test_historical_blobs_are_lazy_and_resolved_by_event_id(
    history_store: DatagenStore,
) -> None:
    store = history_store
    writer = store.open_stream("root", **_CONTEXT)
    for index, payload in enumerate([b"old-image", b"new-image"]):
        field = {
            "name": "image",
            "op": "set",
            "field_type": "image",
            "codec_version": 7,
            "value": {"kind": "blob", "bytes": payload, "size": len(payload)},
        }
        store.append_checkpoint(
            writer.step_completed(_leaf_position("capture", index), [field])
        )
    events = _wait_for_events(store, "root", 5)
    blobs = [row for row in events if row["field_name"] == "image"]
    assert len(blobs) == 2
    assert all(row["value"]["bytes"] is None for row in blobs)
    assert [store.get_blob(row["event_id"]) for row in blobs] == [
        b"old-image",
        b"new-image",
    ]
    for eager in [False, True]:
        folded = store.fold_item("root", load_blobs=eager)
        assert folded is not None
        field = folded["fields"]["image"]
        assert field["field_type"] == "image"
        assert field["codec_version"] == 7
        assert field["value"]["bytes"] == (b"new-image" if eager else None)
