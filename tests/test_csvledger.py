"""Merge policies, partitioning, the folder store and the manifest."""

import json

import pytest

from csvledger import (
    Files,
    TableSpec,
    merge_rows,
    read_table,
    table_files,
    write_manifest,
    write_rows,
)

VIEWS = TableSpec(
    "views",
    ("date", "repo", "count"),
    key=("date", "repo"),
    merge="max",
    int_columns=("count",),
    time_column="date",
)
SNAP = TableSpec(
    "referrers",
    ("fetched", "repo", "referrer", "count"),
    key=("fetched", "repo", "referrer"),
    merge="replace_group",
    group=("fetched", "repo"),
    int_columns=("count",),
)
STARS = TableSpec(
    "stars",
    ("date", "repo", "stars"),
    key=("date", "repo"),
    merge="change_only",
    entity=("repo",),
    int_columns=("stars",),
)
META = TableSpec("repos", ("repo", "desc"), key=("repo",), merge="upsert")


def _row(d, repo="a/b", count=1):
    return {"date": d, "repo": repo, "count": count}


def test_max_is_idempotent_and_order_free():
    a = [_row("2026-01-01", count=3)]
    b = [_row("2026-01-01", count=5), _row("2026-01-02")]
    ab, ba = merge_rows(VIEWS, a, b), merge_rows(VIEWS, b, a)
    assert sorted(map(str, ab)) == sorted(map(str, ba))
    assert merge_rows(VIEWS, ab, b) == ab
    assert {r["date"]: r["count"] for r in ab} == {"2026-01-01": 5, "2026-01-02": 1}


def test_replace_group_drops_stale_rows():
    old = [
        {"fetched": "d", "repo": "r", "referrer": "x", "count": 1},
        {"fetched": "d", "repo": "r", "referrer": "y", "count": 1},
    ]
    new = [{"fetched": "d", "repo": "r", "referrer": "x", "count": 2}]
    assert merge_rows(SNAP, old, new) == [
        {"fetched": "d", "repo": "r", "referrer": "x", "count": 2}
    ]


def test_change_only_keeps_changes_per_entity():
    rows = [
        {"date": "1", "repo": "a", "stars": 1},
        {"date": "2", "repo": "a", "stars": 1},
        {"date": "2", "repo": "b", "stars": 1},
        {"date": "3", "repo": "a", "stars": 2},
    ]
    kept = merge_rows(STARS, [], rows)
    assert [(r["date"], r["repo"]) for r in kept] == [("1", "a"), ("3", "a"), ("2", "b")]
    with pytest.raises(ValueError):
        TableSpec("x", ("date", "a"), key=("date", "a"), merge="change_only")


def test_upsert_new_wins():
    old = [{"repo": "a", "desc": "old"}]
    assert merge_rows(META, old, [{"repo": "a", "desc": "new"}]) == [
        {"repo": "a", "desc": "new"}
    ]


def test_files_store_partitions_by_year_and_writes_manifest(tmp_path):
    store = Files(tmp_path)
    n = write_rows(store, VIEWS, [_row("2025-12-31"), _row("2026-01-01")])
    assert n == 2 and table_files(store, VIEWS) == ["views/2025.csv", "views/2026.csv"]
    assert write_rows(store, VIEWS, [_row("2026-01-01")]) == 0  # nothing changed
    assert len(read_table(store, VIEWS, years={"2026"})) == 1
    m = write_manifest(store, {"views": VIEWS}, schema_version=1, updated="x")
    assert json.loads(store["manifest.json"]) == m
    assert m["schema_version"] == 1 and m["tables"]["views"]["files"]
    assert not list(tmp_path.rglob("*.tmp"))  # atomic writes leave no temp files
