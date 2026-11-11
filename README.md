# csvledger

Long-form CSV tables that merge idempotently: a git-friendly history store with no dependencies.

You collect numbers on a schedule (traffic, downloads, prices), each run sees an overlapping window, and you want one growing history in plain files: re-runs harmless, the order of runs irrelevant, small diffs, past years never rewritten. `csvledger` is that store.

```python
from csvledger import TableSpec, Files, write_rows, read_table

views = TableSpec(
    "views", ("date", "repo", "count"),
    key=("date", "repo"), merge="max", int_columns=("count",), time_column="date",
)
store = Files("history")                      # or any MutableMapping[str, str]
write_rows(store, views, [{"date": "2026-01-01", "repo": "a/b", "count": 3}])
write_rows(store, views, [{"date": "2026-01-01", "repo": "a/b", "count": 2}])  # no change
read_table(store, views)   # [{'date': '2026-01-01', 'repo': 'a/b', 'count': 3}]
```

This writes `history/views/2026.csv`, sorted by date, so each run appends at the end of the current year's file.

## Merge policies

| `merge=` | per key | for |
|---|---|---|
| `max` | integer fields keep the larger value | daily counts seen by overlapping windows (a partial day only grows) |
| `replace_group` | rows of a `group` (e.g. one repo, one fetch date) replace that group's earlier rows | top-N snapshots |
| `change_only` | a row is kept only when its values differ from the previous row of its `entity` (key = entity + one date column) | slowly changing values (stars, versions) |
| `upsert` | the new row wins | current metadata |

All four are idempotent: merging the same rows again changes nothing.

## The store

Any `MutableMapping[str, str]` (relative path to text). `Files(root)` is a folder with atomic writes; a [dol](https://github.com/i2mint/dol) store puts the same tables in S3 or a database. `write_manifest(store, tables, **extra)` writes a `manifest.json` listing every table's files and columns, so a static dashboard can discover the data in one request.

Used by [ghtraffic](https://github.com/thorwhalen/ghtraffic) (GitHub traffic) and [pkgreach](https://github.com/thorwhalen/pkgreach) (package downloads), where it was extracted from.
