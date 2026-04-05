# CLAUDE.md

## Project

`sstable_timeline.sh` — parses Cassandra debug logs, generates an interactive HTML timeline of SSTable lifecycles.

## Architecture

`sstable_timeline.sh` is a single self-contained file with clearly marked internal sections:

- **`SECTION: ARGUMENT PARSING & SETUP`** — `--parse-only` flag and argument handling
- **`SECTION: AWK LOG PARSER`** — `run_parser()` function wrapping the AWK block; outputs `timestamp|type|sstable|size_mb|compaction_id|keyspace.table`
- **`SECTION: HTML GENERATION`** — two heredocs (`HTML_PART1`, `HTML_PART2`) assembled with the events in between
- **`SECTION: ASSEMBLY`** — `cat $HTML_PART1 $EVENTS_FILE $HTML_PART2 > $OUTPUT`

The JS data-processing logic inside the HTML is delimited with `===BEGIN_DATA_PROCESSING===` / `===END_DATA_PROCESSING===` comments so it can be extracted and tested independently.

## `--parse-only` mode

```bash
./sstable_timeline.sh --parse-only debug.log
```

Prints a header line followed by pipe-delimited events to stdout; progress goes to stderr. Useful for debugging and is the interface used by parser tests.

```
timestamp|event_type|sstable_name|size_mb|compaction_id|keyspace.table
2026-01-16 00:00:08|flush|nb-4477-big|59.399||test_keyspace.test_table
2026-01-16 03:17:04|compaction|nb-4478-big|13840.4|06401930-...|test_keyspace.test_table
2026-01-16 03:17:04|delete|nb-4472-big|0||test_keyspace.test_table
```

## AWK parser functions

- `parse_size()` — converts GiB/MiB/KiB to MiB (sub-byte sizes return 0)
- `extract_sstable_name(path)` — extracts `nb-XXXX-big` from full path
- `extract_keyspace_table(path)` — extracts `keyspace.table` (strips UUID suffix from table dir; handles relative `./` paths)
- Three pattern blocks: `Flushed to [BigTableReader`, `Compacted.*sstables to`, `Deleting sstable:`

## Log patterns

Cassandra 4.1:
```
Flushed to [BigTableReader(path='/ks/tbl-uuid/nb-4477-big-Data.db')] ... biggest 59.399MiB
Compacted (uuid) N sstables to [/ks/tbl-uuid/nb-4478-big,] ... 13.516GiB to 13.516GiB
Deleting sstable: /ks/tbl-uuid/nb-4472-big
```

Cassandra 5.0 (UCS):
```
Flushed to [BigTableReader:big(path='/data/ks/tbl-uuid/nb-2-big-Data.db'), ...] ... biggest 114.322MiB
Compacted (uuid) N sstables to [./data/ks/tbl-uuid/nb-105-big,] ... 1.058KiB to 761B
Deleting sstable: /cassandra1/./data/data/ks/tbl-uuid/nb-101-big
```

Key differences handled: `BigTableReader:big(...)` syntax, KiB sizes, relative `./` paths in compaction/deletion, multiple paths per flush line (sharded flush), empty output list `to []` (produces no compaction event).

## Testing

Output filename defaults to input filename with `.html` extension (e.g. `foo.log` → `foo.html`). Override with a second argument.

```bash
# Cassandra 4.1 sample
./sstable_timeline.sh 4.1_debug_grep_prefixed.log
open 4.1_debug_grep_prefixed.html

# Cassandra 5.0 UCS sharded flush sample
./sstable_timeline.sh 5.0_debug_ucs_sharded_flush.log
open 5.0_debug_ucs_sharded_flush.html
```

### Automated tests

```bash
./tests/run_tests.sh
```

- **`tests/test_parser.bats`** — bats tests invoking `--parse-only` against fixture files in `tests/fixtures/`
- **`tests/test_visualization.js`** — node tests that extract the `===BEGIN/END_DATA_PROCESSING===` block from the script and run it with test data

Test fixtures (all in `tests/fixtures/`):

| Fixture | What it covers |
|---------|----------------|
| `flush_mib.log` | 4.1 flush, MiB size |
| `flush_gib.log` | flush, GiB→MiB conversion |
| `flush_5x_single.log` | 5.0 `BigTableReader:big` format, KiB→MiB conversion |
| `multi_path_flush.log` | 5.0 UCS sharded flush (8 SSTables, one log line) |
| `compaction.log` | 4.1 compaction + delete |
| `compaction_5x.log` | 5.0 compaction with relative `./` paths |
| `compaction_5x_empty.log` | 5.0 `to []` — no compaction event produced |
| `delete_preexisting.log` | delete with no prior creation |
| `delete_5x.log` | 5.0 deletion with `./` path components |
| `4.1_full.expected` | golden output of `4.1_debug_grep_prefixed.log` |
| `5.0_full.expected` | golden output of `5.0_debug_ucs_sharded_flush.log` |

## Requirements

- `gawk` (GNU AWK): `brew install gawk` / `apt-get install gawk`
- Tests: `bats-core` (`brew install bats-core`) and `node` (`brew install node`)
