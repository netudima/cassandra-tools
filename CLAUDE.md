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
./sstable_timeline.sh --parse-only debug.log    # prints pipe-delimited events to stdout
```

Useful for debugging and is the interface used by parser tests.

## AWK parser functions

- `parse_size()` — converts GiB/MiB to MB
- `extract_sstable_name(path)` — extracts `nb-XXXX-big` from full path
- `extract_keyspace_table(path)` — extracts `keyspace.table` (strips UUID suffix from table dir)
- Three pattern blocks: `Flushed to [BigTableReader`, `Compacted.*sstables to`, `Deleting sstable:`

## Log patterns

```
Flushed to [BigTableReader(path='/ks/tbl-uuid/xx-4477-yy-Data.db')] ... biggest 59.399MiB
Compacted (uuid) N sstables to [/ks/tbl-uuid/xx-4478-yy,] ... 13.516GiB to 13.516GiB
Deleting sstable: /ks/tbl-uuid/xx-4472-yy
```

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

Test fixtures:
- `flush_mib.log`, `flush_gib.log`, `compaction.log`, `delete_preexisting.log`, `multi_path_flush.log` — targeted unit fixtures
- `4.1_full.expected` — golden output of `4.1_debug_grep_prefixed.log` (regression test)

## Requirements

- `gawk` (GNU AWK): `brew install gawk` / `apt-get install gawk`
- Tests: `bats-core` (`brew install bats-core`) and `node` (`brew install node`)
