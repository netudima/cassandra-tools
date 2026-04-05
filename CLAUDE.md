# CLAUDE.md

## Project

Two tools, each a single self-contained script at the repo root:

- **`sstable_timeline.sh`** — parses Cassandra debug logs, generates an interactive HTML timeline of SSTable lifecycles.
- **`sstablemetadata_viz.sh`** — parses `sstablemetadata` output, generates a five-tab HTML visualization (Timestamp Ranges, Token Ranges, Density, Tombstones, TTL).

Tests and sample data live under `tests/<tool-name>/`.

## Architecture

Both scripts share the same internal structure:

```
script.sh
├── SECTION: ARGUMENT PARSING & SETUP   (--parse-only flag, output filename derivation)
├── SECTION: AWK PARSER                 (run_parser() function)
├── SECTION: HTML GENERATION            (HTML_HEAD heredoc + HTML_TAIL heredoc)
└── SECTION: ASSEMBLY                   (cat HEAD EVENTS TAIL > OUTPUT)
```

The JS data-processing block inside each HTML heredoc is delimited with `===BEGIN_DATA_PROCESSING===` / `===END_DATA_PROCESSING===` comments so it can be extracted and tested independently by the node test suite.

### sstable_timeline.sh

- **`SECTION: AWK LOG PARSER`** — `run_parser()` outputs `timestamp|event_type|sstable_name|size_mb|compaction_id|keyspace.table`
- **`SECTION: HTML GENERATION`** — heredocs `HTML_PART1` / `HTML_PART2` assembled around the events file

### sstablemetadata_viz.sh

- **`SECTION: AWK PARSER`** — `run_parser()` outputs 13 fields: `sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token|compression_ratio|cardinality|partition_size_p50|droppable_tombstones|tombstone_drop_p50_s|ttl_min_s|ttl_max_s`
- **`SECTION: HTML GENERATION`** — heredocs `HTML_HEAD` / `HTML_TAIL`; five-tab Canvas visualization (Timestamp Ranges, Token Ranges, Density, Tombstones, TTL)

## `--parse-only` mode

Both scripts support `--parse-only`. Progress messages go to stderr; only the header and pipe-delimited data rows go to stdout.

**sstable_timeline.sh:**
```bash
./sstable_timeline.sh --parse-only debug.log
```
```
timestamp|event_type|sstable_name|size_mb|compaction_id|keyspace.table
2026-01-16 00:00:08|flush|nb-4477-big|59.399||test_keyspace.test_table
2026-01-16 03:17:04|compaction|nb-4478-big|13840.4|06401930-...|test_keyspace.test_table
2026-01-16 03:17:04|delete|nb-4472-big|0||test_keyspace.test_table
```

**sstablemetadata_viz.sh:**
```bash
./sstablemetadata_viz.sh --parse-only metadata.out
```
```
sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token|compression_ratio|cardinality|partition_size_p50|droppable_tombstones|tombstone_drop_p50_s|ttl_min_s|ttl_max_s
nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|-5519576429900224076|8615509011068470516|0.38903394255874674|16|50|1.0|1996099046|0|0
```

## AWK parser functions

### sstable_timeline.sh

- `parse_size()` — converts GiB/MiB/KiB to MiB (sub-byte sizes return 0)
- `extract_sstable_name(path)` — extracts `nb-XXXX-big` from full path
- `extract_keyspace_table(path)` — extracts `keyspace.table` (strips UUID suffix from table dir; handles relative `./` paths)
- Three pattern blocks: `Flushed to [BigTableReader`, `Compacted.*sstables to`, `Deleting sstable:`

### sstablemetadata_viz.sh

- `extract_sstable_name(path)` — last path component
- `extract_keyspace_table(path)` — same UUID-stripping logic as sstable_timeline.sh
- State machine: `SSTable:` resets current block; `Minimum/Maximum timestamp:` extracts µs epoch — tries `(13+ digits)` first (5.0 format), falls back to `space + 13+ digits` (4.1 format); `First/Last token:` extracts numeric prefix; unified `cur_section` + `in_section_percentiles` captures p50 from `Estimated tombstone drop times:` and `Partition Size:` histogram blocks; `TTL min/max:` extracts first integer; `IsTransient:` emits the row if not already seen (deduplication via `seen[]` array); `END` rule emits the last block if it had no `IsTransient:` line

## Log / input patterns

### sstable_timeline.sh — Cassandra debug log

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

### sstablemetadata_viz.sh — sstablemetadata output

Cassandra 4.1 — epoch µs before the parenthesised date:
```
Minimum timestamp: 1775400530638000 (04/05/2026 10:48:50)
```

Cassandra 5.0 — epoch µs inside the parentheses:
```
Minimum timestamp: 04/05/2026 03:38:44 (1775374724542000)
```

Other extracted fields (same format in both versions):
```
SSTable: /cassandra/data/keyspace/table-uuid/nb-13-big
First token: -5519576429900224076 (keyspace:table:9)
Last token:  8615509011068470516 (keyspace:table:10)
Compression ratio: 0.389
TTL min: 0
TTL max: 0
Estimated droppable tombstones: 1.0
Estimated tombstone drop times:
   Percentiles
   50th      1996099046 (04/02/2033 19:57:26)
Partition Size:
   Percentiles
   50th      50 (50 B)
Estimated cardinality: 16
IsTransient: false
```

Token values are 64-bit signed integers (Murmur3 range: −2⁶³ to 2⁶³−1). Stored as exact strings for tooltips; `Number()` used for canvas rendering (precision loss at this scale is acceptable for pixel-level display).

## Testing

Output filename defaults to input filename with `.html` extension (e.g. `foo.log` → `foo.html`). Override with a second argument.

```bash
# Cassandra 4.1 sample
./sstable_timeline.sh tests/sstable_timeline/4.1_debug_grep_prefixed.log
open 4.1_debug_grep_prefixed.html

# Cassandra 5.0 UCS sharded flush sample
./sstable_timeline.sh tests/sstable_timeline/5.0_debug_ucs_sharded_flush.log
open 5.0_debug_ucs_sharded_flush.html

# sstablemetadata 5.0 sample
./sstablemetadata_viz.sh tests/sstablemetadata_viz/5.0_sstablemetadata_sstable_activity.out
open 5.0_sstablemetadata_sstable_activity.html

# sstablemetadata 4.1 sample
./sstablemetadata_viz.sh tests/sstablemetadata_viz/4.1_sstablemetadata_sstable_activity.out
open 4.1_sstablemetadata_sstable_activity.html
```

### Automated tests

```bash
./tests/run_tests.sh                          # all tools
./tests/sstable_timeline/run_tests.sh         # sstable_timeline only
./tests/sstablemetadata_viz/run_tests.sh      # sstablemetadata_viz only
```

Each tool's test directory contains:
- `test_parser.bats` — bats tests invoking `--parse-only` against fixture files
- `test_visualization.js` — node tests extracting the `===BEGIN/END_DATA_PROCESSING===` block from the script and running it with synthetic data

### sstable_timeline fixtures (`tests/sstable_timeline/fixtures/`)

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

### sstablemetadata_viz fixtures (`tests/sstablemetadata_viz/fixtures/`)

| Fixture | What it covers |
|---------|----------------|
| `single_block.out` | one SSTable block — name, keyspace, timestamps, tokens, all 13 fields |
| `multi_block.out` | two distinct SSTable blocks |
| `duplicate_blocks.out` | same SSTable repeated 3× — deduplication to one row |
| `no_istransient.out` | block with no `IsTransient:` line — emitted by AWK `END` rule |
| `4.1_single_block.out` | 4.1-format timestamps (`epoch (date)`) — verifies fallback parsing |
| `4.1_full.expected` | golden output of `4.1_sstablemetadata_sstable_activity.out` |
| `5.0_full.expected` | golden output of `5.0_sstablemetadata_sstable_activity.out` |

## Requirements

- `gawk` (GNU AWK): `brew install gawk` / `apt-get install gawk`
- Tests: `bats-core` (`brew install bats-core`) and `node` (`brew install node`)
