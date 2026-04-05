# CLAUDE.md

## Project

`sstable_timeline.sh` — parses Cassandra debug logs, generates an interactive HTML timeline of SSTable lifecycles.

## Architecture

- **AWK parser** extracts events from the log, outputs `timestamp|type|sstable|size_mb|compaction_id|keyspace.table`
- **HTML/JS** (embedded in the script between `HTML_PART1` and `HTML_PART2`) renders a Canvas-based timeline
- Output is a single self-contained HTML file

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

## Requirements

- `gawk` (GNU AWK): `brew install gawk` / `apt-get install gawk`
