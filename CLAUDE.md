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

```bash
./sstable_timeline.sh sample_data_from_debug.log
open sample_data_from_debug.html
```

## Requirements

- `gawk` (GNU AWK): `brew install gawk` / `apt-get install gawk`
