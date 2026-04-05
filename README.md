# Cassandra Tools

Tools for analyzing and visualizing Apache Cassandra operations.

## Tools

- [SSTable Timeline Generator](#sstable-timeline-generator) — visualize SSTable lifecycles from Cassandra debug logs
- [SSTable Metadata Visualizer](#sstable-metadata-visualizer) — visualize timestamp and token ranges from `sstablemetadata` output

---

## SSTable Timeline Generator

Visualize the lifecycle of Cassandra SSTables from creation to deletion.

### Features

- **Interactive Timeline Visualization** - See when SSTables are created and deleted over time
- **Color-Coded by Type** - Distinguish between flush operations (green), compactions (blue), and pre-existing files (gray)
- **Compaction Relationship Highlighting** - Click any SSTable to highlight related SSTables in the same compaction
- **Pre-existing SSTable Detection** - Automatically identifies SSTables that existed before the log period
- **Still-Active SSTable Display** - Shows SSTables that exist at the end of the log period with a marker (►)
- **Sticky Time Axis** - Time labels remain visible when scrolling vertically through many SSTables
- **Mouse Zoom** - Click and drag to select a time range and zoom in for detailed inspection
- **Keyspace.Table Labels** - Each bar displays `keyspace.table/sstable-id` for quick identification
- **Detailed Hover Information** - View SSTable size, timestamps, lifetime, and compaction relationships
- **One-Click Copy** - Click any SSTable bar to copy its name to clipboard
- **Filterable Views** - Toggle between flush, compaction, and pre-existing operations
- **Size-Based Sorting** - Y-axis sorted by file size for better pattern recognition
- **Short-lived SSTable Visibility** - Very short-lived SSTables automatically widened for visibility

### Quick Start

```bash
./sstable_timeline.sh your_cassandra_log.log output.html
open output.html
```

### Example Output

![SSTable Timeline Screenshot](sstable_timeline_example.jpg)

The generated HTML shows:
- **X-axis**: Timeline of events
- **Y-axis**: SSTables sorted by size (smallest to largest, "?" for unknown sizes)
- **Bars**: Each bar represents one SSTable's lifetime
  - Start = creation time (flush or compaction), or first log timestamp for pre-existing files
  - End = deletion time, or last event timestamp for still-active SSTables
  - Color = operation type:
    - **Green** = flush
    - **Blue** = compaction
    - **Gray** = pre-existing (created before log period, size unknown and shown as "?")
  - **Amber arrow marker (►)** at the end = SSTable still active at end of log period
  - Bar label shows `keyspace.table/nb-XXXX-big` for immediate identification
  - Minimum width ensures very short-lived SSTables are visible
- **Y-axis**: Keyspace.table name for each SSTable row

**Interacting with the Timeline:**
- **Hover** over any bar to see detailed information including:
  - SSTable name, keyspace.table, type, size, timestamps, and lifetime
  - Number of input/output SSTables and co-inputs in compaction relationships
- **Click** on any bar to:
  - Copy the SSTable name to your clipboard
  - Highlight all related SSTables involved in the same compaction:
    - **Click on compaction output** (blue bar): highlights all input SSTables
    - **Click on compaction input**: highlights the output AND all other inputs (co-inputs)
  - Selected SSTable shows with amber border
  - Related SSTables show with orange border
  - Other SSTables are dimmed for clarity
- **Click again** on the same bar to deselect and clear highlighting
- **Click and drag** on the timeline to select a time range and zoom in
- **Reset Zoom** button returns to the full timeline view
- Use **checkboxes** at the top to filter by flush, compaction, or pre-existing operations
- **Scroll** horizontally and vertically to navigate large timelines
  - Time axis remains sticky at the top while scrolling vertically

### Usage

```bash
./sstable_timeline.sh [--parse-only] <logfile> [output.html]
```

**Arguments:**
- `--parse-only` - (Optional) Print pipe-delimited parsed events to stdout instead of generating HTML; useful for debugging and scripting
- `logfile` - Path to Cassandra debug log file
- `output.html` - (Optional) Output HTML filename (default: input filename with `.html` extension)

**Examples:**

```bash
# Basic usage with default output
./sstable_timeline.sh debug.log

# Specify output file
./sstable_timeline.sh debug.log my_timeline.html

# Inspect parsed events (header + pipe-delimited rows)
./sstable_timeline.sh --parse-only debug.log | head

# Process a specific date's logs
grep "2026-01-16" debug.log > filtered.log
./sstable_timeline.sh filtered.log

# Extract only relevant lines recursively across multiple log files (much smaller file, faster processing)
egrep -r "(Flushed to)|(Partition merge counts were)|(Deleting sstable)" /path/to/logs/ > filtered.log
./sstable_timeline.sh filtered.log
```

### Requirements

- `bash` (4.0+)
- `gawk` (GNU AWK)
- Modern web browser (Chrome, Firefox, Safari, Edge)

**Installing gawk:**

```bash
# macOS
brew install gawk

# Ubuntu/Debian
sudo apt-get install gawk

# CentOS/RHEL
sudo yum install gawk
```

### Supported Cassandra Versions

| Version | Flush | Sharded flush (UCS) | Compaction | Deletion |
|---------|-------|---------------------|------------|----------|
| 4.1     | ✓     | N/A                 | ✓          | ✓        |
| 5.0     | ✓     | ✓                   | ✓          | ✓        |

Sizes in GiB, MiB, and KiB are all converted to MiB.

**Cassandra 4.1** — fully supported. Each flush produces one SSTable.

**Cassandra 5.0** — fully supported, including the Unified Compaction Strategy (UCS):
- Sharded flushes produce multiple SSTables per flush event — each is tracked independently
- Compaction and deletion paths may be relative (`./data/data/...`) — correctly handled
- UCS compactions with no output SSTables (`to []`) produce no compaction event; the SSTable deletions that follow will appear as pre-existing (gray) bars

Earlier versions (3.x, 4.0) are likely to work if the log format matches the patterns above, but have not been tested.

### Log Format

The script parses Cassandra debug logs for three types of events.

1. **Flush Events** (MemTable → SSTable):
```
# Cassandra 4.1
DEBUG [MemtableFlushWriter:1] 2026-01-16 00:00:08,377 - Flushed to [BigTableReader(path='/path/nb-4477-big-Data.db')] (1 sstables, 59.399MiB), biggest 59.399MiB

# Cassandra 5.0 (single SSTable)
DEBUG [MemtableFlushWriter:2] 2026-04-01 16:52:25,293 - Flushed to [BigTableReader:big(path='/path/nb-102-big-Data.db')] (1 sstables, 5.975KiB), biggest 5.975KiB

# Cassandra 5.0 UCS sharded flush (multiple SSTables per flush)
DEBUG [MemtableFlushWriter:6] 2026-04-01 17:12:43,392 - Flushed to [BigTableReader:big(path='/path/nb-2-big-Data.db'), BigTableReader:big(path='/path/nb-3-big-Data.db'), ...] (8 sstables, 911.193MiB), biggest 114.322MiB
```

2. **Compaction Events** (SSTable merges):
```
# Cassandra 4.1
INFO  [CompactionExecutor:1] 2026-01-16 03:17:04,340 - Compacted (uuid) 1 sstables to [/path/nb-4478-big,] to level=0.  13.516GiB to 13.516GiB

# Cassandra 5.0
INFO  [CompactionExecutor:1] 2026-04-01 16:52:25,843 - Compacted (uuid) 4 sstables to [./data/path/nb-105-big,] to level=0.  1.058KiB to 761B
```

3. **Deletion Events**:
```
# Cassandra 4.1
INFO  [NonPeriodicTasks:1] 2026-01-16 03:17:04,345 SSTable.java:111 - Deleting sstable: /path/nb-4472-big

# Cassandra 5.0
INFO  [NonPeriodicTasks:1] 2026-04-01 16:52:25,900 BigFormat.java:231 - Deleting sstable: /cassandra1/./data/data/path/nb-101-big
```

### Understanding the Visualization

The timeline helps identify:
- **Write patterns**: Frequency and size of flushes (use zoom to inspect bursts)
- **Compaction efficiency**: How quickly SSTables are merged
- **Compaction relationships**: Click any SSTable to see which files were merged together
  - **Click compaction output** (blue bars): Highlights all input SSTables that were merged
  - **Click compaction input**: Highlights the output SSTable AND all other inputs (co-inputs) in the same compaction
  - Helps understand merge trees, compaction behavior, and which SSTables were processed together
  - "Co-inputs" shown in tooltip indicate other inputs merged in the same compaction operation
- **SSTable lifetime**: How long files exist before compaction (zoom for precision)
- **Size distribution**: Relative sizes of SSTables over time
- **Compaction strategy behavior**: Patterns of merges and deletions
- **Pre-existing SSTables**: Files that existed before the log period (gray bars with "?" size)
  - Helps understand what was inherited from previous operations
  - Shows when old files finally get compacted away
  - Size is unknown since creation event was not captured
  - Can be highlighted as compaction inputs when clicked or when related SSTables are selected
- **Short-lived issues**: Minimum bar width ensures even millisecond-lived SSTables are visible

### Troubleshooting

**No events extracted**:
- Verify log file contains DEBUG level logs
- Check that log format matches expected patterns
- Ensure MemtableFlushWriter and CompactionExecutor messages are present

**Empty timeline**:
- SSTables need at least a creation (flush/compaction) or deletion event to appear
- SSTables still active at the end of the log are shown with an amber (►) marker
- Pre-existing SSTables (deleted but not created in log) appear as gray bars

**No compaction events in Cassandra 5.0 UCS logs**:
- UCS compactions that produce no output SSTables (`to []`) are skipped — this is expected
- Use `--parse-only` to verify which events were extracted

**Performance with large logs**:
- Consider filtering logs by date range first
- Use `grep` to extract relevant time periods

---

## SSTable Metadata Visualizer

Visualize timestamp ranges and token ranges for Cassandra SSTables from `sstablemetadata` output.

### Features

- **Five-Tab HTML Visualization** — Timestamp Ranges, Token Ranges, Density, Tombstones, TTL
- **Timestamp Ranges Tab** — horizontal bar per SSTable showing min/max timestamp extent
- **Token Ranges Tab** — horizontal bar per SSTable plotted against the full Murmur3 ring (−2⁶³ to 2⁶³−1)
- **Density Tab** — estimated bytes per token fraction on a log₂ scale, colored by UCS level
- **Tombstones Tab** — droppable tombstone fraction (0–1) per SSTable; color ranges from green (near 0) through yellow and orange to red (near 1); drop time p50 in tooltip
- **TTL Tab** — min/max TTL range bars per SSTable with human-readable durations; "No TTL" shown for SSTables without TTL data
- **Hover Tooltips** — exact min/max timestamps, token ring coverage, density, tombstone fraction, and TTL details per SSTable
- **Mouse Zoom** — click and drag to zoom into any range; independent zoom state per tab; Reset Zoom to return
- **Deduplication** — if the same SSTable path appears multiple times in the input, it is shown once
- **Dark Theme** — consistent styling with SSTable Timeline Generator

### Quick Start

```bash
# Capture sstablemetadata output for multiple SSTables
sstablemetadata /path/to/data/keyspace/table-uuid/*.db > metadata.out

# Or with nodetool (Cassandra 4.x+)
nodetool sstablemetadata /path/to/data/keyspace/table-uuid/*.db > metadata.out

./sstablemetadata_viz.sh metadata.out
open metadata.html
```

### Usage

```bash
./sstablemetadata_viz.sh [--parse-only] <metadata-file> [output.html]
```

**Arguments:**
- `--parse-only` — (Optional) Print pipe-delimited parsed rows to stdout instead of generating HTML; useful for debugging and scripting
- `metadata-file` — Path to concatenated `sstablemetadata` output
- `output.html` — (Optional) Output HTML filename (default: input filename with `.html` extension)

**Examples:**

```bash
# Basic usage
./sstablemetadata_viz.sh metadata.out

# Specify output file
./sstablemetadata_viz.sh metadata.out my_viz.html

# Inspect parsed rows (header + pipe-delimited rows)
./sstablemetadata_viz.sh --parse-only metadata.out
```

### Supported Cassandra Versions

Both Cassandra 4.1 and 5.0 `sstablemetadata` output formats are supported. The timestamp field layout differs between versions:

```
# Cassandra 4.1 — epoch µs before the parenthesised date
Minimum timestamp: 1775400530638000 (04/05/2026 10:48:50)

# Cassandra 5.0 — epoch µs inside the parentheses
Minimum timestamp: 04/05/2026 03:38:44 (1775374724542000)
```

### Input Format

The script reads the output of `sstablemetadata` (one or more SSTables concatenated). Each SSTable block starts with `SSTable: <path>`. The following fields are extracted:

```
SSTable: /cassandra/data/keyspace/table-uuid/nb-13-big
Minimum timestamp: 04/05/2026 03:38:44 (1775374724542000)
Maximum timestamp: 04/05/2026 03:38:45 (1775374725524002)
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

All other lines in each block are ignored. The same SSTable path appearing multiple times (e.g. from running `sstablemetadata` on overlapping file lists) is deduplicated — only the first occurrence is kept.

### Requirements

- `bash` (4.0+)
- `gawk` (GNU AWK)
- Modern web browser (Chrome, Firefox, Safari, Edge)

See [Installing gawk](#installing-gawk) above.

## License

Apache License 2.0
