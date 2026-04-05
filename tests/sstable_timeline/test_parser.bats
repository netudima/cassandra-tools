#!/usr/bin/env bats

SCRIPT="$BATS_TEST_DIRNAME/../../sstable_timeline.sh"
FIXTURES="$BATS_TEST_DIRNAME/fixtures"
LOG_4_1="$BATS_TEST_DIRNAME/4.1_debug_grep_prefixed.log"
LOG_5_0="$BATS_TEST_DIRNAME/5.0_debug_ucs_sharded_flush.log"

parse() { bash -c "'$SCRIPT' --parse-only '$1' 2>/dev/null"; }

@test "flush: MiB size and keyspace.table extraction" {
    run parse "$FIXTURES/flush_mib.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/flush_mib.expected"
}

@test "flush: GiB size is converted to MiB" {
    run parse "$FIXTURES/flush_gib.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/flush_gib.expected"
}

@test "compaction: UUID extracted, GiB output size, plus delete event" {
    run parse "$FIXTURES/compaction.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/compaction.expected"
}

@test "delete: pre-existing SSTable (delete with no prior creation)" {
    run parse "$FIXTURES/delete_preexisting.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/delete_preexisting.expected"
}

@test "multi-path flush: UCS sharded flush produces one event per SSTable" {
    run parse "$FIXTURES/multi_path_flush.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/multi_path_flush.expected"
}

# ── Cassandra 5.0 format ─────────────────────────────────────────────────────

@test "5.0 flush: single SSTable, BigTableReader:big format, KiB size converted to MiB" {
    run parse "$FIXTURES/flush_5x_single.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/flush_5x_single.expected"
}

@test "5.0 flush shared: UCS sharded flush — one event per SSTable (same as multi_path_flush)" {
    run parse "$FIXTURES/multi_path_flush.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/multi_path_flush.expected"
    # Verify correct count: header + 8 SSTables from one flush line
    [ "$(echo "$output" | wc -l | tr -d ' ')" -eq 9 ]
}

@test "5.0 compaction: relative path (./data/data/...) correctly extracts keyspace and SSTable" {
    run parse "$FIXTURES/compaction_5x.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/compaction_5x.expected"
}

@test "5.0 compaction with empty output list (to []) produces no compaction event" {
    run parse "$FIXTURES/compaction_5x_empty.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/compaction_5x_empty.expected"
    # Only the header line — no data events
    [ "$(echo "$output" | wc -l | tr -d ' ')" -eq 1 ]
}

@test "5.0 deletion: path with ./ components correctly extracts keyspace and SSTable" {
    run parse "$FIXTURES/delete_5x.log"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/delete_5x.expected"
}

# ── Golden regression tests ───────────────────────────────────────────────────

@test "golden: full 4.1 log produces expected output (regression)" {
    run parse "$LOG_4_1"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/4.1_full.expected"
}

@test "golden: full 5.0 UCS log produces expected output (regression)" {
    run parse "$LOG_5_0"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/5.0_full.expected"
}

@test "--parse-only stdout contains only data lines, not progress messages" {
    run parse "$FIXTURES/flush_mib.log"
    [ "$status" -eq 0 ]
    [[ "$output" != *"Parsing log file:"* ]]
    [[ "$output" == *"|flush|"* ]]
}

@test "missing log file exits with error" {
    run bash -c "'$SCRIPT' --parse-only /nonexistent/file.log 2>/dev/null"
    [ "$status" -ne 0 ]
}

@test "no arguments prints usage and exits with error" {
    run "$SCRIPT"
    [ "$status" -ne 0 ]
    [[ "$output" == *"Usage:"* ]]
}
