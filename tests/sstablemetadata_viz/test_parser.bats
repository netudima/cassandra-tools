#!/usr/bin/env bats

SCRIPT="$BATS_TEST_DIRNAME/../../sstablemetadata_viz.sh"
FIXTURES="$BATS_TEST_DIRNAME/fixtures"
SAMPLE_50="$BATS_TEST_DIRNAME/5.0_sstablemetadata_sstable_activity.out"
SAMPLE_41="$BATS_TEST_DIRNAME/4.1_sstablemetadata_sstable_activity.out"

parse() { bash -c "'$SCRIPT' --parse-only '$1' 2>/dev/null"; }

@test "single block: name, keyspace, timestamps, tokens extracted" {
    run parse "$FIXTURES/single_block.out"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/single_block.expected"
}

@test "multiple blocks: each block produces one row" {
    run parse "$FIXTURES/multi_block.out"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/multi_block.expected"
    # header + 2 SSTables
    [ "$(echo "$output" | wc -l | tr -d ' ')" -eq 3 ]
}

@test "duplicate blocks: same SSTable path deduplicated to one row" {
    run parse "$FIXTURES/duplicate_blocks.out"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/duplicate_blocks.expected"
    # header + 1 row despite 3 identical blocks
    [ "$(echo "$output" | wc -l | tr -d ' ')" -eq 2 ]
}

@test "block without IsTransient: still emitted via END rule" {
    run parse "$FIXTURES/no_istransient.out"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/no_istransient.expected"
}

@test "--parse-only stdout contains header and data, not progress messages" {
    run parse "$FIXTURES/single_block.out"
    [ "$status" -eq 0 ]
    [[ "$output" != *"Parsing metadata file:"* ]]
    [[ "$output" == *"sstable_name|"* ]]
}

# ── Golden regression test ────────────────────────────────────────────────────

@test "golden: full 5.0 sstablemetadata sample produces expected output (regression)" {
    run parse "$SAMPLE_50"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/5.0_full.expected"
}

@test "4.1 format: timestamp before parenthesis (epoch then date) parsed correctly" {
    run parse "$FIXTURES/4.1_single_block.out"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/4.1_single_block.expected"
}

@test "golden: full 4.1 sstablemetadata sample produces expected output (regression)" {
    run parse "$SAMPLE_41"
    [ "$status" -eq 0 ]
    diff <(echo "$output") "$FIXTURES/4.1_full.expected"
}

@test "missing file exits with error" {
    run bash -c "'$SCRIPT' --parse-only /nonexistent/file.out 2>/dev/null"
    [ "$status" -ne 0 ]
}

@test "no arguments prints usage and exits with error" {
    run "$SCRIPT"
    [ "$status" -ne 0 ]
    [[ "$output" == *"Usage:"* ]]
}
