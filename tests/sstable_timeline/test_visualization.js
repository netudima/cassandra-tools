'use strict';
// Tests for the JS data-processing logic embedded in sstable_timeline.sh.
// The ===BEGIN_DATA_PROCESSING=== / ===END_DATA_PROCESSING=== block is extracted
// from the real script and executed with node, so this always tests the live code.

const { execSync } = require('child_process');
const assert = require('assert');
const path = require('path');
const fs = require('fs');

const SCRIPT = path.resolve(__dirname, '../../sstable_timeline.sh');

// Extract the data-processing block from sstable_timeline.sh at runtime.
const dataProcessingJS = execSync(
    `awk '/===BEGIN_DATA_PROCESSING===/,/===END_DATA_PROCESSING===/' '${SCRIPT}'`
).toString();

if (!dataProcessingJS.trim()) {
    console.error('ERROR: Could not extract data-processing block from script');
    process.exit(1);
}

// Run the data-processing JS with the given pipe-delimited rawData string.
// Returns a plain object with: events, sstables, compactionRelations, timelineData.
function buildTimeline(rawData) {
    const code = `
const rawData = \`${rawData}\`;
${dataProcessingJS}
process.stdout.write(JSON.stringify({
    events: events.map(e => ({
        timestamp: e.timestamp.toISOString(),
        type: e.type,
        name: e.name,
        size: e.size,
        compactionId: e.compactionId,
        keyspace: e.keyspace
    })),
    sstables: Object.fromEntries(
        Array.from(sstables.entries()).map(([k, v]) => [k, {
            name: v.name,
            type: v.type,
            created: v.created.toISOString(),
            deleted: v.deleted ? v.deleted.toISOString() : null,
            size: v.size,
            preExisting: v.preExisting,
            stillAlive: v.stillAlive !== undefined ? v.stillAlive : null,
            compactionId: v.compactionId || null,
            keyspace: v.keyspace
        }])
    ),
    compactionRelations: Object.fromEntries(
        Array.from(compactionRelations.entries()).map(([k, v]) => [k, v])
    ),
    timelineData: timelineData.map(s => ({
        name: s.name,
        type: s.type,
        size: s.size,
        preExisting: s.preExisting,
        stillAlive: s.stillAlive
    }))
}));
`;
    const tmpFile = `/tmp/test_viz_${Date.now()}_${Math.random().toString(36).slice(2)}.js`;
    fs.writeFileSync(tmpFile, code);
    try {
        const result = execSync(`node '${tmpFile}'`, { timeout: 10000 }).toString();
        return JSON.parse(result);
    } finally {
        try { fs.unlinkSync(tmpFile); } catch (_) {}
    }
}

let passed = 0;
let failed = 0;

function test(name, fn) {
    try {
        fn();
        console.log(`  ok  ${name}`);
        passed++;
    } catch (err) {
        console.error(`  FAIL  ${name}`);
        console.error(`        ${err.message}`);
        failed++;
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test('flush event: parsed with correct fields', () => {
    const result = buildTimeline(
        '2026-01-16 00:00:00|flush|nb-100-big|59.399||ks1.tbl1'
    );
    assert.strictEqual(result.events.length, 1);
    const e = result.events[0];
    assert.strictEqual(e.type, 'flush');
    assert.strictEqual(e.name, 'nb-100-big');
    assert.strictEqual(e.size, 59.399);
    assert.strictEqual(e.compactionId, null);
    assert.strictEqual(e.keyspace, 'ks1.tbl1');
});

test('compaction event: compactionId and size parsed', () => {
    const result = buildTimeline(
        '2026-01-16 00:00:00|compaction|nb-200-big|13840.4|abc-uuid-1|ks1.tbl1'
    );
    const e = result.events[0];
    assert.strictEqual(e.type, 'compaction');
    assert.strictEqual(e.size, 13840.4);
    assert.strictEqual(e.compactionId, 'abc-uuid-1');
});

test('delete event: marks sstable as deleted', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-100-big|10||ks1.tbl1',
        '2026-01-16 01:00:00|delete|nb-100-big|0||ks1.tbl1'
    ].join('\n'));
    const s = result.sstables['nb-100-big'];
    assert.ok(s.deleted, 'deleted should be set');
    assert.strictEqual(s.stillAlive, false);
});

test('sstable still alive when not deleted', () => {
    const result = buildTimeline(
        '2026-01-16 00:00:00|flush|nb-100-big|10||ks1.tbl1'
    );
    const s = result.sstables['nb-100-big'];
    assert.strictEqual(s.stillAlive, true);
    assert.ok(s.deleted, 'deleted is set to max event time for still-alive');
});

test('pre-existing SSTable: delete without prior creation', () => {
    const result = buildTimeline(
        '2026-01-16 01:00:00|delete|nb-old-big|0||ks1.tbl1'
    );
    const s = result.sstables['nb-old-big'];
    assert.strictEqual(s.preExisting, true);
    assert.strictEqual(s.size, null);
    assert.ok(s.deleted);
});

test('compaction relations: output knows its inputs', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-1-big|10||ks1.tbl1',
        '2026-01-16 00:01:00|flush|nb-2-big|20||ks1.tbl1',
        '2026-01-16 00:02:00|compaction|nb-3-big|30|test-uuid-1|ks1.tbl1',
        '2026-01-16 00:02:05|delete|nb-1-big|0||ks1.tbl1',
        '2026-01-16 00:02:05|delete|nb-2-big|0||ks1.tbl1'
    ].join('\n'));
    const rel = result.compactionRelations['nb-3-big'];
    assert.ok(rel, 'compaction output should have a relation entry');
    assert.ok(rel.inputs.includes('nb-1-big'), 'nb-1-big should be an input');
    assert.ok(rel.inputs.includes('nb-2-big'), 'nb-2-big should be an input');
});

test('compaction relations: input knows its output and siblings', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-1-big|10||ks1.tbl1',
        '2026-01-16 00:01:00|flush|nb-2-big|20||ks1.tbl1',
        '2026-01-16 00:02:00|compaction|nb-3-big|30|test-uuid-1|ks1.tbl1',
        '2026-01-16 00:02:05|delete|nb-1-big|0||ks1.tbl1',
        '2026-01-16 00:02:05|delete|nb-2-big|0||ks1.tbl1'
    ].join('\n'));
    const rel1 = result.compactionRelations['nb-1-big'];
    assert.ok(rel1.outputs.includes('nb-3-big'), 'output should be nb-3-big');
    assert.ok(rel1.siblingInputs.includes('nb-2-big'), 'sibling should be nb-2-big');
});

test('sort: ascending by size, pre-existing at bottom', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-big-big|1000||ks1.tbl1',
        '2026-01-16 00:00:00|flush|nb-small-big|10||ks1.tbl1',
        '2026-01-16 00:00:01|delete|nb-pre-big|0||ks1.tbl1'
    ].join('\n'));
    const td = result.timelineData;
    assert.strictEqual(td[0].name, 'nb-small-big');
    assert.strictEqual(td[1].name, 'nb-big-big');
    assert.strictEqual(td[2].name, 'nb-pre-big');
    assert.strictEqual(td[2].preExisting, true);
});

test('shared flush: multiple SSTables from one flush appear as independent entries', () => {
    // Simulates 5.0 UCS sharded flush: one flush produces N SSTables, all same timestamp
    const result = buildTimeline([
        '2026-04-01 17:12:43|flush|nb-2-big|114.322||easy_cass_stress1.random_access',
        '2026-04-01 17:12:43|flush|nb-3-big|114.322||easy_cass_stress1.random_access',
        '2026-04-01 17:12:43|flush|nb-4-big|114.322||easy_cass_stress1.random_access'
    ].join('\n'));
    assert.strictEqual(result.timelineData.length, 3, 'all 3 SSTables tracked independently');
    const names = result.timelineData.map(s => s.name);
    assert.ok(names.includes('nb-2-big'));
    assert.ok(names.includes('nb-3-big'));
    assert.ok(names.includes('nb-4-big'));
    // All are flush type, none pre-existing
    result.timelineData.forEach(s => {
        assert.strictEqual(s.type, 'flush');
        assert.strictEqual(s.preExisting, false);
    });
});

test('compaction with no deletions: no compaction relations built', () => {
    // Compaction event exists but no deletions follow — no relation should be created
    const result = buildTimeline(
        '2026-01-16 00:02:00|compaction|nb-3-big|30|test-uuid-1|ks1.tbl1'
    );
    assert.strictEqual(Object.keys(result.compactionRelations).length, 0);
    assert.strictEqual(result.sstables['nb-3-big'].type, 'compaction');
    assert.strictEqual(result.sstables['nb-3-big'].stillAlive, true);
});

test('flush then compaction then deletion: full lifecycle tracked', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-1-big|100||ks1.tbl1',
        '2026-01-16 01:00:00|compaction|nb-2-big|200|uuid-1|ks1.tbl1',
        '2026-01-16 01:00:05|delete|nb-1-big|0||ks1.tbl1',
        '2026-01-16 02:00:00|delete|nb-2-big|0||ks1.tbl1'
    ].join('\n'));
    // nb-1-big: flushed, then deleted as compaction input
    assert.strictEqual(result.sstables['nb-1-big'].type, 'flush');
    assert.ok(result.sstables['nb-1-big'].deleted);
    assert.strictEqual(result.sstables['nb-1-big'].stillAlive, false);
    // nb-2-big: compaction output, later deleted
    assert.strictEqual(result.sstables['nb-2-big'].type, 'compaction');
    assert.strictEqual(result.sstables['nb-2-big'].stillAlive, false);
    // compaction relation: nb-2-big → input nb-1-big
    const rel = result.compactionRelations['nb-2-big'];
    assert.ok(rel.inputs.includes('nb-1-big'));
});

test('multiple keyspaces: each SSTable gets correct keyspace', () => {
    const result = buildTimeline([
        '2026-01-16 00:00:00|flush|nb-1-big|10||ks1.tbl1',
        '2026-01-16 00:00:00|flush|nb-2-big|20||ks2.tbl2'
    ].join('\n'));
    assert.strictEqual(result.sstables['nb-1-big'].keyspace, 'ks1.tbl1');
    assert.strictEqual(result.sstables['nb-2-big'].keyspace, 'ks2.tbl2');
});

// ─── Summary ─────────────────────────────────────────────────────────────────

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
