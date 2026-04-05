'use strict';
// Tests for the JS data-processing logic embedded in sstablemetadata_viz.sh.
// The ===BEGIN_DATA_PROCESSING=== / ===END_DATA_PROCESSING=== block is extracted
// from the real script and executed with node, so this always tests the live code.

const { execSync } = require('child_process');
const assert = require('assert');
const path = require('path');
const fs = require('fs');

const SCRIPT = path.resolve(__dirname, '../../sstablemetadata_viz.sh');

// Extract the data-processing block from sstablemetadata_viz.sh at runtime.
const dataProcessingJS = execSync(
    `awk '/===BEGIN_DATA_PROCESSING===/,/===END_DATA_PROCESSING===/' '${SCRIPT}'`
).toString();

if (!dataProcessingJS.trim()) {
    console.error('ERROR: Could not extract data-processing block from script');
    process.exit(1);
}

// Run the data-processing JS with the given pipe-delimited rawData string.
// Returns the parsed sstables array.
function parseMetadata(rawData) {
    const code = `
const rawData = \`${rawData}\`;
${dataProcessingJS}
process.stdout.write(JSON.stringify(sstables));
`;
    const tmpFile = `/tmp/test_viz_meta_${Date.now()}_${Math.random().toString(36).slice(2)}.js`;
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

test('single row: all fields parsed correctly', () => {
    const result = parseMetadata(
        'nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|-5519576429900224076|8615509011068470516'
    );
    assert.strictEqual(result.length, 1);
    const s = result[0];
    assert.strictEqual(s.name, 'nb-13-big');
    assert.strictEqual(s.keyspace, 'system.sstable_activity_v2');
    assert.strictEqual(s.minTs, 1775374724542000);
    assert.strictEqual(s.maxTs, 1775374725524002);
    assert.ok(Number.isFinite(s.firstToken));
    assert.ok(Number.isFinite(s.lastToken));
});

test('header line filtered out', () => {
    const result = parseMetadata([
        'sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token',
        'nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|-5519576429900224076|8615509011068470516'
    ].join('\n'));
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].name, 'nb-13-big');
});

test('empty lines filtered out', () => {
    const result = parseMetadata([
        '',
        'nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|-5519576429900224076|8615509011068470516',
        ''
    ].join('\n'));
    assert.strictEqual(result.length, 1);
});

test('minTs and maxTs parsed as integers', () => {
    const result = parseMetadata(
        'nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|0|0'
    );
    assert.strictEqual(typeof result[0].minTs, 'number');
    assert.strictEqual(typeof result[0].maxTs, 'number');
    assert.strictEqual(result[0].minTs, 1775374724542000);
    assert.strictEqual(result[0].maxTs, 1775374725524002);
});

test('negative first token parsed as negative number', () => {
    const result = parseMetadata(
        'nb-13-big|system.sstable_activity_v2|1000|2000|-5519576429900224076|8615509011068470516'
    );
    assert.ok(result[0].firstToken < 0, 'firstToken should be negative');
});

test('firstTokenStr and lastTokenStr preserve exact string values', () => {
    const result = parseMetadata(
        'nb-13-big|system.sstable_activity_v2|1000|2000|-5519576429900224076|8615509011068470516'
    );
    assert.strictEqual(result[0].firstTokenStr, '-5519576429900224076');
    assert.strictEqual(result[0].lastTokenStr, '8615509011068470516');
});

test('label formatted as keyspace / name', () => {
    const result = parseMetadata(
        'nb-13-big|system.sstable_activity_v2|1000|2000|0|100'
    );
    assert.strictEqual(result[0].label, 'system.sstable_activity_v2 / nb-13-big');
});

test('multiple rows all parsed in order', () => {
    const result = parseMetadata([
        'nb-13-big|system.sstable_activity_v2|1775374724542000|1775374725524002|-5519576429900224076|8615509011068470516',
        'nb-14-big|system.sstable_activity_v2|1775385525477000|1775385525907000|-9171254530300049344|8686683291491149315',
        'nb-15-big|system.sstable_activity_v2|1775396324652001|1775396325022004|-7841743544415113649|7574960944173289686'
    ].join('\n'));
    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0].name, 'nb-13-big');
    assert.strictEqual(result[1].name, 'nb-14-big');
    assert.strictEqual(result[2].name, 'nb-15-big');
});

test('different keyspace correctly parsed', () => {
    const result = parseMetadata(
        'nb-1-big|keyspace1.table1|1000|2000|0|100'
    );
    assert.strictEqual(result[0].keyspace, 'keyspace1.table1');
});

// ─── Summary ─────────────────────────────────────────────────────────────────

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
