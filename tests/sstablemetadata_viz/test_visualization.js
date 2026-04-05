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

// ─── Row format helpers ───────────────────────────────────────────────────────

function row(name, ks, minTs, maxTs, ft, lt, cr, card, p50, droppable, dropP50S, ttlMinS, ttlMaxS) {
    return [name, ks, minTs, maxTs, ft, lt,
            cr       !== undefined ? cr       : '',
            card     !== undefined ? card     : '',
            p50      !== undefined ? p50      : '',
            droppable !== undefined ? droppable : '',
            dropP50S  !== undefined ? dropP50S  : '',
            ttlMinS   !== undefined ? ttlMinS   : '',
            ttlMaxS   !== undefined ? ttlMaxS   : ''].join('|');
}

// ─── Tests ───────────────────────────────────────────────────────────────────

test('single row: core fields parsed correctly', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1775374724542000', '1775374725524002',
        '-5519576429900224076', '8615509011068470516',
        '0.38903394255874674', '16', '50'
    ));
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
        'sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token|compression_ratio|cardinality|partition_size_p50',
        row('nb-13-big', 'system.sstable_activity_v2',
            '1775374724542000', '1775374725524002',
            '-5519576429900224076', '8615509011068470516',
            '0.38903394255874674', '16', '50')
    ].join('\n'));
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].name, 'nb-13-big');
});

test('empty lines filtered out', () => {
    const result = parseMetadata([
        '',
        row('nb-13-big', 'system.sstable_activity_v2',
            '1775374724542000', '1775374725524002',
            '-5519576429900224076', '8615509011068470516',
            '0.38903394255874674', '16', '50'),
        ''
    ].join('\n'));
    assert.strictEqual(result.length, 1);
});

test('minTs and maxTs parsed as integers', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1775374724542000', '1775374725524002', '0', '0',
        '0.389', '16', '50'
    ));
    assert.strictEqual(typeof result[0].minTs, 'number');
    assert.strictEqual(typeof result[0].maxTs, 'number');
    assert.strictEqual(result[0].minTs, 1775374724542000);
    assert.strictEqual(result[0].maxTs, 1775374725524002);
});

test('negative first token parsed as negative number', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000',
        '-5519576429900224076', '8615509011068470516',
        '0.389', '16', '50'
    ));
    assert.ok(result[0].firstToken < 0, 'firstToken should be negative');
});

test('firstTokenStr and lastTokenStr preserve exact string values', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000',
        '-5519576429900224076', '8615509011068470516',
        '0.389', '16', '50'
    ));
    assert.strictEqual(result[0].firstTokenStr, '-5519576429900224076');
    assert.strictEqual(result[0].lastTokenStr,  '8615509011068470516');
});

test('label formatted as keyspace / name', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50'
    ));
    assert.strictEqual(result[0].label, 'system.sstable_activity_v2 / nb-13-big');
});

test('multiple rows all parsed in order', () => {
    const result = parseMetadata([
        row('nb-13-big', 'system.sstable_activity_v2',
            '1775374724542000', '1775374725524002',
            '-5519576429900224076', '8615509011068470516',
            '0.38903394255874674', '16', '50'),
        row('nb-14-big', 'system.sstable_activity_v2',
            '1775385525477000', '1775385525907000',
            '-9171254530300049344', '8686683291491149315',
            '0.3629032258064516', '8', '50'),
        row('nb-15-big', 'system.sstable_activity_v2',
            '1775396324652001', '1775396325022004',
            '-7841743544415113649', '7574960944173289686',
            '0.3951612903225806', '8', '50')
    ].join('\n'));
    assert.strictEqual(result.length, 3);
    assert.strictEqual(result[0].name, 'nb-13-big');
    assert.strictEqual(result[1].name, 'nb-14-big');
    assert.strictEqual(result[2].name, 'nb-15-big');
});

test('different keyspace correctly parsed', () => {
    const result = parseMetadata(row(
        'nb-1-big', 'keyspace1.table1',
        '1000', '2000', '0', '100',
        '0.5', '10', '100'
    ));
    assert.strictEqual(result[0].keyspace, 'keyspace1.table1');
});

// ─── New field tests ──────────────────────────────────────────────────────────

test('compressionRatio parsed correctly', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.38903394255874674', '16', '50'
    ));
    assert.ok(Math.abs(result[0].compressionRatio - 0.38903394255874674) < 1e-10);
});

test('cardinality parsed as integer', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50'
    ));
    assert.strictEqual(result[0].cardinality, 16);
    assert.strictEqual(typeof result[0].cardinality, 'number');
});

test('partitionP50 parsed as integer', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50'
    ));
    assert.strictEqual(result[0].partitionP50, 50);
    assert.strictEqual(typeof result[0].partitionP50, 'number');
});

test('missing density fields yield null density and ucsLevel', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000',
        '-5519576429900224076', '8615509011068470516'
        // no compression_ratio, cardinality, partition_size_p50
    ));
    assert.strictEqual(result[0].density,   null);
    assert.strictEqual(result[0].ucsLevel,  null);
});

test('tokenFraction computed from token span', () => {
    // Simple case: tokens spanning exactly half the ring
    // ring = 2^64 ≈ 1.8446744073709552e19
    // half = 0 to 9223372036854775807 → fraction ~ 0.5
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000',
        '0', '9223372036854775807',
        '1.0', '100', '100'
    ));
    const tf = result[0].tokenFraction;
    assert.ok(tf > 0.4 && tf < 0.6, 'tokenFraction for half-ring should be ~0.5, got ' + tf);
});

test('estimatedBytes computed as cardinality * p50 * compressionRatio', () => {
    // 10 partitions * 200 bytes * 0.5 compression = 1000 bytes
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000',
        '0', '9223372036854775807',
        '0.5', '10', '200'
    ));
    const eb = result[0].estimatedBytes;
    assert.ok(Math.abs(eb - 1000) < 1, 'estimatedBytes should be ~1000, got ' + eb);
});

test('density = estimatedBytes / tokenFraction', () => {
    // token span = half ring → tokenFraction ≈ 0.5
    // estimatedBytes = 10 * 200 * 0.5 = 1000
    // density ≈ 1000 / 0.5 = 2000
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000',
        '0', '9223372036854775807',
        '0.5', '10', '200'
    ));
    const d = result[0].density;
    assert.ok(d > 1800 && d < 2200, 'density should be ~2000, got ' + d);
});

test('ucsLevel = floor(log2(density))', () => {
    // density ≈ 2000 → log2(2000) ≈ 10.96 → floor = 10
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000',
        '0', '9223372036854775807',
        '0.5', '10', '200'
    ));
    assert.strictEqual(result[0].ucsLevel, 10);
});

// ─── Tombstone field tests ────────────────────────────────────────────────────

test('droppable tombstones parsed as float', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50', '1.0', '1996099046'
    ));
    assert.ok(Math.abs(result[0].droppable - 1.0) < 1e-9);
    assert.strictEqual(typeof result[0].droppable, 'number');
});

test('droppable = 0.0 parsed correctly (not treated as missing)', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50', '0.0', '1996099046'
    ));
    // parseFloat('0.0') = 0, but || 0 also gives 0 — result is 0, not null
    // The field is present so droppable should be 0 (not null)
    assert.strictEqual(result[0].droppable, 0);
});

test('tombstone_drop_p50_s converted to milliseconds in dropP50Ms', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50', '1.0', '1996099046'
    ));
    assert.strictEqual(result[0].dropP50S,  1996099046);
    assert.strictEqual(result[0].dropP50Ms, 1996099046 * 1000);
});

test('missing tombstone fields yield null droppable and dropP50Ms', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100'
        // no compression_ratio, cardinality, p50, droppable, dropP50S
    ));
    assert.strictEqual(result[0].droppable,  null);
    assert.strictEqual(result[0].dropP50Ms,  null);
});

test('partial row with tombstone but no density fields', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'ks.tbl',
        '1000', '2000',
        '-100', '100',
        '', '', '', '0.75', '1700000000'
    ));
    assert.ok(Math.abs(result[0].droppable - 0.75) < 1e-9);
    assert.strictEqual(result[0].dropP50Ms, 1700000000 * 1000);
    assert.strictEqual(result[0].density, null);
});

// ─── TTL field tests ──────────────────────────────────────────────────────────

test('ttlMin and ttlMax parsed as integers when zero', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100',
        '0.389', '16', '50', '1.0', '1996099046', '0', '0'
    ));
    assert.strictEqual(result[0].ttlMin, 0);
    assert.strictEqual(result[0].ttlMax, 0);
    assert.strictEqual(typeof result[0].ttlMin, 'number');
    assert.strictEqual(typeof result[0].ttlMax, 'number');
});

test('ttlMin and ttlMax parsed correctly for non-zero TTL', () => {
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000', '0', '100',
        '0.5', '10', '100', '0.5', '1700000000', '3600', '86400'
    ));
    assert.strictEqual(result[0].ttlMin, 3600);
    assert.strictEqual(result[0].ttlMax, 86400);
});

test('missing TTL fields yield null', () => {
    const result = parseMetadata(row(
        'nb-13-big', 'system.sstable_activity_v2',
        '1000', '2000', '0', '100'
    ));
    assert.strictEqual(result[0].ttlMin, null);
    assert.strictEqual(result[0].ttlMax, null);
});

test('TTL fields present but only up to tombstone (no ttl columns)', () => {
    // Row has density + tombstone fields but missing TTL columns
    const result = parseMetadata(row(
        'nb-1-big', 'ks.tbl',
        '1000', '2000', '0', '100',
        '0.5', '10', '100', '1.0', '1996099046'
        // no ttl_min_s, ttl_max_s
    ));
    assert.strictEqual(result[0].ttlMin, null);
    assert.strictEqual(result[0].ttlMax, null);
});

// ─── Summary ─────────────────────────────────────────────────────────────────

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
if (failed > 0) process.exit(1);
