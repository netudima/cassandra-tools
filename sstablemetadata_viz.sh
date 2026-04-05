#!/bin/bash

# SSTable Metadata Visualizer
# Parses sstablemetadata output and generates an interactive HTML visualization
# Usage: ./sstablemetadata_viz.sh [--parse-only] <metadata-file> [output.html]
set -euo pipefail

# ===== SECTION: ARGUMENT PARSING & SETUP =====

PARSE_ONLY=false
if [ "${1:-}" = "--parse-only" ]; then
    PARSE_ONLY=true
    shift
fi

if [ $# -lt 1 ]; then
    echo "Usage: $0 [--parse-only] <metadata-file> [output.html]"
    echo "Example: $0 sstablemetadata.out visualization.html"
    echo "         $0 --parse-only sstablemetadata.out"
    exit 1
fi

INFILE="$1"
OUTPUT="${2:-${INFILE%.*}.html}"

if [ ! -f "$INFILE" ]; then
    echo "Error: File '$INFILE' not found"
    exit 1
fi

# ===== SECTION: AWK PARSER =====
# Output format:
#   sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token
#   |compression_ratio|cardinality|partition_size_p50
#   |droppable_tombstones|tombstone_drop_p50_s|ttl_min_s|ttl_max_s

run_parser() {
gawk '
function extract_sstable_name(path) {
    if (match(path, /\/([^\/]+)$/, arr)) {
        return arr[1]
    }
    return path
}

function extract_keyspace_table(path) {
    n = split(path, parts, "/")
    if (n >= 3) {
        keyspace = parts[n-2]
        table_dir = parts[n-1]
        if (match(table_dir, /^(.+)-[0-9a-f]{32}$/, arr)) {
            return keyspace "." arr[1]
        }
        return keyspace "." table_dir
    }
    return ""
}

/^SSTable:/ {
    match($0, /^SSTable: (.+)$/, arr)
    path = arr[1]
    cur_sstable = extract_sstable_name(path)
    cur_kstable = extract_keyspace_table(path)
    cur_min_ts = ""
    cur_max_ts = ""
    cur_first_token = ""
    cur_last_token = ""
    cur_compression = ""
    cur_cardinality = ""
    cur_partition_p50 = ""
    cur_droppable = ""
    cur_tombstone_p50 = ""
    cur_ttl_min = ""
    cur_ttl_max = ""
    cur_section = ""
    in_section_percentiles = 0
}

/^Minimum timestamp:/ {
    if (match($0, /\(([0-9]{13,})\)/, arr)) cur_min_ts = arr[1]
    else { match($0, /[[:space:]]([0-9]{13,})/, arr); cur_min_ts = arr[1] }
}

/^Maximum timestamp:/ {
    if (match($0, /\(([0-9]{13,})\)/, arr)) cur_max_ts = arr[1]
    else { match($0, /[[:space:]]([0-9]{13,})/, arr); cur_max_ts = arr[1] }
}

/^First token:/ {
    match($0, /First token:[[:space:]]+(-?[0-9]+)/, arr)
    cur_first_token = arr[1]
}

/^Last token:/ {
    match($0, /Last token:[[:space:]]+(-?[0-9]+)/, arr)
    cur_last_token = arr[1]
}

/^Compression ratio:/ {
    match($0, /Compression ratio:[[:space:]]+([0-9.]+)/, arr)
    cur_compression = arr[1]
}

/^TTL min:/ {
    match($0, /TTL min:[[:space:]]+([0-9]+)/, arr)
    cur_ttl_min = arr[1]
}

/^TTL max:/ {
    match($0, /TTL max:[[:space:]]+([0-9]+)/, arr)
    cur_ttl_max = arr[1]
}

/^Estimated droppable tombstones:/ {
    match($0, /Estimated droppable tombstones:[[:space:]]+([0-9.]+)/, arr)
    cur_droppable = arr[1]
}

/^Estimated cardinality:/ {
    match($0, /Estimated cardinality:[[:space:]]+([0-9]+)/, arr)
    cur_cardinality = arr[1]
}

# Unified section state machine - covers both "Estimated tombstone drop times:"
# and "Partition Size:" histogram blocks to extract the 50th percentile of each.

/^Estimated tombstone drop times:/ {
    cur_section = "tombstone_drop"
    in_section_percentiles = 0
}

/^Partition Size:/ {
    cur_section = "partition_size"
    in_section_percentiles = 0
}

/^[A-Z]/ && !/^Estimated tombstone drop times:/ && !/^Partition Size:/ {
    cur_section = ""
    in_section_percentiles = 0
}

cur_section != "" && /Percentiles/ { in_section_percentiles = 1 }

in_section_percentiles && /50th/ {
    match($0, /50th[[:space:]]+([0-9]+)/, arr)
    if (cur_section == "tombstone_drop") {
        cur_tombstone_p50 = arr[1]
    } else if (cur_section == "partition_size") {
        cur_partition_p50 = arr[1]
    }
    cur_section = ""
    in_section_percentiles = 0
}

/^IsTransient:/ {
    if (cur_sstable != "" && !(cur_sstable in seen)) {
        seen[cur_sstable] = 1
        print cur_sstable "|" cur_kstable "|" cur_min_ts "|" cur_max_ts \
            "|" cur_first_token "|" cur_last_token \
            "|" cur_compression "|" cur_cardinality "|" cur_partition_p50 \
            "|" cur_droppable "|" cur_tombstone_p50 \
            "|" cur_ttl_min "|" cur_ttl_max
    }
}

END {
    if (cur_sstable != "" && !(cur_sstable in seen)) {
        print cur_sstable "|" cur_kstable "|" cur_min_ts "|" cur_max_ts \
            "|" cur_first_token "|" cur_last_token \
            "|" cur_compression "|" cur_cardinality "|" cur_partition_p50 \
            "|" cur_droppable "|" cur_tombstone_p50 \
            "|" cur_ttl_min "|" cur_ttl_max
    }
}
' "$1"
}

echo "Parsing metadata file: $INFILE" >&2

if [ "$PARSE_ONLY" = true ]; then
    echo "sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token|compression_ratio|cardinality|partition_size_p50|droppable_tombstones|tombstone_drop_p50_s|ttl_min_s|ttl_max_s"
    run_parser "$INFILE"
    exit 0
fi

# ===== SECTION: HTML GENERATION =====

EVENTS_FILE=$(mktemp)
HTML_HEAD=$(mktemp)
HTML_TAIL=$(mktemp)
trap "rm -f $EVENTS_FILE $HTML_HEAD $HTML_TAIL" EXIT

run_parser "$INFILE" > "$EVENTS_FILE"
echo "Parsed $(wc -l < "$EVENTS_FILE") unique SSTable(s)" >&2

cat > "$HTML_HEAD" << 'HTMLEOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cassandra SSTable Metadata</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a1a;
            color: #e0e0e0;
            padding: 20px;
        }
        h1 { text-align: center; margin-bottom: 16px; color: #fff; font-size: 22px; }
        .stats { text-align: center; margin-bottom: 16px; color: #aaa; font-size: 14px; }
        .tabs { display: flex; gap: 8px; margin-bottom: 14px; }
        .tab-btn {
            padding: 9px 22px;
            background: #333;
            color: #aaa;
            border: 1px solid #555;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            transition: background 0.15s;
        }
        .tab-btn.active { background: #2196F3; color: #fff; border-color: #2196F3; }
        .tab-btn:hover:not(.active) { background: #444; color: #eee; }
        .controls { display: flex; gap: 12px; align-items: center; margin-bottom: 14px; }
        .reset-btn {
            padding: 7px 15px;
            background: #2196F3;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
        }
        .reset-btn:hover { background: #1976D2; }
        #chart-container {
            position: relative;
            width: 100%;
            background: #2a2a2a;
            border-radius: 8px;
            overflow: auto;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            min-height: 180px;
        }
        #chart-canvas { position: absolute; top: 0; left: 0; cursor: crosshair; }
        #zoom-selection {
            position: absolute;
            border: 2px dashed #2196F3;
            background: rgba(33,150,243,0.1);
            pointer-events: none;
            display: none;
        }
        #tooltip {
            position: fixed;
            background: rgba(0,0,0,0.92);
            color: #eee;
            padding: 10px 14px;
            border-radius: 6px;
            font-size: 13px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.15s;
            z-index: 1000;
            max-width: 440px;
            border: 1px solid #555;
            line-height: 1.5;
        }
        #tooltip.show { opacity: 1; }
        .tooltip-row { margin: 2px 0; }
        .tooltip-label { color: #999; display: inline-block; min-width: 130px; }
    </style>
</head>
<body>
    <div>
        <h1>Cassandra SSTable Metadata</h1>
        <div class="stats" id="stats"></div>
        <div class="tabs">
            <button class="tab-btn active" id="tab-ts"   onclick="switchTab('timestamps')">Timestamp Ranges</button>
            <button class="tab-btn"        id="tab-tok"  onclick="switchTab('tokens')">Token Ranges</button>
            <button class="tab-btn"        id="tab-den"  onclick="switchTab('density')">Density</button>
            <button class="tab-btn"        id="tab-tomb" onclick="switchTab('tombstones')">Tombstones</button>
            <button class="tab-btn"        id="tab-ttl"  onclick="switchTab('ttl')">TTL</button>
        </div>
        <div class="controls">
            <button class="reset-btn" onclick="resetZoom()">Reset Zoom</button>
        </div>
        <div id="chart-container">
            <canvas id="chart-canvas"></canvas>
            <div id="zoom-selection"></div>
        </div>
        <div id="tooltip"></div>
    </div>

    <script>
        const rawData = `
HTMLEOF

cat > "$HTML_TAIL" << 'HTMLEOF'
`;

        // ===BEGIN_DATA_PROCESSING===
        const TOKEN_RING = 1.8446744073709552e19;

        const sstables = rawData.trim().split('\n')
            .filter(line => line.length > 0 && !line.startsWith('sstable_name'))
            .map(line => {
                const [name, keyspace, minTs, maxTs, firstToken, lastToken,
                       compressionRatio, cardinality, partitionP50,
                       droppableTombstones, tombstoneDropP50S,
                       ttlMinS, ttlMaxS] = line.split('|');
                const cr       = parseFloat(compressionRatio)    || 0;
                const card     = parseInt(cardinality)           || 0;
                const p50      = parseInt(partitionP50)          || 0;
                const ft       = Number(firstToken);
                const lt       = Number(lastToken);
                const droppable = droppableTombstones !== ''
                    ? parseFloat(droppableTombstones) : null;
                const dropP50S  = tombstoneDropP50S !== ''
                    ? parseInt(tombstoneDropP50S) : null;
                const ttlMin = (ttlMinS !== undefined && ttlMinS !== '')
                    ? parseInt(ttlMinS) : null;
                const ttlMax = (ttlMaxS !== undefined && ttlMaxS !== '')
                    ? parseInt(ttlMaxS) : null;
                const tokenFraction = (lt >= ft)
                    ? (lt - ft) / TOKEN_RING
                    : (lt - ft + TOKEN_RING) / TOKEN_RING;
                const estimatedBytes = (card > 0 && p50 > 0)
                    ? card * p50 * (cr > 0 ? cr : 1)
                    : null;
                const density = (estimatedBytes !== null && tokenFraction > 0)
                    ? estimatedBytes / tokenFraction
                    : null;
                const ucsLevel = (density !== null && density > 0)
                    ? Math.floor(Math.log2(density))
                    : null;
                return {
                    name,
                    keyspace,
                    label: (keyspace || name) + ' / ' + name,
                    minTs:  parseInt(minTs),
                    maxTs:  parseInt(maxTs),
                    firstToken:    ft,
                    lastToken:     lt,
                    firstTokenStr: firstToken,
                    lastTokenStr:  lastToken,
                    compressionRatio: cr,
                    cardinality:  card,
                    partitionP50: p50,
                    tokenFraction,
                    estimatedBytes,
                    density,
                    ucsLevel,
                    droppable,
                    dropP50S,
                    dropP50Ms: dropP50S !== null ? dropP50S * 1000 : null,
                    ttlMin,
                    ttlMax
                };
            });
        // ===END_DATA_PROCESSING===

        const COLORS = [
            '#4CAF50', '#2196F3', '#FF9800', '#E91E63', '#9C27B0',
            '#00BCD4', '#CDDC39', '#FF5722', '#607D8B', '#795548',
            '#F44336', '#03A9F4', '#8BC34A', '#FFC107', '#673AB7'
        ];

        const canvas    = document.getElementById('chart-canvas');
        const ctx       = canvas.getContext('2d');
        const container = document.getElementById('chart-container');
        const tooltip   = document.getElementById('tooltip');

        document.getElementById('stats').textContent =
            sstables.length + ' unique SSTable' + (sstables.length !== 1 ? 's' : '');

        const MARGIN     = { top: 50, right: 30, bottom: 20, left: 300 };
        const ROW_HEIGHT = 28;
        const ROW_SPACING = 6;

        let currentTab  = 'timestamps';
        let tsZoom      = null;
        let tokZoom     = null;
        let densZoom    = null;
        let tombZoom    = null;
        let ttlZoom     = null;
        let hoveredSstable = null;
        let isDragging  = false;
        let dragStart   = null;

        function getZoom() {
            if (currentTab === 'timestamps') return tsZoom;
            if (currentTab === 'tokens')     return tokZoom;
            if (currentTab === 'density')    return densZoom;
            if (currentTab === 'tombstones') return tombZoom;
            return ttlZoom;
        }
        function setZoom(z) {
            if (currentTab === 'timestamps')      tsZoom   = z;
            else if (currentTab === 'tokens')     tokZoom  = z;
            else if (currentTab === 'density')    densZoom = z;
            else if (currentTab === 'tombstones') tombZoom = z;
            else                                  ttlZoom  = z;
        }

        function switchTab(tab) {
            currentTab = tab;
            document.getElementById('tab-ts').classList.toggle('active',   tab === 'timestamps');
            document.getElementById('tab-tok').classList.toggle('active',  tab === 'tokens');
            document.getElementById('tab-den').classList.toggle('active',  tab === 'density');
            document.getElementById('tab-tomb').classList.toggle('active', tab === 'tombstones');
            document.getElementById('tab-ttl').classList.toggle('active',  tab === 'ttl');
            draw();
        }

        function resetZoom() { setZoom(null); draw(); }

        // ─── Canvas helpers ───────────────────────────────────────────────────

        function setupCanvas(rowCount) {
            if (rowCount === undefined) rowCount = sstables.length;
            const width  = Math.max(container.clientWidth, 800);
            const height = MARGIN.top + rowCount * (ROW_HEIGHT + ROW_SPACING) + MARGIN.bottom;
            canvas.width  = width;
            canvas.height = height;
            container.style.height = Math.min(height + 4, 600) + 'px';
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, 0, width, height);
            return { width, height, chartWidth: width - MARGIN.left - MARGIN.right };
        }

        function drawBarsAndLabels(arr, minVal, maxVal, getMin, getMax, chartWidth) {
            if (arr === undefined) arr = sstables;
            arr.forEach((s, i) => {
                const y     = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING);
                const range = maxVal - minVal;
                const sMin  = getMin(s);
                const sMax  = getMax(s);
                const x1    = MARGIN.left + (sMin - minVal) / range * chartWidth;
                const rawW  = (sMax - sMin) / range * chartWidth;
                const barW  = Math.max(rawW, 3);
                ctx.fillStyle = COLORS[i % COLORS.length];
                ctx.fillRect(x1, y, barW, ROW_HEIGHT);
                s._rect = { x: x1, y, width: barW, height: ROW_HEIGHT };

                if (barW > 60) {
                    ctx.save();
                    ctx.beginPath();
                    ctx.rect(x1, y, barW, ROW_HEIGHT);
                    ctx.clip();
                    ctx.fillStyle = 'rgba(0,0,0,0.55)';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText(s.name, x1 + 5, y + ROW_HEIGHT / 2);
                    ctx.restore();
                }
            });
        }

        function drawXAxis(offsetY, minVal, maxVal, formatFn, chartWidth) {
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, offsetY, canvas.width, MARGIN.top);
            ctx.strokeStyle = '#555';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(MARGIN.left, offsetY + MARGIN.top);
            ctx.lineTo(MARGIN.left + chartWidth, offsetY + MARGIN.top);
            ctx.stroke();
            ctx.fillStyle = '#aaa';
            ctx.font = '11px monospace';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'bottom';
            const numTicks = 6;
            for (let i = 0; i <= numTicks; i++) {
                const val = minVal + (maxVal - minVal) * i / numTicks;
                const x   = MARGIN.left + (val - minVal) / (maxVal - minVal) * chartWidth;
                ctx.fillText(formatFn(val), x, offsetY + MARGIN.top - 4);
                ctx.strokeStyle = '#555';
                ctx.beginPath();
                ctx.moveTo(x, offsetY + MARGIN.top);
                ctx.lineTo(x, offsetY + MARGIN.top - 5);
                ctx.stroke();
            }
        }

        function drawYAxisLabels(arr) {
            if (arr === undefined) arr = sstables;
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, MARGIN.top, MARGIN.left - 1, canvas.height - MARGIN.top);
            ctx.strokeStyle = '#555';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(MARGIN.left, MARGIN.top);
            ctx.lineTo(MARGIN.left, canvas.height);
            ctx.stroke();
            ctx.fillStyle = '#ccc';
            ctx.font = '11px monospace';
            ctx.textAlign = 'right';
            ctx.textBaseline = 'middle';
            arr.forEach((s, i) => {
                const y = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING) + ROW_HEIGHT / 2;
                ctx.fillText(s.label, MARGIN.left - 8, y);
            });
        }

        // ─── Timestamp tab ───────────────────────────────────────────────────

        function drawTimestamps() {
            const { chartWidth } = setupCanvas();

            const allMinTs = Math.min(...sstables.map(s => s.minTs));
            const allMaxTs = Math.max(...sstables.map(s => s.maxTs));
            if (!tsZoom) tsZoom = { min: allMinTs, max: allMaxTs };
            const { min: minVal, max: maxVal } = tsZoom;

            drawBarsAndLabels(sstables, minVal, maxVal, s => s.minTs, s => s.maxTs, chartWidth);

            function formatTs(us) {
                const d     = new Date(us / 1000);
                const range = maxVal - minVal;
                if (range < 10e6)   return d.toISOString().substr(11, 12);
                if (range < 3600e6) return d.toISOString().substr(11, 8);
                return d.toISOString().substr(0, 16).replace('T', ' ');
            }

            const scrollY = container.scrollTop;
            drawXAxis(scrollY, minVal, maxVal, formatTs, chartWidth);
            if (scrollY > 0) drawXAxis(0, minVal, maxVal, formatTs, chartWidth);
            drawYAxisLabels();
        }

        // ─── Token tab ───────────────────────────────────────────────────────

        function drawTokens() {
            const { chartWidth } = setupCanvas();

            const RING_MIN = -9223372036854775808;
            const RING_MAX =  9223372036854775807;
            if (!tokZoom) tokZoom = { min: RING_MIN, max: RING_MAX };
            const { min: minVal, max: maxVal } = tokZoom;

            drawBarsAndLabels(sstables, minVal, maxVal, s => s.firstToken, s => s.lastToken, chartWidth);

            function formatTok(v) {
                const av   = Math.abs(v);
                const sign = v < 0 ? '-' : '';
                if (av >= 1e18) return sign + (av / 1e18).toFixed(1) + 'e18';
                if (av >= 1e15) return sign + (av / 1e15).toFixed(1) + 'e15';
                if (av >= 1e12) return sign + (av / 1e12).toFixed(1) + 'e12';
                if (av >= 1e9)  return sign + (av / 1e9).toFixed(1)  + 'e9';
                return v.toFixed(0);
            }

            const scrollY = container.scrollTop;
            drawXAxis(scrollY, minVal, maxVal, formatTok, chartWidth);
            if (scrollY > 0) drawXAxis(0, minVal, maxVal, formatTok, chartWidth);
            drawYAxisLabels();
        }

        // ─── Density tab ─────────────────────────────────────────────────────

        function formatBytes(b) {
            if (b === null || b === undefined) return 'N/A';
            if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GiB';
            if (b >= 1048576)    return (b / 1048576).toFixed(2)    + ' MiB';
            if (b >= 1024)       return (b / 1024).toFixed(2)       + ' KiB';
            return b.toFixed(1) + ' B';
        }

        function buildDensityArr() {
            const withDensity = sstables
                .filter(s => s.density !== null)
                .sort((a, b) => {
                    if (a.ucsLevel !== b.ucsLevel) return a.ucsLevel - b.ucsLevel;
                    return a.density - b.density;
                });
            const withoutDensity = sstables.filter(s => s.density === null);
            return [...withDensity, ...withoutDensity];
        }

        function drawDensityXAxis(offsetY, minVal, maxVal, chartWidth) {
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, offsetY, canvas.width, MARGIN.top);
            ctx.strokeStyle = '#555';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(MARGIN.left, offsetY + MARGIN.top);
            ctx.lineTo(MARGIN.left + chartWidth, offsetY + MARGIN.top);
            ctx.stroke();

            const range = maxVal - minVal;
            ctx.font = '11px monospace';
            ctx.textBaseline = 'bottom';
            for (let level = Math.ceil(minVal); level <= Math.floor(maxVal); level++) {
                const x = MARGIN.left + (level - minVal) / range * chartWidth;
                ctx.fillStyle = '#aaa';
                ctx.textAlign = 'center';
                ctx.fillText('L' + level, x, offsetY + MARGIN.top - 4);
                ctx.strokeStyle = '#555';
                ctx.setLineDash([]);
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(x, offsetY + MARGIN.top);
                ctx.lineTo(x, offsetY + MARGIN.top - 5);
                ctx.stroke();
            }
        }

        function drawDensity() {
            const densityArr  = buildDensityArr();
            const withDensity = densityArr.filter(s => s.density !== null);

            const { width, chartWidth } = setupCanvas(densityArr.length);

            if (withDensity.length === 0) {
                ctx.fillStyle = '#888';
                ctx.font = '13px monospace';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(
                    'No density data — requires compression_ratio, cardinality, partition_size_p50',
                    width / 2, canvas.height / 2);
                drawYAxisLabels(densityArr);
                return;
            }

            const log2Densities = withDensity.map(s => Math.log2(s.density));
            const globalMin = Math.floor(Math.min(...log2Densities)) - 1;
            const globalMax = Math.ceil(Math.max(...log2Densities))  + 1;
            if (!densZoom) densZoom = { min: globalMin, max: globalMax };
            const { min: minVal, max: maxVal } = densZoom;
            const range = maxVal - minVal;

            // Alternating level bands
            for (let level = Math.floor(minVal); level < Math.ceil(maxVal); level++) {
                if (level % 2 === 0) {
                    const bx  = MARGIN.left + Math.max(0, (level     - minVal) / range * chartWidth);
                    const bx2 = MARGIN.left + Math.min(chartWidth, (level + 1 - minVal) / range * chartWidth);
                    ctx.fillStyle = 'rgba(255,255,255,0.03)';
                    ctx.fillRect(bx, MARGIN.top, bx2 - bx, canvas.height - MARGIN.top - MARGIN.bottom);
                }
            }

            // Level boundary lines
            ctx.save();
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = '#3a3a3a';
            ctx.lineWidth = 1;
            for (let level = Math.ceil(minVal); level <= Math.floor(maxVal); level++) {
                const x = MARGIN.left + (level - minVal) / range * chartWidth;
                ctx.beginPath();
                ctx.moveTo(x, MARGIN.top);
                ctx.lineTo(x, canvas.height - MARGIN.bottom);
                ctx.stroke();
            }
            ctx.restore();

            // Bars
            densityArr.forEach((s, i) => {
                const y = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING);
                if (s.density === null) {
                    ctx.fillStyle = '#444';
                    ctx.fillRect(MARGIN.left, y, 20, ROW_HEIGHT);
                    s._rect = { x: MARGIN.left, y, width: 20, height: ROW_HEIGHT };
                    return;
                }
                const log2d = Math.log2(s.density);
                const barW  = Math.max((log2d - minVal) / range * chartWidth, 3);
                ctx.fillStyle = COLORS[((s.ucsLevel || 0) + COLORS.length) % COLORS.length];
                ctx.fillRect(MARGIN.left, y, barW, ROW_HEIGHT);
                s._rect = { x: MARGIN.left, y, width: barW, height: ROW_HEIGHT };

                if (barW > 80) {
                    ctx.save();
                    ctx.beginPath();
                    ctx.rect(MARGIN.left, y, barW, ROW_HEIGHT);
                    ctx.clip();
                    ctx.fillStyle = 'rgba(0,0,0,0.55)';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText('L' + s.ucsLevel + ': ' + s.name, MARGIN.left + 5, y + ROW_HEIGHT / 2);
                    ctx.restore();
                }
            });

            const scrollY = container.scrollTop;
            drawDensityXAxis(scrollY, minVal, maxVal, chartWidth);
            if (scrollY > 0) drawDensityXAxis(0, minVal, maxVal, chartWidth);
            drawYAxisLabels(densityArr);
        }

        // ─── Tombstones tab ───────────────────────────────────────────────────

        // Color by droppable fraction: green ≥ 0.9, amber 0.3–0.9, red < 0.3
        function tombColor(droppable) {
            if (droppable === null) return '#555';
            const hue = Math.round((1 - droppable) * 120);
            return `hsl(${hue}, 80%, 42%)`;
        }

        function buildTombArr() {
            return [...sstables].sort((a, b) => {
                // Most droppable (cleanest) at bottom; least droppable (problem) at top
                const da = a.droppable !== null ? a.droppable : -1;
                const db = b.droppable !== null ? b.droppable : -1;
                return da - db;
            });
        }

        function drawTombstones() {
            const tombArr = buildTombArr();
            const hasData = tombArr.some(s => s.droppable !== null);
            const { width, chartWidth } = setupCanvas(tombArr.length);

            if (!hasData) {
                ctx.fillStyle = '#888';
                ctx.font = '13px monospace';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillText(
                    'No tombstone data — requires Estimated droppable tombstones field',
                    width / 2, canvas.height / 2);
                drawYAxisLabels(tombArr);
                return;
            }

            if (!tombZoom) tombZoom = { min: 0, max: 1 };
            const { min: minVal, max: maxVal } = tombZoom;
            const range = maxVal - minVal;

            // 25% / 50% / 75% guide lines
            ctx.save();
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = '#3a3a3a';
            ctx.lineWidth = 1;
            for (const frac of [0.25, 0.5, 0.75]) {
                if (frac > minVal && frac < maxVal) {
                    const x = MARGIN.left + (frac - minVal) / range * chartWidth;
                    ctx.beginPath();
                    ctx.moveTo(x, MARGIN.top);
                    ctx.lineTo(x, canvas.height - MARGIN.bottom);
                    ctx.stroke();
                }
            }
            ctx.restore();

            // Bars
            tombArr.forEach((s, i) => {
                const y = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING);

                // Faint full-width background so the row is hoverable even with short bars
                ctx.fillStyle = 'rgba(255,255,255,0.03)';
                ctx.fillRect(MARGIN.left, y, chartWidth, ROW_HEIGHT);
                s._rect = { x: MARGIN.left, y, width: chartWidth, height: ROW_HEIGHT };

                if (s.droppable === null) {
                    ctx.fillStyle = '#444';
                    ctx.fillRect(MARGIN.left, y, 20, ROW_HEIGHT);
                    return;
                }

                const barW = Math.max(
                    Math.min((s.droppable - minVal) / range * chartWidth, chartWidth),
                    3
                );
                ctx.fillStyle = tombColor(s.droppable);
                ctx.fillRect(MARGIN.left, y, barW, ROW_HEIGHT);

                if (chartWidth > 80) {
                    ctx.save();
                    ctx.beginPath();
                    ctx.rect(MARGIN.left, y, chartWidth, ROW_HEIGHT);
                    ctx.clip();
                    ctx.fillStyle = 'rgba(0,0,0,0.6)';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText(
                        (s.droppable * 100).toFixed(0) + '% droppable  ' + s.name,
                        MARGIN.left + 5, y + ROW_HEIGHT / 2);
                    ctx.restore();
                }
            });

            function formatPct(v) { return (v * 100).toFixed(0) + '%'; }
            const scrollY = container.scrollTop;
            drawXAxis(scrollY, minVal, maxVal, formatPct, chartWidth);
            if (scrollY > 0) drawXAxis(0, minVal, maxVal, formatPct, chartWidth);
            drawYAxisLabels(tombArr);
        }

        // ─── TTL tab ──────────────────────────────────────────────────────────

        function formatDuration(s) {
            if (s === 0) return '0';
            const d = Math.floor(s / 86400);
            const h = Math.floor((s % 86400) / 3600);
            const m = Math.floor((s % 3600) / 60);
            const r = s % 60;
            if (d > 0) return d + 'd' + (h > 0 ? h + 'h' : '');
            if (h > 0) return h + 'h' + (m > 0 ? m + 'm' : '');
            if (m > 0) return m + 'm' + (r > 0 ? r + 's' : '');
            return r + 's';
        }

        function drawTTL() {
            // Sort: TTL > 0 descending (longest first), then TTL = 0, then null
            const ttlArr = [...sstables].sort((a, b) => {
                const ta = (a.ttlMax !== null && a.ttlMax > 0) ? a.ttlMax : -1;
                const tb = (b.ttlMax !== null && b.ttlMax > 0) ? b.ttlMax : -1;
                return tb - ta;
            });

            const { width, chartWidth } = setupCanvas(ttlArr.length);
            const hasTTL = sstables.some(s => s.ttlMax !== null && s.ttlMax > 0);
            const maxTTL = hasTTL
                ? Math.max(...sstables.filter(s => s.ttlMax > 0).map(s => s.ttlMax))
                : 0;

            if (!ttlZoom) ttlZoom = { min: 0, max: hasTTL ? maxTTL : 86400 };
            const { min: minVal, max: maxVal } = ttlZoom;
            const range = maxVal - minVal;

            ttlArr.forEach((s, i) => {
                const y = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING);
                // Full-row rect so entire row is hoverable
                s._rect = { x: MARGIN.left, y, width: chartWidth, height: ROW_HEIGHT };

                if (s.ttlMax === null) {
                    ctx.fillStyle = '#3a3a3a';
                    ctx.fillRect(MARGIN.left, y, chartWidth, ROW_HEIGHT);
                    ctx.fillStyle = '#666';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText('N/A  ' + s.name, MARGIN.left + 5, y + ROW_HEIGHT / 2);
                    return;
                }

                if (s.ttlMax === 0) {
                    ctx.fillStyle = 'rgba(80,80,80,0.35)';
                    ctx.fillRect(MARGIN.left, y, chartWidth, ROW_HEIGHT);
                    ctx.fillStyle = '#777';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText('No TTL  ' + s.name, MARGIN.left + 5, y + ROW_HEIGHT / 2);
                    return;
                }

                const x1   = MARGIN.left + Math.max(0, (s.ttlMin - minVal) / range * chartWidth);
                const barW = Math.max((s.ttlMax - Math.max(s.ttlMin, minVal)) / range * chartWidth, 3);
                ctx.fillStyle = COLORS[i % COLORS.length];
                ctx.fillRect(x1, y, barW, ROW_HEIGHT);
                s._rect = { x: x1, y, width: barW, height: ROW_HEIGHT };

                if (barW > 60) {
                    ctx.save();
                    ctx.beginPath();
                    ctx.rect(x1, y, barW, ROW_HEIGHT);
                    ctx.clip();
                    ctx.fillStyle = 'rgba(0,0,0,0.55)';
                    ctx.font = '10px monospace';
                    ctx.textAlign = 'left';
                    ctx.textBaseline = 'middle';
                    ctx.fillText(formatDuration(s.ttlMax) + '  ' + s.name, x1 + 5, y + ROW_HEIGHT / 2);
                    ctx.restore();
                }
            });

            if (!hasTTL) {
                ctx.fillStyle = '#888';
                ctx.font = '12px monospace';
                ctx.textAlign = 'center';
                ctx.textBaseline = 'bottom';
                ctx.fillText('No SSTables have TTL set', MARGIN.left + chartWidth / 2, MARGIN.top - 6);
            }

            const scrollY = container.scrollTop;
            drawXAxis(scrollY, minVal, maxVal, formatDuration, chartWidth);
            if (scrollY > 0) drawXAxis(0, minVal, maxVal, formatDuration, chartWidth);
            drawYAxisLabels(ttlArr);
        }

        // ─── Dispatch ────────────────────────────────────────────────────────

        function draw() {
            if (currentTab === 'timestamps')       drawTimestamps();
            else if (currentTab === 'tokens')      drawTokens();
            else if (currentTab === 'density')     drawDensity();
            else if (currentTab === 'tombstones')  drawTombstones();
            else                                   drawTTL();
        }

        // ─── Tooltip ─────────────────────────────────────────────────────────

        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const cx = e.clientX - rect.left;
            const cy = e.clientY - rect.top;

            if (isDragging && dragStart) {
                const left = Math.min(dragStart.x, cx);
                const w    = Math.abs(cx - dragStart.x);
                const zoomDiv = document.getElementById('zoom-selection');
                zoomDiv.style.left    = left + 'px';
                zoomDiv.style.top     = MARGIN.top + 'px';
                zoomDiv.style.width   = w + 'px';
                zoomDiv.style.height  = (canvas.height - MARGIN.top - MARGIN.bottom) + 'px';
                zoomDiv.style.display = 'block';
                tooltip.classList.remove('show');
                return;
            }

            hoveredSstable = null;
            for (const s of sstables) {
                const r = s._rect;
                if (r && cx >= r.x && cx <= r.x + r.width && cy >= r.y && cy <= r.y + r.height) {
                    hoveredSstable = s;
                    break;
                }
            }

            if (hoveredSstable) {
                canvas.style.cursor = 'pointer';
                const s = hoveredSstable;
                let html = '<div class="tooltip-row"><strong>' + s.name + '</strong></div>';
                html += '<div class="tooltip-row"><span class="tooltip-label">Table:</span> ' + s.keyspace + '</div>';

                if (currentTab === 'timestamps') {
                    const minD   = new Date(s.minTs / 1000).toISOString().replace('T', ' ').replace('Z', ' UTC');
                    const maxD   = new Date(s.maxTs / 1000).toISOString().replace('T', ' ').replace('Z', ' UTC');
                    const durUs  = s.maxTs - s.minTs;
                    const durStr = durUs < 1000 ? durUs + ' µs'
                        : durUs < 1e6  ? (durUs / 1000).toFixed(2) + ' ms'
                        : (durUs / 1e6).toFixed(3) + ' s';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Min timestamp:</span> ' + minD + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Max timestamp:</span> ' + maxD + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Duration:</span> '      + durStr + '</div>';

                } else if (currentTab === 'tokens') {
                    const span = s.lastToken - s.firstToken;
                    const pct  = (span / TOKEN_RING * 100).toFixed(4) + '% of ring';
                    html += '<div class="tooltip-row"><span class="tooltip-label">First token:</span> ' + s.firstTokenStr + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Last token:</span> '  + s.lastTokenStr  + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Token span:</span> '  + pct + '</div>';

                } else if (currentTab === 'density') {
                    if (s.density !== null) {
                        html += '<div class="tooltip-row"><span class="tooltip-label">UCS Level:</span> '         + s.ucsLevel + '</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Est. density:</span> '      + formatBytes(s.density)       + ' / token-fraction</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Est. size:</span> '         + formatBytes(s.estimatedBytes) + '</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Token fraction:</span> '    + (s.tokenFraction * 100).toFixed(4) + '%</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Compression ratio:</span> ' + s.compressionRatio.toFixed(4) + '</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Cardinality:</span> '       + s.cardinality + '</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Partition p50:</span> '     + s.partitionP50 + ' B</div>';
                    } else {
                        html += '<div class="tooltip-row" style="color:#888">Density not available<br>(missing cardinality, partition p50, or compression ratio)</div>';
                    }

                } else if (currentTab === 'ttl') {
                    if (s.ttlMax === null) {
                        html += '<div class="tooltip-row" style="color:#888">TTL data not available</div>';
                    } else if (s.ttlMax === 0) {
                        html += '<div class="tooltip-row"><span class="tooltip-label">TTL:</span> not set (data lives forever)</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">TTL min:</span> ' + s.ttlMin + ' s</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">TTL max:</span> ' + s.ttlMax + ' s</div>';
                    } else {
                        html += '<div class="tooltip-row"><span class="tooltip-label">TTL min:</span> ' + formatDuration(s.ttlMin) + ' (' + s.ttlMin + ' s)</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">TTL max:</span> ' + formatDuration(s.ttlMax) + ' (' + s.ttlMax + ' s)</div>';
                        if (s.ttlMin !== s.ttlMax) {
                            html += '<div class="tooltip-row"><span class="tooltip-label">TTL spread:</span> ' + formatDuration(s.ttlMax - s.ttlMin) + '</div>';
                        }
                    }

                } else {
                    // tombstones tab
                    if (s.droppable !== null) {
                        const pct     = (s.droppable * 100).toFixed(1) + '%';
                        const dropStr = s.dropP50Ms !== null
                            ? new Date(s.dropP50Ms).toISOString().replace('T', ' ').replace('Z', ' UTC')
                            : 'N/A';
                        const now       = Date.now();
                        const futureMs  = s.dropP50Ms !== null ? s.dropP50Ms - now : null;
                        let   urgency   = '';
                        if (futureMs !== null) {
                            if (futureMs <= 0)                       urgency = ' (past — droppable now)';
                            else if (futureMs < 30  * 86400e3)       urgency = ' (< 1 month)';
                            else if (futureMs < 365 * 86400e3)       urgency = ' (< 1 year)';
                            else if (futureMs < 3 * 365 * 86400e3)   urgency = ' (1–3 years)';
                            else                                     urgency = ' (> 3 years)';
                        }
                        html += '<div class="tooltip-row"><span class="tooltip-label">Droppable now:</span> ' + pct + '</div>';
                        html += '<div class="tooltip-row"><span class="tooltip-label">Drop time p50:</span> ' + dropStr + urgency + '</div>';
                    } else {
                        html += '<div class="tooltip-row" style="color:#888">No tombstone data available</div>';
                    }
                }

                tooltip.innerHTML = html;
                tooltip.classList.add('show');
                const tipW = tooltip.offsetWidth, tipH = tooltip.offsetHeight;
                const left = e.clientX + 15 + tipW > window.innerWidth  ? e.clientX - 15 - tipW : e.clientX + 15;
                const top  = e.clientY + 15 + tipH > window.innerHeight ? e.clientY - 15 - tipH : e.clientY + 15;
                tooltip.style.left = left + 'px';
                tooltip.style.top  = top  + 'px';
            } else {
                tooltip.classList.remove('show');
                canvas.style.cursor = 'crosshair';
            }
        });

        canvas.addEventListener('mouseleave', () => {
            tooltip.classList.remove('show');
            if (isDragging) {
                isDragging = false;
                document.getElementById('zoom-selection').style.display = 'none';
                dragStart = null;
            }
        });

        canvas.addEventListener('mousedown', (e) => {
            if (!hoveredSstable) {
                isDragging = true;
                const rect = canvas.getBoundingClientRect();
                dragStart = { x: e.clientX - rect.left };
            }
        });

        canvas.addEventListener('mouseup', (e) => {
            if (isDragging && dragStart) {
                isDragging = false;
                document.getElementById('zoom-selection').style.display = 'none';
                const rect = canvas.getBoundingClientRect();
                const endX = e.clientX - rect.left;
                if (Math.abs(endX - dragStart.x) > 10) {
                    const chartWidth = canvas.width - MARGIN.left - MARGIN.right;
                    const zoom  = getZoom();
                    const range = zoom.max - zoom.min;
                    const x1    = Math.min(dragStart.x, endX) - MARGIN.left;
                    const x2    = Math.max(dragStart.x, endX) - MARGIN.left;
                    setZoom({
                        min: zoom.min + (x1 / chartWidth) * range,
                        max: zoom.min + (x2 / chartWidth) * range
                    });
                    draw();
                }
                dragStart = null;
            }
        });

        container.addEventListener('scroll', draw);
        window.addEventListener('resize', draw);

        draw();
    </script>
</body>
</html>
HTMLEOF

# ===== SECTION: ASSEMBLY =====
echo "Generating HTML visualization..." >&2
cat "$HTML_HEAD" "$EVENTS_FILE" "$HTML_TAIL" > "$OUTPUT"

echo "Visualization generated: $OUTPUT" >&2
echo "" >&2
echo "Open in browser with:" >&2
echo "  open $OUTPUT" >&2
