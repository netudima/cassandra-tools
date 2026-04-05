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
# Output format: sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token

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
}

/^Minimum timestamp:/ {
    match($0, /\(([0-9]+)\)/, arr)
    cur_min_ts = arr[1]
}

/^Maximum timestamp:/ {
    match($0, /\(([0-9]+)\)/, arr)
    cur_max_ts = arr[1]
}

/^First token:/ {
    match($0, /First token:[[:space:]]+(-?[0-9]+)/, arr)
    cur_first_token = arr[1]
}

/^Last token:/ {
    match($0, /Last token:[[:space:]]+(-?[0-9]+)/, arr)
    cur_last_token = arr[1]
}

/^IsTransient:/ {
    if (cur_sstable != "" && !(cur_sstable in seen)) {
        seen[cur_sstable] = 1
        print cur_sstable "|" cur_kstable "|" cur_min_ts "|" cur_max_ts "|" cur_first_token "|" cur_last_token
    }
}

END {
    if (cur_sstable != "" && !(cur_sstable in seen)) {
        print cur_sstable "|" cur_kstable "|" cur_min_ts "|" cur_max_ts "|" cur_first_token "|" cur_last_token
    }
}
' "$1"
}

echo "Parsing metadata file: $INFILE" >&2

if [ "$PARSE_ONLY" = true ]; then
    echo "sstable_name|keyspace_table|min_ts_us|max_ts_us|first_token|last_token"
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
        .tooltip-label { color: #999; display: inline-block; min-width: 110px; }
    </style>
</head>
<body>
    <div>
        <h1>Cassandra SSTable Metadata</h1>
        <div class="stats" id="stats"></div>
        <div class="tabs">
            <button class="tab-btn active" id="tab-ts" onclick="switchTab('timestamps')">Timestamp Ranges</button>
            <button class="tab-btn" id="tab-tok" onclick="switchTab('tokens')">Token Ranges</button>
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
        const sstables = rawData.trim().split('\n')
            .filter(line => line.length > 0 && !line.startsWith('sstable_name'))
            .map(line => {
                const [name, keyspace, minTs, maxTs, firstToken, lastToken] = line.split('|');
                return {
                    name,
                    keyspace,
                    label: (keyspace || name) + ' / ' + name,
                    minTs: parseInt(minTs),
                    maxTs: parseInt(maxTs),
                    firstToken: Number(firstToken),
                    lastToken: Number(lastToken),
                    firstTokenStr: firstToken,
                    lastTokenStr: lastToken
                };
            });
        // ===END_DATA_PROCESSING===

        const COLORS = [
            '#4CAF50', '#2196F3', '#FF9800', '#E91E63', '#9C27B0',
            '#00BCD4', '#CDDC39', '#FF5722', '#607D8B', '#795548',
            '#F44336', '#03A9F4', '#8BC34A', '#FFC107', '#673AB7'
        ];

        const canvas = document.getElementById('chart-canvas');
        const ctx = canvas.getContext('2d');
        const container = document.getElementById('chart-container');
        const tooltip = document.getElementById('tooltip');

        document.getElementById('stats').textContent =
            sstables.length + ' unique SSTable' + (sstables.length !== 1 ? 's' : '');

        const MARGIN = { top: 50, right: 30, bottom: 20, left: 300 };
        const ROW_HEIGHT = 28;
        const ROW_SPACING = 6;

        let currentTab = 'timestamps';
        let tsZoom = null;
        let tokZoom = null;
        let hoveredSstable = null;
        let isDragging = false;
        let dragStart = null;

        function getZoom() { return currentTab === 'timestamps' ? tsZoom : tokZoom; }
        function setZoom(z) {
            if (currentTab === 'timestamps') tsZoom = z;
            else tokZoom = z;
        }

        function switchTab(tab) {
            currentTab = tab;
            document.getElementById('tab-ts').classList.toggle('active', tab === 'timestamps');
            document.getElementById('tab-tok').classList.toggle('active', tab === 'tokens');
            draw();
        }

        function resetZoom() { setZoom(null); draw(); }

        // ─── Canvas helpers ───────────────────────────────────────────────────

        function setupCanvas() {
            const width = Math.max(container.clientWidth, 800);
            const height = MARGIN.top + sstables.length * (ROW_HEIGHT + ROW_SPACING) + MARGIN.bottom;
            canvas.width = width;
            canvas.height = height;
            container.style.height = Math.min(height + 4, 600) + 'px';
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, 0, width, height);
            return { width, height, chartWidth: width - MARGIN.left - MARGIN.right };
        }

        function drawBarsAndLabels(minVal, maxVal, getMin, getMax, chartWidth) {
            sstables.forEach((s, i) => {
                const y = MARGIN.top + i * (ROW_HEIGHT + ROW_SPACING);
                const range = maxVal - minVal;
                const sMin = getMin(s);
                const sMax = getMax(s);
                const x1 = MARGIN.left + (sMin - minVal) / range * chartWidth;
                const rawW = (sMax - sMin) / range * chartWidth;
                const barW = Math.max(rawW, 3);
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
                const x = MARGIN.left + (val - minVal) / (maxVal - minVal) * chartWidth;
                ctx.fillText(formatFn(val), x, offsetY + MARGIN.top - 4);
                ctx.strokeStyle = '#555';
                ctx.beginPath();
                ctx.moveTo(x, offsetY + MARGIN.top);
                ctx.lineTo(x, offsetY + MARGIN.top - 5);
                ctx.stroke();
            }
        }

        function drawYAxisLabels() {
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
            sstables.forEach((s, i) => {
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

            drawBarsAndLabels(minVal, maxVal, s => s.minTs, s => s.maxTs, chartWidth);

            function formatTs(us) {
                const d = new Date(us / 1000);
                const range = maxVal - minVal;
                if (range < 10e6)   return d.toISOString().substr(11, 12); // < 10s: HH:MM:SS.mmm
                if (range < 3600e6) return d.toISOString().substr(11, 8);  // < 1h: HH:MM:SS
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

            // Full Murmur3 ring (approximate — Number() precision is sufficient for visualization)
            const RING_MIN = -9223372036854775808;
            const RING_MAX =  9223372036854775807;
            if (!tokZoom) tokZoom = { min: RING_MIN, max: RING_MAX };
            const { min: minVal, max: maxVal } = tokZoom;

            drawBarsAndLabels(minVal, maxVal, s => s.firstToken, s => s.lastToken, chartWidth);

            function formatTok(v) {
                const av = Math.abs(v);
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

        function draw() {
            if (currentTab === 'timestamps') drawTimestamps();
            else drawTokens();
        }

        // ─── Tooltip ─────────────────────────────────────────────────────────

        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const cx = e.clientX - rect.left;
            const cy = e.clientY - rect.top;

            if (isDragging && dragStart) {
                const left = Math.min(dragStart.x, cx);
                const w = Math.abs(cx - dragStart.x);
                const zoomDiv = document.getElementById('zoom-selection');
                zoomDiv.style.left = left + 'px';
                zoomDiv.style.top = MARGIN.top + 'px';
                zoomDiv.style.width = w + 'px';
                zoomDiv.style.height = (canvas.height - MARGIN.top - MARGIN.bottom) + 'px';
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
                let html = '<div class="tooltip-row"><strong>' + hoveredSstable.name + '</strong></div>';
                html += '<div class="tooltip-row"><span class="tooltip-label">Table:</span> ' + hoveredSstable.keyspace + '</div>';

                if (currentTab === 'timestamps') {
                    const minD = new Date(hoveredSstable.minTs / 1000).toISOString().replace('T', ' ').replace('Z', ' UTC');
                    const maxD = new Date(hoveredSstable.maxTs / 1000).toISOString().replace('T', ' ').replace('Z', ' UTC');
                    const durUs = hoveredSstable.maxTs - hoveredSstable.minTs;
                    const durStr = durUs < 1000 ? durUs + ' µs'
                        : durUs < 1e6 ? (durUs / 1000).toFixed(2) + ' ms'
                        : (durUs / 1e6).toFixed(3) + ' s';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Min timestamp:</span> ' + minD + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Max timestamp:</span> ' + maxD + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Duration:</span> ' + durStr + '</div>';
                } else {
                    const span = hoveredSstable.lastToken - hoveredSstable.firstToken;
                    const pct = (span / 1.8446744073709552e19 * 100).toFixed(4) + '% of ring';
                    html += '<div class="tooltip-row"><span class="tooltip-label">First token:</span> ' + hoveredSstable.firstTokenStr + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Last token:</span> ' + hoveredSstable.lastTokenStr + '</div>';
                    html += '<div class="tooltip-row"><span class="tooltip-label">Token span:</span> ' + pct + '</div>';
                }

                tooltip.innerHTML = html;
                tooltip.classList.add('show');
                const tipW = tooltip.offsetWidth, tipH = tooltip.offsetHeight;
                const left = e.clientX + 15 + tipW > window.innerWidth ? e.clientX - 15 - tipW : e.clientX + 15;
                const top  = e.clientY + 15 + tipH > window.innerHeight ? e.clientY - 15 - tipH : e.clientY + 15;
                tooltip.style.left = left + 'px';
                tooltip.style.top = top + 'px';
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
                    const zoom = getZoom();
                    const range = zoom.max - zoom.min;
                    const x1 = Math.min(dragStart.x, endX) - MARGIN.left;
                    const x2 = Math.max(dragStart.x, endX) - MARGIN.left;
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
