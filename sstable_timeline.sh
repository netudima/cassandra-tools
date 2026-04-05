#!/bin/bash

# SSTable Timeline Generator
# Parses Cassandra logs and generates an interactive HTML timeline visualization
# Usage: ./sstable_timeline.sh [--parse-only] <logfile> [output.html]
set -euo pipefail

# ===== SECTION: ARGUMENT PARSING & SETUP =====

PARSE_ONLY=false
if [ "${1:-}" = "--parse-only" ]; then
    PARSE_ONLY=true
    shift
fi

if [ $# -lt 1 ]; then
    echo "Usage: $0 [--parse-only] <logfile> [output.html]"
    echo "Example: $0 Jan16_17.log timeline.html"
    echo "         $0 --parse-only Jan16_17.log"
    exit 1
fi

LOGFILE="$1"
OUTPUT="${2:-${LOGFILE%.*}.html}"

if [ ! -f "$LOGFILE" ]; then
    echo "Error: Log file '$LOGFILE' not found"
    exit 1
fi

# ===== SECTION: AWK LOG PARSER =====
# Output format: timestamp|event_type|sstable_name|size_mb|compaction_id|keyspace.table

run_parser() {
gawk '
function parse_size(size_str) {
    if (match(size_str, /([0-9.]+)(GiB|MiB|KiB)/, arr)) {
        val = arr[1]
        unit = arr[2]
        if (unit == "GiB") return val * 1024
        if (unit == "MiB") return val
        if (unit == "KiB") return val / 1024
    }
    return 0
}

function extract_sstable_name(path) {
    # SSTable filename format: prefix-XXXX-suffix[-Data.db]
    # Extract the last path component, strip -Data.db and any trailing comma
    # Note: gawk non-greedy +? does not backtrack into optional (-Data\.db)? group,
    # so use two explicit patterns: with -Data.db first, then without.
    if (match(path, /\/([^\/,]+)-Data\.db[,]?$/, arr)) {
        return arr[1]
    }
    if (match(path, /\/([^\/,]+)[,]?$/, arr)) {
        return arr[1]
    }
    return path
}

function extract_keyspace_table(path) {
    # Path: .../keyspace/tablename-uuid/prefix-XXXX-suffix[-Data.db]
    n = split(path, parts, "/")
    if (n >= 3) {
        keyspace = parts[n-2]
        table_dir = parts[n-1]
        # Strip UUID suffix (dash + 32 hex chars)
        if (match(table_dir, /^(.+)-[0-9a-f]{32}$/, arr)) {
            return keyspace "." arr[1]
        }
        return keyspace "." table_dir
    }
    return ""
}

/Flushed to \[BigTableReader/ {
    match($0, /([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})/, ts)
    timestamp = ts[1]

    match($0, /biggest ([0-9.]+[GMK]iB)/, s)
    size_mb = parse_size(s[1])

    str = $0
    while (match(str, /path=.([^'"'"']+)/, p)) {
        path = p[1]
        rstart = RSTART; rlength = RLENGTH
        sstable = extract_sstable_name(path)
        kstable = extract_keyspace_table(path)
        print timestamp "|flush|" sstable "|" size_mb "||" kstable
        str = substr(str, rstart + rlength)
    }
}

/Compacted.*sstables to/ {
    match($0, /([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})/, ts)
    timestamp = ts[1]

    # Extract compaction UUID
    match($0, /Compacted \(([^)]+)\)/, uuid)
    compaction_id = uuid[1]

    match($0, /sstables to \[([^\]]+)\]/, p)
    bracket_content = p[1]

    match($0, /\.  ([0-9.]+[GMK]iB) to ([0-9.]+[GMK]iB)/, s)
    size_mb = parse_size(s[2])

    n_paths = split(bracket_content, paths, ",")
    for (i = 1; i <= n_paths; i++) {
        path = paths[i]
        if (path == "") continue
        sstable = extract_sstable_name(path)
        kstable = extract_keyspace_table(path)
        print timestamp "|compaction|" sstable "|" size_mb "|" compaction_id "|" kstable
    }
}

/Deleting sstable:/ {
    match($0, /([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})/, ts)
    timestamp = ts[1]

    match($0, /Deleting sstable: (.+)$/, p)
    path = p[1]
    sstable = extract_sstable_name(path)
    kstable = extract_keyspace_table(path)

    print timestamp "|delete|" sstable "|0||" kstable
}
' "$1" | sort
}

echo "Parsing log file: $LOGFILE" >&2

if [ "$PARSE_ONLY" = true ]; then
    echo "timestamp|event_type|sstable_name|size_mb|compaction_id|keyspace.table"
    run_parser "$LOGFILE"
    exit 0
fi

# ===== SECTION: HTML GENERATION =====

# Temporary files
EVENTS_FILE=$(mktemp)
HTML_PART1=$(mktemp)
HTML_PART2=$(mktemp)
trap "rm -f $EVENTS_FILE $HTML_PART1 $HTML_PART2" EXIT

run_parser "$LOGFILE" > "$EVENTS_FILE"

echo "Extracted $(wc -l < "$EVENTS_FILE") events"

# Generate HTML part 1 (before data)
cat > "$HTML_PART1" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cassandra SSTable Timeline</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a1a;
            color: #e0e0e0;
            padding: 20px;
        }

        .container {
            max-width: 100%;
            margin: 0 auto;
        }

        h1 {
            text-align: center;
            margin-bottom: 10px;
            color: #fff;
        }

        .stats {
            text-align: center;
            margin-bottom: 20px;
            color: #aaa;
            font-size: 14px;
        }

        .legend {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 20px;
            font-size: 14px;
        }

        .legend-item {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .legend-color {
            width: 20px;
            height: 20px;
            border-radius: 3px;
        }

        .flush-color { background: #4CAF50; }
        .compaction-color { background: #2196F3; }

        #timeline-container {
            position: relative;
            width: 100%;
            height: 600px;
            background: #2a2a2a;
            border-radius: 8px;
            overflow: auto;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }

        #timeline-canvas {
            position: absolute;
            top: 0;
            left: 0;
            cursor: crosshair;
        }

        #zoom-selection {
            position: absolute;
            border: 2px dashed #2196F3;
            background: rgba(33, 150, 243, 0.1);
            pointer-events: none;
            display: none;
        }

        #tooltip {
            position: fixed;
            background: rgba(0, 0, 0, 0.9);
            color: white;
            padding: 10px 15px;
            border-radius: 6px;
            font-size: 13px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.2s;
            z-index: 1000;
            max-width: 400px;
            border: 1px solid #555;
        }

        #tooltip.show {
            opacity: 1;
        }

        .tooltip-row {
            margin: 3px 0;
        }

        .tooltip-label {
            color: #aaa;
            display: inline-block;
            min-width: 80px;
        }

        .tooltip-hint {
            margin-top: 8px;
            padding-top: 8px;
            border-top: 1px solid #555;
            color: #888;
            font-size: 11px;
            font-style: italic;
        }

        #copy-notification {
            position: fixed;
            top: 20px;
            right: 20px;
            background: #4CAF50;
            color: white;
            padding: 12px 20px;
            border-radius: 6px;
            font-size: 14px;
            opacity: 0;
            transition: opacity 0.3s;
            z-index: 2000;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        }

        #copy-notification.show {
            opacity: 1;
        }

        .controls {
            margin-bottom: 20px;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }

        .control-group {
            display: flex;
            align-items: center;
            gap: 8px;
        }

        label {
            font-size: 14px;
            color: #ccc;
        }

        input[type="checkbox"] {
            width: 18px;
            height: 18px;
            cursor: pointer;
        }

        button {
            padding: 8px 16px;
            background: #2196F3;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }

        button:hover {
            background: #1976D2;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Cassandra SSTable Timeline</h1>
        <div class="stats" id="stats"></div>

        <div class="legend">
            <div class="legend-item">
                <div class="legend-color flush-color"></div>
                <span>Flush (MemTable → SSTable)</span>
            </div>
            <div class="legend-item">
                <div class="legend-color compaction-color"></div>
                <span>Compaction (SSTable merge)</span>
            </div>
            <div class="legend-item">
                <div class="legend-color" style="background: #888888;"></div>
                <span>Pre-existing (created before log period)</span>
            </div>
            <div class="legend-item">
                <span style="color: #FFC107; font-size: 16px; font-weight: bold;">►</span>
                <span>Still Active (at end of log)</span>
            </div>
        </div>

        <div class="controls">
            <div class="control-group">
                <input type="checkbox" id="showFlush" checked>
                <label for="showFlush">Show Flush</label>
            </div>
            <div class="control-group">
                <input type="checkbox" id="showCompaction" checked>
                <label for="showCompaction">Show Compaction</label>
            </div>
            <div class="control-group">
                <input type="checkbox" id="showPreExisting" checked>
                <label for="showPreExisting">Show Pre-existing</label>
            </div>
            <button onclick="resetZoom()">Reset Zoom</button>
        </div>

        <div id="timeline-container">
            <canvas id="timeline-canvas"></canvas>
            <div id="zoom-selection"></div>
        </div>

        <div id="tooltip"></div>
        <div id="copy-notification">Copied to clipboard!</div>
    </div>

    <script>
        // Parse data from bash script
        const rawData = `
EOF

# Generate HTML part 2 (after data)
cat > "$HTML_PART2" << 'EOF'
`;

        // ===BEGIN_DATA_PROCESSING===

        // Parse events
        const events = rawData.trim().split('\n')
            .filter(line => line.length > 0)
            .map(line => {
                const [timestamp, type, name, size, compactionId, keyspace] = line.split('|');
                return {
                    timestamp: new Date(timestamp),
                    type,
                    name,
                    size: parseFloat(size),
                    compactionId: compactionId || null,
                    keyspace: keyspace || ''
                };
            });

        // Build SSTable lifecycle map
        const sstables = new Map();
        const deletions = new Map(); // Track deletions without creation events
        const compactionRelations = new Map(); // Map compaction outputs to inputs

        events.forEach(event => {
            if (event.type === 'flush' || event.type === 'compaction') {
                sstables.set(event.name, {
                    name: event.name,
                    type: event.type,
                    created: event.timestamp,
                    deleted: null,
                    size: event.size,
                    preExisting: false,
                    compactionId: event.compactionId,
                    keyspace: event.keyspace
                });
            } else if (event.type === 'delete') {
                const sstable = sstables.get(event.name);
                if (sstable) {
                    sstable.deleted = event.timestamp;
                    if (!sstable.keyspace && event.keyspace) sstable.keyspace = event.keyspace;
                } else {
                    // Track deletion of SSTable we never saw created
                    deletions.set(event.name, { time: event.timestamp, keyspace: event.keyspace });
                }
            }
        });

        // Find the earliest timestamp to use for pre-existing SSTables
        const minTimestamp = Math.min(...events.map(e => e.timestamp.getTime()));

        // Add pre-existing SSTables (deleted but never created in this log)
        // Do this BEFORE building compaction relationships so they're included
        deletions.forEach(({ time: deletedTime, keyspace }, name) => {
            sstables.set(name, {
                name: name,
                type: 'unknown', // We don't know if it was from flush or compaction
                created: new Date(minTimestamp),
                deleted: deletedTime,
                size: null, // Unknown size
                preExisting: true,
                keyspace: keyspace || ''
            });
        });

        // Build compaction relationships
        // Associate deletions that happen within 30 seconds after a compaction
        sstables.forEach(output => {
            if (output.type === 'compaction' && output.compactionId) {
                const inputs = [];
                const compactionTime = output.created.getTime();

                sstables.forEach(candidate => {
                    if (candidate.deleted &&
                        candidate.deleted.getTime() >= compactionTime &&
                        candidate.deleted.getTime() <= compactionTime + 30000) {
                        // This SSTable was deleted around the time of this compaction
                        inputs.push(candidate.name);
                    }
                });

                if (inputs.length > 0) {
                    compactionRelations.set(output.name, {
                        output: output.name,
                        inputs: inputs,
                        compactionId: output.compactionId
                    });

                    // Create mappings for each input to the output and all sibling inputs
                    inputs.forEach(inputName => {
                        // Get other inputs (siblings) excluding this one
                        const siblingInputs = inputs.filter(name => name !== inputName);

                        const existing = compactionRelations.get(inputName);
                        if (existing) {
                            if (!existing.outputs) existing.outputs = [];
                            existing.outputs.push(output.name);
                            if (!existing.siblingInputs) existing.siblingInputs = [];
                            existing.siblingInputs.push(...siblingInputs);
                        } else {
                            compactionRelations.set(inputName, {
                                outputs: [output.name],
                                siblingInputs: siblingInputs,
                                compactionId: output.compactionId
                            });
                        }
                    });
                }
            }
        });

        // Find the last event timestamp for still-alive SSTables
        const maxEventTime = Math.max(...events.map(e => e.timestamp.getTime()));

        // Mark SSTables that are still alive and set their end time to last event
        sstables.forEach(sstable => {
            if (!sstable.deleted) {
                sstable.deleted = new Date(maxEventTime);
                sstable.stillAlive = true;
            } else {
                sstable.stillAlive = false;
            }
        });

        // Convert to array - now includes all SSTables (deleted and still alive)
        let timelineData = Array.from(sstables.values())
            .sort((a, b) => {
                // Sort pre-existing to bottom, then by size
                if (a.preExisting && !b.preExisting) return 1;
                if (!a.preExisting && b.preExisting) return -1;
                // Handle null sizes (treat as 0 for sorting)
                const sizeA = a.size || 0;
                const sizeB = b.size || 0;
                return sizeA - sizeB;
            });

        // ===END_DATA_PROCESSING===

        // Update stats
        const flushCount = timelineData.filter(s => s.type === 'flush').length;
        const compactionCount = timelineData.filter(s => s.type === 'compaction').length;
        const preExistingCount = timelineData.filter(s => s.preExisting).length;
        const stillAliveCount = timelineData.filter(s => s.stillAlive).length;
        const totalSize = timelineData.reduce((sum, s) => sum + (s.size || 0), 0);

        document.getElementById('stats').innerHTML =
            `Total: ${timelineData.length} SSTables | ` +
            `Flushes: ${flushCount} | Compactions: ${compactionCount} | ` +
            `Pre-existing: ${preExistingCount} | ` +
            `Still Active: ${stillAliveCount} | ` +
            `Total Size: ${(totalSize / 1024).toFixed(2)} GB`;

        // Canvas setup
        const canvas = document.getElementById('timeline-canvas');
        const ctx = canvas.getContext('2d');
        const container = document.getElementById('timeline-container');
        const tooltip = document.getElementById('tooltip');

        // Get time range
        const globalMinTime = Math.min(...timelineData.map(s => s.created.getTime()));
        const globalMaxTime = Math.max(...timelineData.map(s => s.deleted.getTime()));

        // Layout constants
        const MARGIN = { top: 50, right: 50, bottom: 60, left: 100 };
        const ROW_HEIGHT = 18;
        const ROW_SPACING = 2;
        const MIN_BAR_WIDTH = 3; // Minimum pixels for very short-lived SSTables

        let currentFilters = { flush: true, compaction: true, preExisting: true };
        let zoomState = { minTime: globalMinTime, maxTime: globalMaxTime };
        let dragSelection = null;
        let selectedSStable = null; // Track selected SSTable for highlighting
        let highlightedSSTables = new Set(); // Set of related SSTables to highlight

        function getFilteredData() {
            return timelineData.filter(s => {
                if (s.preExisting && !currentFilters.preExisting) return false;
                if (s.type === 'flush' && !currentFilters.flush) return false;
                if (s.type === 'compaction' && !currentFilters.compaction) return false;
                return true;
            });
        }

        function draw() {
            const data = getFilteredData();

            // Set canvas size
            const width = Math.max(container.clientWidth, 1200);
            const height = MARGIN.top + (data.length * (ROW_HEIGHT + ROW_SPACING)) + MARGIN.bottom;

            canvas.width = width;
            canvas.height = height;

            const chartWidth = width - MARGIN.left - MARGIN.right;
            const chartHeight = height - MARGIN.top - MARGIN.bottom;

            // Clear canvas
            ctx.fillStyle = '#2a2a2a';
            ctx.fillRect(0, 0, width, height);

            // Get current scroll position
            const scrollY = container.scrollTop;

            // Helper function to convert time to x coordinate
            function timeToX(time) {
                const t = time.getTime();
                return MARGIN.left + (t - zoomState.minTime) / (zoomState.maxTime - zoomState.minTime) * chartWidth;
            }

            // Helper function to format size
            function formatSize(mb) {
                if (mb === null || mb === undefined) return '?';
                if (mb >= 1024) return (mb / 1024).toFixed(2) + ' GB';
                return mb.toFixed(2) + ' MB';
            }

            // Function to draw time axis (will be called again at the end for sticky header)
            function drawTimeAxis(yOffset) {
                // Draw background for time axis
                ctx.fillStyle = '#2a2a2a';
                ctx.fillRect(0, yOffset, width, MARGIN.top);

                // Draw time axis line
                ctx.strokeStyle = '#555';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(MARGIN.left, yOffset + MARGIN.top);
                ctx.lineTo(MARGIN.left + chartWidth, yOffset + MARGIN.top);
                ctx.stroke();

                // Draw time labels
                ctx.fillStyle = '#aaa';
                ctx.font = '12px monospace';
                ctx.textAlign = 'center';

                const timeRange = zoomState.maxTime - zoomState.minTime;
                const numTicks = 8;
                for (let i = 0; i <= numTicks; i++) {
                    const t = zoomState.minTime + (timeRange * i / numTicks);
                    const x = timeToX(new Date(t));
                    const date = new Date(t);
                    const label = date.toLocaleString('en-US', {
                        month: 'short',
                        day: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit'
                    });

                    ctx.fillText(label, x, yOffset + MARGIN.top - 10);

                    // Tick mark
                    ctx.beginPath();
                    ctx.moveTo(x, yOffset + MARGIN.top);
                    ctx.lineTo(x, yOffset + MARGIN.top - 5);
                    ctx.stroke();
                }
            }

            // Draw initial time axis
            drawTimeAxis(0);

            // Y-axis label
            ctx.save();
            ctx.translate(20, MARGIN.top + chartHeight / 2);
            ctx.rotate(-Math.PI / 2);
            ctx.textAlign = 'center';
            ctx.fillStyle = '#ccc';
            ctx.font = '14px sans-serif';
            ctx.fillText('SSTables (sorted by size ↑)', 0, 0);
            ctx.restore();

            // Draw SSTables
            data.forEach((sstable, index) => {
                const y = MARGIN.top + index * (ROW_HEIGHT + ROW_SPACING);
                let x1 = timeToX(sstable.created);
                let x2 = timeToX(sstable.deleted);
                let barWidth = x2 - x1;

                // Ensure minimum width for very short-lived SSTables
                if (barWidth < MIN_BAR_WIDTH) {
                    const center = (x1 + x2) / 2;
                    x1 = center - MIN_BAR_WIDTH / 2;
                    x2 = center + MIN_BAR_WIDTH / 2;
                    barWidth = MIN_BAR_WIDTH;
                }

                // Color based on type
                let color;
                if (sstable.preExisting) {
                    color = '#888888'; // Gray for pre-existing (unknown type)
                } else {
                    color = sstable.type === 'flush' ? '#4CAF50' : '#2196F3';
                }

                // Determine if this SSTable is highlighted or dimmed
                const isSelected = selectedSStable === sstable.name;
                const isHighlighted = highlightedSSTables.has(sstable.name);
                const isDimmed = selectedSStable && !isSelected && !isHighlighted;

                // Draw bar with appropriate styling
                if (isDimmed) {
                    ctx.globalAlpha = 0.2;
                }
                ctx.fillStyle = color;
                ctx.fillRect(x1, y, barWidth, ROW_HEIGHT);

                // Draw highlight border for selected or related SSTables
                if (isSelected || isHighlighted) {
                    ctx.strokeStyle = isSelected ? '#FFC107' : '#FF9800'; // Amber for selected, orange for related
                    ctx.lineWidth = isSelected ? 3 : 2;
                    ctx.strokeRect(x1, y, barWidth, ROW_HEIGHT);
                }

                ctx.globalAlpha = 1.0; // Reset alpha

                // Draw "still alive" marker at the end for active SSTables
                if (sstable.stillAlive) {
                    ctx.fillStyle = '#FFC107'; // Amber color for "still alive"
                    ctx.beginPath();
                    const markerSize = ROW_HEIGHT * 0.8;
                    // Draw a right-pointing arrow/triangle at the end
                    ctx.moveTo(x2 - markerSize, y + 2);
                    ctx.lineTo(x2, y + ROW_HEIGHT / 2);
                    ctx.lineTo(x2 - markerSize, y + ROW_HEIGHT - 2);
                    ctx.lineTo(x2 - markerSize, y + 2);
                    ctx.closePath();
                    ctx.fill();
                }

                // Draw SSTable name on the bar
                ctx.save();
                ctx.rect(x1, y, barWidth, ROW_HEIGHT); // Clip text to bar width
                ctx.clip();

                ctx.fillStyle = 'white';
                ctx.font = '10px monospace';
                ctx.textAlign = 'left';
                ctx.textBaseline = 'middle';

                // Only draw text if bar is wide enough (at least 40px)
                if (barWidth > 40) {
                    const barLabel = sstable.keyspace ? `${sstable.keyspace}/${sstable.name}` : sstable.name;
                    ctx.fillText(barLabel, x1 + 4, y + ROW_HEIGHT / 2);
                }

                ctx.restore();

                // Store rectangle for hover detection
                sstable._rect = { x: x1, y, width: barWidth, height: ROW_HEIGHT };
            });

            // Draw size labels on Y-axis
            ctx.fillStyle = '#aaa';
            ctx.font = '10px monospace';
            ctx.textAlign = 'right';

            // Show labels for every Nth item to avoid crowding
            const labelEvery = Math.max(1, Math.floor(data.length / 20));
            data.forEach((sstable, index) => {
                if (index % labelEvery === 0) {
                    const y = MARGIN.top + index * (ROW_HEIGHT + ROW_SPACING) + ROW_HEIGHT / 2;
                    ctx.fillText(formatSize(sstable.size), MARGIN.left - 10, y + 3);
                }
            });

            // Redraw time axis on top at scrolled position (sticky header effect)
            if (scrollY > 0) {
                drawTimeAxis(scrollY);
            }
        }

        // Scroll event listener to redraw sticky time axis
        container.addEventListener('scroll', () => {
            draw();
        });

        // Tooltip handling and zoom selection
        let hoveredSstable = null;
        let isDragging = false;
        let dragStart = null;

        canvas.addEventListener('mousemove', (e) => {
            const rect = canvas.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;

            // Handle zoom selection dragging
            if (isDragging && dragStart) {
                const currentX = e.clientX - rect.left;
                dragSelection.end = currentX;

                // Update selection rectangle
                const left = Math.min(dragStart.x, currentX);
                const width = Math.abs(currentX - dragStart.x);

                const zoomSelectionDiv = document.getElementById('zoom-selection');
                zoomSelectionDiv.style.left = left + 'px';
                zoomSelectionDiv.style.top = MARGIN.top + 'px';
                zoomSelectionDiv.style.width = width + 'px';
                zoomSelectionDiv.style.height = (canvas.height - MARGIN.top - MARGIN.bottom) + 'px';
                zoomSelectionDiv.style.display = 'block';

                // Hide tooltip while dragging
                tooltip.classList.remove('show');
                return;
            }

            // Handle tooltip
            const data = getFilteredData();
            hoveredSstable = null;

            for (const sstable of data) {
                const r = sstable._rect;
                if (r && x >= r.x && x <= r.x + r.width && y >= r.y && y <= r.y + r.height) {
                    hoveredSstable = sstable;
                    break;
                }
            }

            if (hoveredSstable && !isDragging) {
                const lifetime = (hoveredSstable.deleted - hoveredSstable.created) / 1000;
                const hours = Math.floor(lifetime / 3600);
                const minutes = Math.floor((lifetime % 3600) / 60);
                const seconds = Math.floor(lifetime % 60);

                let tooltipContent = `<div class="tooltip-row"><strong>${hoveredSstable.name}</strong></div>` +
                    (hoveredSstable.keyspace ? `<div class="tooltip-row"><span class="tooltip-label">Table:</span> ${hoveredSstable.keyspace}</div>` : '');

                if (hoveredSstable.preExisting) {
                    tooltipContent += `
                        <div class="tooltip-row"><span class="tooltip-label">Type:</span> Pre-existing (unknown)</div>
                        <div class="tooltip-row"><span class="tooltip-label">Size:</span> Unknown</div>
                        <div class="tooltip-row"><span class="tooltip-label">Created:</span> Before log period</div>
                    `;
                    if (hoveredSstable.stillAlive) {
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Status:</span> Still Active (end of log)</div>`;
                    } else {
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Deleted:</span> ${hoveredSstable.deleted.toLocaleString()}</div>`;
                    }
                    tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">In Log:</span> ${hours}h ${minutes}m ${seconds}s</div>`;
                } else {
                    tooltipContent += `
                        <div class="tooltip-row"><span class="tooltip-label">Type:</span> ${hoveredSstable.type}</div>
                        <div class="tooltip-row"><span class="tooltip-label">Size:</span> ${formatSize(hoveredSstable.size)}</div>
                        <div class="tooltip-row"><span class="tooltip-label">Created:</span> ${hoveredSstable.created.toLocaleString()}</div>
                    `;
                    if (hoveredSstable.stillAlive) {
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Status:</span> Still Active (end of log)</div>`;
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Age:</span> ${hours}h ${minutes}m ${seconds}s</div>`;
                    } else {
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Deleted:</span> ${hoveredSstable.deleted.toLocaleString()}</div>`;
                        tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Lifetime:</span> ${hours}h ${minutes}m ${seconds}s</div>`;
                    }

                    // Show compaction relationships
                    const relation = compactionRelations.get(hoveredSstable.name);
                    if (relation) {
                        if (relation.inputs && relation.inputs.length > 0) {
                            tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Inputs:</span> ${relation.inputs.length} SSTable(s)</div>`;
                        }
                        if (relation.outputs && relation.outputs.length > 0) {
                            tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Output:</span> ${relation.outputs.length} SSTable(s)</div>`;
                        }
                        if (relation.siblingInputs && relation.siblingInputs.length > 0) {
                            tooltipContent += `<div class="tooltip-row"><span class="tooltip-label">Co-inputs:</span> ${relation.siblingInputs.length} SSTable(s)</div>`;
                        }
                    }
                }

                tooltipContent += `<div class="tooltip-hint">Click to copy & highlight related SSTables</div>`;
                tooltip.innerHTML = tooltipContent;
                tooltip.classList.add('show');
                const tipW = tooltip.offsetWidth;
                const tipH = tooltip.offsetHeight;
                const left = e.clientX + 15 + tipW > window.innerWidth ? e.clientX - 15 - tipW : e.clientX + 15;
                const top = e.clientY + 15 + tipH > window.innerHeight ? e.clientY - 15 - tipH : e.clientY + 15;
                tooltip.style.left = left + 'px';
                tooltip.style.top = top + 'px';

                canvas.style.cursor = 'pointer';
            } else {
                tooltip.classList.remove('show');
                canvas.style.cursor = 'crosshair';
            }
        });

        canvas.addEventListener('mouseleave', () => {
            tooltip.classList.remove('show');
            canvas.style.cursor = 'crosshair';
            if (isDragging) {
                isDragging = false;
                document.getElementById('zoom-selection').style.display = 'none';
                dragStart = null;
                dragSelection = null;
            }
        });

        // Click to select/highlight and copy SSTable name
        const copyNotification = document.getElementById('copy-notification');
        let copyTimeout;

        canvas.addEventListener('click', (e) => {
            if (hoveredSstable) {
                const clickedName = hoveredSstable.name;

                // Toggle selection
                if (selectedSStable === clickedName) {
                    // Deselect
                    selectedSStable = null;
                    highlightedSSTables.clear();
                } else {
                    // Select and find related SSTables
                    selectedSStable = clickedName;
                    highlightedSSTables.clear();
                    highlightedSSTables.add(clickedName);

                    // Find related SSTables from compaction relationships
                    const relation = compactionRelations.get(clickedName);
                    if (relation) {
                        // Add inputs if this is a compaction output
                        if (relation.inputs) {
                            relation.inputs.forEach(name => highlightedSSTables.add(name));
                        }
                        // Add outputs if this is a compaction input
                        if (relation.outputs) {
                            relation.outputs.forEach(name => highlightedSSTables.add(name));
                        }
                        // Add sibling inputs if this is a compaction input
                        if (relation.siblingInputs) {
                            relation.siblingInputs.forEach(name => highlightedSSTables.add(name));
                        }
                    }
                }

                // Redraw to show highlighting
                draw();

                // Copy to clipboard
                navigator.clipboard.writeText(clickedName).then(() => {
                    // Show notification
                    copyNotification.classList.add('show');

                    // Clear any existing timeout
                    if (copyTimeout) clearTimeout(copyTimeout);

                    // Hide notification after 2 seconds
                    copyTimeout = setTimeout(() => {
                        copyNotification.classList.remove('show');
                    }, 2000);
                }).catch(err => {
                    console.error('Failed to copy:', err);
                    // Fallback for older browsers
                    try {
                        const textArea = document.createElement('textarea');
                        textArea.value = clickedName;
                        textArea.style.position = 'fixed';
                        textArea.style.left = '-9999px';
                        document.body.appendChild(textArea);
                        textArea.select();
                        document.execCommand('copy');
                        document.body.removeChild(textArea);

                        copyNotification.classList.add('show');
                        if (copyTimeout) clearTimeout(copyTimeout);
                        copyTimeout = setTimeout(() => {
                            copyNotification.classList.remove('show');
                        }, 2000);
                    } catch (fallbackErr) {
                        console.error('Fallback copy failed:', fallbackErr);
                    }
                });
            }
        });

        function formatSize(mb) {
            if (mb === null || mb === undefined) return '?';
            if (mb >= 1024) return (mb / 1024).toFixed(2) + ' GB';
            return mb.toFixed(2) + ' MB';
        }

        // Filter controls
        document.getElementById('showFlush').addEventListener('change', (e) => {
            currentFilters.flush = e.target.checked;
            draw();
        });

        document.getElementById('showCompaction').addEventListener('change', (e) => {
            currentFilters.compaction = e.target.checked;
            draw();
        });

        document.getElementById('showPreExisting').addEventListener('change', (e) => {
            currentFilters.preExisting = e.target.checked;
            draw();
        });

        function resetZoom() {
            zoomState.minTime = globalMinTime;
            zoomState.maxTime = globalMaxTime;
            container.scrollLeft = 0;
            container.scrollTop = 0;
            draw();
        }

        // Mouse down to start zoom selection
        canvas.addEventListener('mousedown', (e) => {
            // Only start drag if not hovering over an SSTable (to preserve click-to-copy)
            if (!hoveredSstable) {
                isDragging = true;
                const rect = canvas.getBoundingClientRect();
                dragStart = {
                    x: e.clientX - rect.left,
                    y: e.clientY - rect.top,
                    scrollLeft: container.scrollLeft,
                    scrollTop: container.scrollTop
                };
                dragSelection = { start: dragStart.x, end: dragStart.x };
            }
        });

        // Mouse up to complete zoom selection
        canvas.addEventListener('mouseup', (e) => {
            if (isDragging && dragStart) {
                isDragging = false;
                document.getElementById('zoom-selection').style.display = 'none';

                const rect = canvas.getBoundingClientRect();
                const endX = e.clientX - rect.left;

                // Only zoom if drag was significant (more than 10 pixels)
                if (Math.abs(endX - dragStart.x) > 10) {
                    // Convert x coordinates to time
                    const chartWidth = canvas.width - MARGIN.left - MARGIN.right;
                    const timeRange = zoomState.maxTime - zoomState.minTime;

                    const x1 = Math.min(dragStart.x, endX) - MARGIN.left;
                    const x2 = Math.max(dragStart.x, endX) - MARGIN.left;

                    const t1 = zoomState.minTime + (x1 / chartWidth) * timeRange;
                    const t2 = zoomState.minTime + (x2 / chartWidth) * timeRange;

                    // Update zoom state
                    zoomState.minTime = Math.max(globalMinTime, t1);
                    zoomState.maxTime = Math.min(globalMaxTime, t2);

                    // Redraw with new zoom
                    draw();
                }

                dragStart = null;
                dragSelection = null;
            }
        });

        // Initial draw
        draw();

        // Redraw on window resize
        window.addEventListener('resize', draw);
    </script>
</body>
</html>
EOF

# ===== SECTION: ASSEMBLY =====
echo "Generating HTML timeline..."
cat "$HTML_PART1" "$EVENTS_FILE" "$HTML_PART2" > "$OUTPUT"

echo "Timeline generated: $OUTPUT"
echo ""
echo "Open in browser with:"
echo "  open $OUTPUT"
echo ""
echo "Or for other systems:"
echo "  xdg-open $OUTPUT    # Linux"
echo "  start $OUTPUT       # Windows"
