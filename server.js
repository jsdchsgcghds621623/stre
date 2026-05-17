'use strict';

const express = require('express');
const cors = require('cors');
const torrentStream = require('torrent-stream');
const addonInterface = require('./addon');
const { getRouter } = require('stremio-addon-sdk');
const axios = require('axios');
const { execSync } = require('child_process');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.static('public'));

// ─── Config ───────────────────────────────────────────────
const RAM_LIMIT_MB     = parseInt(process.env.RAM_LIMIT_MB)  || 300;
const DISK_LIMIT_MB    = parseInt(process.env.DISK_LIMIT_MB) || 300;
const ENGINE_TIMEOUT   = 10 * 60 * 1000;
const CONNECT_TIMEOUT  = 90000;
const ZOMBIE_TIMEOUT   = 2  * 60 * 1000;
const HOT_POOL_TTL     = 5  * 60 * 1000;
const PREBUFFER_BYTES  = 8  * 1024 * 1024;  // 8 MB – enough for moov atom + initial buffer
const VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v'];

const activeEngines = {};   // infoHash → entry
const hotEngines    = new Map();  // infoHash → timestamp

// ─── Startup metrics registry (kept for dashboard) ────────
const startupMetrics = {};  // infoHash → { createdAt, metadataAt, firstPeerAt, firstChunkAt, playableAt }

// ═══════════════════════════════════════════════════════════
// MEMORY STORAGE  (fixed: no aggressive eviction on startup)
// ═══════════════════════════════════════════════════════════
function createMemoryStorage() {
    // 80 MB per engine — enough to hold startup region without evicting it
    const MAX_MEMORY_BYTES = 80 * 1024 * 1024;

    return function (pieceLength, opts) {
        const store  = new Map();
        let   stored = 0;

        const evict = () => {
            // Evict ONLY pieces far behind the read head, never the first 50
            const keys = [...store.keys()].sort((a, b) => a - b);
            while (stored > MAX_MEMORY_BYTES && keys.length > 50) {
                const oldest = keys.shift();
                // never evict pieces 0-49 (startup region)
                if (oldest < 50) continue;
                stored -= store.get(oldest).buf.length;
                store.delete(oldest);
            }
        };

        return {
            get(index, opts2, cb) {
                if (typeof opts2 === 'function') { cb = opts2; opts2 = {}; }
                const entry = store.get(index);
                if (!entry) return cb(new Error('piece not found'));
                const buf    = entry.buf;
                const offset = (opts2 && opts2.offset) || 0;
                const length = (opts2 && opts2.length != null) ? opts2.length : buf.length - offset;
                cb(null, buf.slice(offset, offset + length));
            },
            put(index, buf, cb) {
                if (store.has(index)) stored -= store.get(index).buf.length;
                store.set(index, { buf, t: Date.now() });
                stored += buf.length;
                evict();
                if (cb) cb(null);
            },
            close(cb)   { store.clear(); stored = 0; if (cb) cb(null); },
            destroy(cb) { store.clear(); stored = 0; if (cb) cb(null); },
        };
    };
}

// ═══════════════════════════════════════════════════════════
// HYDRA BRAIN — RAM management
// ═══════════════════════════════════════════════════════════
let _ramHistory = [];

function getRamUsageMB() {
    return Math.round(process.memoryUsage().rss / 1024 / 1024);
}

function recordRamSample() {
    _ramHistory.push({ t: Date.now(), v: getRamUsageMB() });
    if (_ramHistory.length > 6) _ramHistory.shift();
}

function getRamTrend() {
    if (_ramHistory.length < 2) return 0;
    const oldest = _ramHistory[0], newest = _ramHistory[_ramHistory.length - 1];
    const dtSec  = (newest.t - oldest.t) / 1000;
    return dtSec === 0 ? 0 : (newest.v - oldest.v) / dtSec;
}

function getDynamicLimits(forInfoHash) {
    recordRamSample();
    const ram       = getRamUsageMB();
    const trend     = getRamTrend();
    const engines   = Object.values(activeEngines);
    const numEngines = engines.length || 1;
    const projected  = ram + (trend * 10);
    const effective  = Math.max(ram, Math.min(RAM_LIMIT_MB, projected));
    const headRoom   = Math.max(0, RAM_LIMIT_MB - effective);
    const totalBudget = Math.floor(headRoom * 0.7);

    let perEngineConns;
    if (forInfoHash && activeEngines[forInfoHash]) {
        const me       = activeEngines[forInfoHash];
        const myWeight = me.activeStreams > 0 ? (1 + Math.min(3, me.speedSamples?.slice(-1)[0] || 0)) : 0.1;
        const totalW   = engines.reduce((s, e) => s + (e.activeStreams > 0 ? (1 + Math.min(3, e.speedSamples?.slice(-1)[0] || 0)) : 0.1), 0);
        const myShare  = totalW > 0 ? myWeight / totalW : 1 / numEngines;
        perEngineConns = Math.floor(totalBudget * myShare);
    } else {
        perEngineConns = Math.floor(totalBudget / numEngines);
    }

    const pressureRatio = Math.max(0, Math.min(1, effective / RAM_LIMIT_MB));
    perEngineConns = Math.max(1, Math.min(80, Math.floor(perEngineConns * (0.3 + 0.7 * Math.pow(1 - pressureRatio, 1.5)))));

    let mode = 'HIGH';
    if      (effective > 195) mode = 'EMERGENCY';
    else if (effective > 185) mode = 'CRITICAL';
    else if (effective > 170) mode = 'SEVERE';
    else if (effective > 150) mode = 'LOW';
    else if (effective > 120) mode = 'MEDIUM';
    else if (effective > 100) mode = 'BALANCED';

    const trendStr = trend >= 0 ? `+${trend.toFixed(1)}` : trend.toFixed(1);
    return { connections: perEngineConns, mode, label: `${mode} | 🧠${ram}MB ${trendStr}MB/s | ${perEngineConns}c`, ram, trend };
}

// ═══════════════════════════════════════════════════════════
// TRACKERS & MAGNET
// ═══════════════════════════════════════════════════════════
function getTrackers() {
    return [
        'udp://tracker.opentrackr.org:1337/announce',
        'http://tracker.opentrackr.org:1337/announce',
        'udp://open.demonii.com:1337/announce',
        'udp://tracker.torrent.eu.org:451/announce',
        'udp://open.stealth.si:80/announce',
        'udp://exodus.desync.com:6969/announce',
        'udp://tracker.openbittorrent.com:6969/announce',
        'http://tracker.openbittorrent.com:80/announce',
        'udp://9.rarbg.com:2810/announce',
        'udp://bt1.archive.org:6969/announce',
        'udp://bt2.archive.org:6969/announce',
        'udp://utracker.ghostchu-services.top:6969/announce',
        'udp://tracker.qu.ax:6969/announce',
        'udp://tracker.dler.org:6969/announce',
        'udp://tracker.moeking.me:6969/announce',
        'udp://tracker.tiny-vps.com:6969/announce',
        'udp://tracker.army:6969/announce',
        'udp://tracker.bitsearch.to:1337/announce',
        'udp://tracker.bt4g.com:2095/announce',
        'udp://explodie.org:6969/announce',
        'https://tracker.gbitt.info:443/announce',
        'https://tracker.loligirl.cn:443/announce',
        'https://tracker.tamersunion.org:443/announce',
        'http://t.overflow.biz:6969/announce',
    ];
}

function buildMagnet(infoHash) {
    const params = getTrackers().map(t => `&tr=${encodeURIComponent(t)}`).join('');
    return `magnet:?xt=urn:btih:${infoHash}${params}`;
}

// ═══════════════════════════════════════════════════════════
// VIDEO FILE HELPERS
// ═══════════════════════════════════════════════════════════
function isVideoFile(name) {
    const lower = name.toLowerCase();
    return VIDEO_EXTENSIONS.some(e => lower.endsWith(e));
}

function findVideoFile(files, fileIdx) {
    if (fileIdx !== undefined && fileIdx !== null && files[fileIdx]) return files[fileIdx];
    let best = null, bestSize = 0;
    for (const f of files) {
        if (isVideoFile(f.name) && f.length > bestSize) { best = f; bestSize = f.length; }
    }
    return best;
}

// ═══════════════════════════════════════════════════════════
// PIECE PRIORITIZATION  (THE CORE FIX)
// ═══════════════════════════════════════════════════════════
function prioritizeStartupPieces(engine, file) {
    if (!engine.torrent || !engine.torrent.pieceLength) return;

    const pieceLen   = engine.torrent.pieceLength;
    const startPiece = Math.floor(file.offset / pieceLen);
    const totalPieces = Math.ceil(file.length / pieceLen);
    const urgentCount = Math.min(60, totalPieces);          // first 60 pieces = critical
    const warmCount   = Math.min(200, totalPieces);         // next 200 = high priority

    // Deselect everything, then select only our file
    try { engine.files.forEach(f => f.deselect()); } catch (_) {}
    try { file.select(); }                           catch (_) {}

    // Mark startup region as critical (highest priority = 10)
    try { engine.critical(startPiece, startPiece + urgentCount); } catch (_) {}

    // High-priority warm zone
    try { engine.select(startPiece, startPiece + warmCount, 10); } catch (_) {}

    console.log(`[Priority] Pieces ${startPiece}–${startPiece + urgentCount} marked CRITICAL (pieceLen=${(pieceLen/1024).toFixed(0)}KB)`);
}

// ═══════════════════════════════════════════════════════════
// PREBUFFER — wait until we have PREBUFFER_BYTES readable
// ═══════════════════════════════════════════════════════════
function prebuffer(file, infoHashShort) {
    return new Promise(resolve => {
        try {
            const end    = Math.min(PREBUFFER_BYTES - 1, file.length - 1);
            const stream = file.createReadStream({ start: 0, end, highWaterMark: 256 * 1024 });
            let received = 0;
            stream.on('data', chunk => {
                received += chunk.length;
                if (received >= Math.min(PREBUFFER_BYTES, file.length)) {
                    stream.destroy();
                    console.log(`[Prebuffer:${infoHashShort}] ✅ ${(received / 1024 / 1024).toFixed(1)} MB ready`);
                    resolve();
                }
            });
            stream.on('end',   () => resolve());
            stream.on('error', () => resolve());   // non-fatal; let streaming try anyway
            // Timeout safety — don't block forever
            setTimeout(resolve, 20000);
        } catch (e) { resolve(); }
    });
}

// ═══════════════════════════════════════════════════════════
// DISK GUARD
// ═══════════════════════════════════════════════════════════
function getTmpDiskMB() {
    try {
        const out = execSync('du -sm /tmp/torrent-stream 2>/dev/null || echo 0').toString().trim();
        return parseInt(out.split('\t')[0]) || 0;
    } catch (_) { return 0; }
}
function purgeTmpIfNeeded() {
    if (getTmpDiskMB() < DISK_LIMIT_MB) return;
    try {
        const cacheDir   = '/tmp/torrent-stream/torrent-stream';
        const activeSet  = new Set(Object.keys(activeEngines));
        if (!fs.existsSync(cacheDir)) return;
        for (const hash of fs.readdirSync(cacheDir)) {
            if (!activeSet.has(hash)) {
                try { execSync(`rm -rf "${cacheDir}/${hash}"`); } catch (_) {}
            }
        }
    } catch (_) {}
}
setInterval(purgeTmpIfNeeded, 60_000);

// ═══════════════════════════════════════════════════════════
// ENGINE LIFECYCLE
// ═══════════════════════════════════════════════════════════
function destroyEngine(infoHash, force = false) {
    const entry = activeEngines[infoHash];
    if (!entry) return;
    if (entry.activeStreams > 0 && !force) { resetEngineTimeout(infoHash); return; }

    clearTimeout(entry.timeout);
    if (entry.logInterval) clearInterval(entry.logInterval);
    hotEngines.delete(infoHash);
    delete activeEngines[infoHash];
    console.log(`[Engine] Destroying: ${infoHash.substring(0, 8)}… (active: ${Object.keys(activeEngines).length})`);

    try {
        entry.engine.remove(false, err => {
            if (err) { try { entry.engine.destroy(); } catch (_) {} }
            console.log(`[Engine] ✨ Flushed: ${infoHash.substring(0, 8)}`);
        });
    } catch (_) { try { entry.engine.destroy(); } catch (_) {} }
}

function resetEngineTimeout(infoHash) {
    const entry = activeEngines[infoHash];
    if (!entry) return;
    entry.lastAccess = Date.now();
    clearTimeout(entry.timeout);
    const dur = entry.activeStreams > 0 ? ENGINE_TIMEOUT : 3 * 60 * 1000;
    entry.timeout = setTimeout(() => destroyEngine(infoHash), dur);
}

function evictIfNeeded() {
    if (getRamUsageMB() <= RAM_LIMIT_MB) return;
    const keys = Object.keys(activeEngines);

    // 1. Zombie (no streams + stalled)
    const zombie = keys.find(k => activeEngines[k].activeStreams === 0 && Date.now() - (activeEngines[k].lastNonZeroSpeed || 0) > ZOMBIE_TIMEOUT);
    if (zombie) { destroyEngine(zombie); return; }

    // 2. Oldest idle
    let oldest = null, oldestT = Infinity;
    for (const k of keys) {
        if (activeEngines[k].activeStreams === 0 && activeEngines[k].lastAccess < oldestT) {
            oldestT = activeEngines[k].lastAccess; oldest = k;
        }
    }
    if (oldest) { destroyEngine(oldest); return; }

    // 3. Slowest active (emergency only)
    const lim = getDynamicLimits();
    if (lim.mode === 'EMERGENCY' || lim.mode === 'CRITICAL') {
        let slowest = null, slowestSpd = Infinity;
        for (const k of keys) {
            const avg = (activeEngines[k].speedSamples || []).reduce((a, b) => a + b, 0) / ((activeEngines[k].speedSamples?.length) || 1);
            if (avg < slowestSpd) { slowestSpd = avg; slowest = k; }
        }
        if (slowest) destroyEngine(slowest, true);
    }
}

function getOrCreateEngine(infoHash) {
    if (activeEngines[infoHash]) {
        resetEngineTimeout(infoHash);
        return { engine: activeEngines[infoHash].engine, entry: activeEngines[infoHash] };
    }

    evictIfNeeded();

    const limits     = getDynamicLimits(infoHash);
    const magnet     = buildMagnet(infoHash);
    const shortHash  = infoHash.substring(0, 8);

    console.log(`[Engine] Creating (${limits.label}): ${shortHash}…`);

    const engine = torrentStream(magnet, {
        tmp:         '/tmp/torrent-stream',
        connections: limits.connections,
        uploads:     0,
        verify:      false,
        dht:         true,
        tracker:     true,
        storage:     createMemoryStorage(),
    });

    const entry = {
        engine,
        isReady:          false,
        activeStreams:     0,
        lastAccess:       Date.now(),
        createdAt:        Date.now(),
        lastNonZeroSpeed: Date.now(),
        speedSamples:     [],
        state:            'connecting',   // connecting → metadata → ready → streaming
        peersFound:       0,
        prebuffered:      false,
        selectedFile:     null,
    };

    activeEngines[infoHash] = entry;

    // Startup metrics
    startupMetrics[infoHash] = { createdAt: Date.now(), metadataAt: 0, firstPeerAt: 0, firstChunkAt: 0, playableAt: 0 };

    resetEngineTimeout(infoHash);

    // ── Peer tracking ────────────────────────────────────
    engine.swarm?.on('wire', () => {
        entry.peersFound++;
        if (startupMetrics[infoHash] && !startupMetrics[infoHash].firstPeerAt) {
            startupMetrics[infoHash].firstPeerAt = Date.now();
        }
    });

    // ── Metadata fast-path ───────────────────────────────
    engine.on('metadata', () => {
        entry.state = 'metadata';
        if (startupMetrics[infoHash]) startupMetrics[infoHash].metadataAt = Date.now();
        try {
            if (!engine.files?.length) return;
            const file = findVideoFile(engine.files);
            if (!file) return;
            entry.selectedFile = file;
            prioritizeStartupPieces(engine, file);
            console.log(`[Engine:${shortHash}] 📂 Metadata: selected "${file.name}" (${(file.length/1024/1024).toFixed(0)} MB)`);
            // Start prebuffer in background (non-blocking)
            prebuffer(file, shortHash).then(() => {
                entry.prebuffered = true;
                if (startupMetrics[infoHash]) startupMetrics[infoHash].playableAt = Date.now();
            });
        } catch (_) {}
    });

    // ── Ready event ──────────────────────────────────────
    engine.on('ready', () => {
        entry.isReady = true;
        entry.state   = 'ready';
        console.log(`[Engine:${shortHash}] ✅ Ready (${engine.files.length} files)`);
        try {
            const file = findVideoFile(engine.files);
            if (!file) return;
            entry.selectedFile = file;
            prioritizeStartupPieces(engine, file);
            if (!entry.prebuffered) {
                prebuffer(file, shortHash).then(() => {
                    entry.prebuffered = true;
                    if (startupMetrics[infoHash]) startupMetrics[infoHash].playableAt = Date.now();
                });
            }
        } catch (_) {}
    });

    // ── Speed manager ────────────────────────────────────
    let slowTick = 0;
    entry.logInterval = setInterval(() => {
        if (!engine.swarm) return;
        const speedMb = (engine.swarm.downloadSpeed() / 1024 / 1024);
        const peers   = engine.swarm.wires.length;

        if (speedMb >= 0.1) entry.lastNonZeroSpeed = Date.now();

        entry.speedSamples.push(speedMb);
        if (entry.speedSamples.length > 6) entry.speedSamples.shift();

        const lim = getDynamicLimits(infoHash);

        // Adjust swarm size live
        if (engine.swarm.size !== lim.connections) engine.swarm.size = lim.connections;

        // Re-announce if budget grew and peers thin
        const timeSinceAnnounce = Date.now() - (entry._lastAnnounce || 0);
        if (lim.connections - (entry._prevPeerLimit || 0) >= 10 && timeSinceAnnounce > 30000 && peers < lim.connections * 0.5) {
            try { engine.swarm.announce?.(); engine.discovery?.lookup?.(); entry._lastAnnounce = Date.now(); } catch (_) {}
        }
        entry._prevPeerLimit = lim.connections;

        // Prune slow peers
        if (peers > lim.connections) {
            const excess = peers - lim.connections;
            const HOG    = 0.2 * 1024 * 1024;
            const sorted = [...engine.swarm.wires].sort((a, b) => {
                const sa = a.downloadSpeed?.() || 0, sb = b.downloadSpeed?.() || 0;
                return (sa >= HOG ? 1 : 0) - (sb >= HOG ? 1 : 0) || sa - sb;
            });
            let pruned = 0;
            for (let i = 0; i < excess; i++) {
                const spd = sorted[i]?.downloadSpeed?.() || 0;
                if (spd >= HOG) break;
                try { sorted[i].destroy(); pruned++; } catch (_) {}
            }
            if (pruned) console.log(`[SpeedMgr:${shortHash}] ✂️ Pruned ${pruned} slow peers`);
        }

        // Periodic dead-peer sweep
        slowTick++;
        if (slowTick >= 6) {
            slowTick = 0;
            let evicted = 0;
            for (const wire of [...engine.swarm.wires]) {
                try { if ((wire.downloadSpeed?.() || 0) === 0 && peers > 10 && wire.peerChoking) { wire.destroy(); evicted++; } } catch (_) {}
            }
            if (evicted) console.log(`[SpeedMgr:${shortHash}] 🚫 Evicted ${evicted} dead peers`);
        }

        if (speedMb > 0 || peers > 0) {
            const avg   = entry.speedSamples.reduce((a, b) => a + b, 0) / entry.speedSamples.length;
            const warn  = getRamUsageMB() > RAM_LIMIT_MB * 0.9 ? ' (!) RAM' : '';
            console.log(`[Engine:${shortHash}] ⚡ ${speedMb.toFixed(2)} MB/s | 👥 ${peers}p | 🎬 ${entry.activeStreams} active | avg:${avg.toFixed(2)}${warn}`);
        }
    }, 5000);

    // ── Global zombie scanner (once) ─────────────────────
    if (!global._zombieScannerStarted) {
        global._zombieScannerStarted = true;
        setInterval(() => {
            if (getRamUsageMB() > RAM_LIMIT_MB) evictIfNeeded();
            for (const [hash, e] of Object.entries(activeEngines)) {
                const stalled = Date.now() - (e.lastNonZeroSpeed || 0) > ZOMBIE_TIMEOUT;
                if (e.activeStreams === 0 && stalled) { destroyEngine(hash); continue; }
                if (e.activeStreams === 0 && e.speedSamples?.length >= 6) {
                    const avg = e.speedSamples.reduce((a, b) => a + b, 0) / e.speedSamples.length;
                    if (avg < 0.15 && Date.now() - e.createdAt > 120000) {
                        try { e.engine.swarm?.announce?.(); } catch (_) {}
                    }
                }
            }
        }, 30_000);
    }

    return { engine, entry };
}

// ═══════════════════════════════════════════════════════════
// STREAM SERVING  (THE CORE FIX)
// ═══════════════════════════════════════════════════════════
function serveVideoFile(file, req, res, infoHash) {
    resetEngineTimeout(infoHash);

    const entry = activeEngines[infoHash];
    if (entry) { entry.activeStreams++; entry.state = 'streaming'; }
    if (startupMetrics[infoHash] && !startupMetrics[infoHash].firstChunkAt)
        startupMetrics[infoHash].firstChunkAt = Date.now();

    req.on('close', () => {
        if (entry) { entry.activeStreams = Math.max(0, entry.activeStreams - 1); resetEngineTimeout(infoHash); }
    });

    const totalSize = file.length;
    const ext       = file.name.split('.').pop().toLowerCase();
    const mime      = ({ mp4:'video/mp4', mkv:'video/x-matroska', avi:'video/x-msvideo', mov:'video/quicktime',
                         wmv:'video/x-ms-wmv', flv:'video/x-flv', webm:'video/webm', m4v:'video/mp4' })[ext]
                      || 'application/octet-stream';

    const rangeHeader = req.headers.range;
    const shortHash   = infoHash.substring(0, 8);

    let start = 0, end = totalSize - 1;

    if (rangeHeader) {
        const parts = rangeHeader.replace(/bytes=/, '').split('-');
        start = parseInt(parts[0], 10) || 0;
        end   = parts[1] ? parseInt(parts[1], 10) : totalSize - 1;
    }

    const chunkSize = end - start + 1;

    // ── Send headers IMMEDIATELY (fixes player stall) ────
    if (rangeHeader) {
        res.writeHead(206, {
            'Content-Range':  `bytes ${start}-${end}/${totalSize}`,
            'Accept-Ranges':  'bytes',
            'Content-Length': chunkSize,
            'Content-Type':   mime,
            'Connection':     'keep-alive',
            'Cache-Control':  'no-store',
        });
    } else {
        res.writeHead(200, {
            'Content-Length': totalSize,
            'Content-Type':   mime,
            'Accept-Ranges':  'bytes',
            'Connection':     'keep-alive',
            'Cache-Control':  'no-store',
        });
    }
    res.flushHeaders();   // ← critical: send headers before any data arrives

    console.log(`[Stream:${shortHash}] ${rangeHeader ? `Range ${start}-${end}` : 'Full'} (${(chunkSize/1024/1024).toFixed(1)} MB) — "${file.name}"`);

    // ── If this is a seek far into the file, prioritize that region ──
    if (start > 0) {
        const entry = activeEngines[infoHash];
        const engine = entry?.engine;
        if (engine?.torrent?.pieceLength) {
            const pieceLen   = engine.torrent.pieceLength;
            const seekPiece  = Math.floor((file.offset + start) / pieceLen);
            const urgentEnd  = seekPiece + 30;
            try { engine.critical(seekPiece, urgentEnd); } catch (_) {}
            console.log(`[Stream:${shortHash}] 🎯 Seek prioritized pieces ${seekPiece}–${urgentEnd}`);
        }
    }

    // ── Manual pipe with backpressure (more reliable than .pipe) ──
    const stream = file.createReadStream({
        start,
        end,
        highWaterMark: 256 * 1024,   // 256 KB — smaller = faster first chunk
    });

    let firstChunk = true;
    stream.on('data', chunk => {
        if (firstChunk) {
            firstChunk = false;
            console.log(`[Stream:${shortHash}] 📦 First chunk sent (${chunk.length} bytes)`);
        }
        if (!res.write(chunk)) {
            stream.pause();
            res.once('drain', () => stream.resume());
        }
    });

    stream.on('end', () => {
        console.log(`[Stream:${shortHash}] ✅ Stream complete`);
        res.end();
    });

    stream.on('error', err => {
        console.error(`[Stream:${shortHash}] ❌ Read error: ${err.message}`);
        if (!res.writableEnded) res.end();
    });

    res.on('close', () => stream.destroy());
}

// ═══════════════════════════════════════════════════════════
// ROUTES
// ═══════════════════════════════════════════════════════════

// ── Health ───────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.json({
        status:          'ok',
        version:         '3.7.0',
        dashboard:       `https://${req.get('host')}/dashboard`,
        activeEngines:   Object.keys(activeEngines).length,
        hotEngines:      hotEngines.size,
        ramUsageMB:      getRamUsageMB(),
        ramTrendMBs:     getRamTrend().toFixed(2),
        dynamicMode:     getDynamicLimits().mode,
        activePeerBudget:getDynamicLimits().connections,
        ramLimitMB:      RAM_LIMIT_MB,
        uptime:          process.uptime(),
    });
});

// ── Warm endpoint ────────────────────────────────────────
app.get('/warm/:infoHash', (req, res) => {
    const { infoHash } = req.params;
    if (!/^[0-9a-fA-F]{40}$/.test(infoHash || ''))
        return res.status(400).json({ error: 'Invalid infoHash' });

    const alreadyWarm = hotEngines.has(infoHash) || activeEngines[infoHash]?.isReady;
    if (!alreadyWarm) {
        hotEngines.set(infoHash, Date.now());
        getOrCreateEngine(infoHash);
        console.log(`[Preload] ⚡ Warming: ${infoHash.substring(0, 8)}…`);
    }
    res.json({ status: alreadyWarm ? 'already_warm' : 'warming', infoHash, ready: activeEngines[infoHash]?.isReady || false });
});

// ── Engine status (for dashboard polling) ────────────────
app.get('/api/status', (req, res) => {
    const engines = Object.entries(activeEngines).map(([hash, e]) => {
        const m = startupMetrics[hash] || {};
        return {
            id:           hash.substring(0, 8),
            fullHash:     hash,
            ready:        e.isReady,
            hot:          hotEngines.has(hash),
            state:        e.state,
            activeStreams: e.activeStreams,
            peers:        e.engine.swarm?.wires.length || 0,
            speedMBs:     ((e.engine.swarm?.downloadSpeed() || 0) / 1024 / 1024).toFixed(2),
            downloaded:   ((e.engine.swarm?.downloaded || 0) / 1024 / 1024).toFixed(1),
            prebuffered:  e.prebuffered,
            selectedFile: e.selectedFile?.name || null,
            fileSizeMB:   e.selectedFile ? (e.selectedFile.length / 1024 / 1024).toFixed(0) : null,
            lastAccess:   new Date(e.lastAccess).toLocaleTimeString(),
            metadataMs:   m.metadataAt  ? m.metadataAt  - m.createdAt : null,
            firstPeerMs:  m.firstPeerAt ? m.firstPeerAt - m.createdAt : null,
            playableMs:   m.playableAt  ? m.playableAt  - m.createdAt : null,
        };
    });
    res.json({
        engines,
        ram:      getRamUsageMB(),
        ramLimit: RAM_LIMIT_MB,
        ramTrend: getRamTrend().toFixed(2),
        mode:     getDynamicLimits().mode,
        uptime:   Math.round(process.uptime()),
        hotCount: hotEngines.size,
    });
});

// ── Stream route ─────────────────────────────────────────
app.get('/stream/:infoHash', (req, res) => {
    const { infoHash } = req.params;
    const fileIdx      = req.query.fileIdx !== undefined ? parseInt(req.query.fileIdx, 10) : undefined;

    console.log(`[Stream] ← ${infoHash.substring(0, 8)} fileIdx=${fileIdx} range=${req.headers.range || 'none'}`);

    const { engine, entry } = getOrCreateEngine(infoHash);
    engine.setMaxListeners(50);

    // ── Zero-wait hot path ───────────────────────────────
    if (engine.files?.length) {
        const file = findVideoFile(engine.files, fileIdx);
        if (!file) return res.status(404).json({ error: 'No video file found' });
        console.log(`[Stream] ⚡ HOT — serving instantly: "${file.name}"`);
        return serveVideoFile(file, req, res, infoHash);
    }

    // ── Cold path — wait for metadata or ready ───────────
    let responded = false;

    const onMetadata = () => {
        if (responded || !engine.files?.length) return;
        const file = findVideoFile(engine.files, fileIdx);
        if (!file) return;
        responded = true; cleanup();
        console.log(`[Stream] 🔥 Metadata fast-path: "${file.name}"`);
        serveVideoFile(file, req, res, infoHash);
    };

    const onReady = () => {
        if (responded) return;
        const file = findVideoFile(engine.files, fileIdx);
        if (!file) { if (!res.headersSent) res.status(404).json({ error: 'No video file found' }); return; }
        responded = true; cleanup();
        console.log(`[Stream] ✅ Ready path: "${file.name}"`);
        serveVideoFile(file, req, res, infoHash);
    };

    const onError = err => {
        if (responded) return;
        responded = true; cleanup();
        console.error(`[Engine Error] ${err.message}`);
        if (!res.headersSent) res.status(500).json({ error: 'Torrent engine error' });
    };

    const timer = setTimeout(() => {
        if (responded) return;
        responded = true; cleanup();
        console.error(`[Stream] Timeout: ${infoHash.substring(0, 8)}`);
        if (!res.headersSent) res.status(504).json({ error: 'Torrent timed out — try a source with more seeders' });
    }, CONNECT_TIMEOUT);

    const cleanup = () => {
        clearTimeout(timer);
        engine.removeListener('metadata', onMetadata);
        engine.removeListener('ready',    onReady);
        engine.removeListener('error',    onError);
    };

    engine.on('metadata', onMetadata);
    engine.once('ready',  onReady);
    engine.once('error',  onError);

    req.on('close', () => {
        if (!responded) { responded = true; cleanup(); }
        console.log(`[Stream] Client disconnected: ${infoHash.substring(0, 8)}`);
    });
});

// ── Debug ────────────────────────────────────────────────
app.get('/debug', async (req, res) => {
    const results = {};
    for (const mirror of ['https://yts.torrentbay.st', 'https://movies-api.accel.li']) {
        try {
            const r = await axios.get(`${mirror}/api/v2/movie_details.json?imdb_id=tt1375666`, { timeout: 10000 });
            results[mirror] = { status: 'ok', torrents: r.data?.data?.movie?.torrents?.length || 0 };
        } catch (err) { results[mirror] = { status: 'error', message: err.message }; }
    }
    for (const [key, url] of [['eztv', 'https://eztvx.to/api/get-torrents?imdb_id=0944947&limit=5'], ['tpb', 'https://apibay.org/q.php?q=test&cat=0']]) {
        try {
            const r = await axios.get(url, { timeout: 10000 });
            results[key] = { status: 'ok', count: Array.isArray(r.data) ? r.data.length : (r.data?.torrents?.length || 0) };
        } catch (err) { results[key] = { status: 'error', message: err.message }; }
    }
    res.json({ version: '3.7.0', results });
});

// ═══════════════════════════════════════════════════════════
// DASHBOARD  (complete rewrite — live polling + stream tester)
// ═══════════════════════════════════════════════════════════
app.get('/dashboard', (req, res) => {
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Hydra Dashboard</title>
<style>
  :root{--bg:#0d0d17;--card:#141422;--border:#2a2a3d;--accent:#8b5cf6;--accent2:#ec4899;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--text:#e2e8f0;--muted:#64748b;}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:20px;min-height:100vh}
  h1{background:linear-gradient(135deg,#a78bfa,#ec4899);-webkit-background-clip:text;-webkit-text-fill-color:transparent;font-size:1.8rem;font-weight:800;margin-bottom:4px}
  .subtitle{color:var(--muted);font-size:.85rem;margin-bottom:20px}
  /* Stats bar */
  .stats-bar{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}
  .stat-pill{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:10px 18px;min-width:130px;flex:1}
  .stat-pill .label{color:var(--muted);font-size:.72rem;text-transform:uppercase;letter-spacing:.05em}
  .stat-pill .value{font-size:1.4rem;font-weight:700;margin-top:2px}
  /* Mode badge */
  .mode{display:inline-block;padding:2px 10px;border-radius:999px;font-size:.75rem;font-weight:700}
  .mode-HIGH{background:#16a34a22;color:#22c55e;border:1px solid #22c55e44}
  .mode-BALANCED{background:#0ea5e922;color:#38bdf8;border:1px solid #38bdf844}
  .mode-MEDIUM{background:#f59e0b22;color:#fbbf24;border:1px solid #fbbf2444}
  .mode-LOW{background:#f97316;color:#fff;border:1px solid #fb923c88}
  .mode-SEVERE,.mode-CRITICAL,.mode-EMERGENCY{background:#ef444422;color:#f87171;border:1px solid #f8717166;animation:pulse-red 1s infinite}
  @keyframes pulse-red{50%{border-color:#ef4444}}
  /* RAM bar */
  .ram-bar-wrap{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:14px 18px;margin-bottom:20px}
  .ram-bar-label{display:flex;justify-content:space-between;font-size:.8rem;color:var(--muted);margin-bottom:8px}
  .ram-bar-bg{background:#1e1e2e;border-radius:4px;height:10px;overflow:hidden}
  .ram-bar-fill{height:100%;border-radius:4px;transition:width .5s,background .3s}
  /* Engines grid */
  .section-title{font-size:.9rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:10px}
  .engines-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px;margin-bottom:24px}
  .engine-card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:16px;transition:border-color .3s}
  .engine-card.hot{border-color:#22c55e66}
  .engine-card.streaming{border-color:var(--accent)}
  .engine-header{display:flex;align-items:center;gap:8px;margin-bottom:12px}
  .engine-id{font-family:monospace;font-weight:700;font-size:.95rem}
  .badge{font-size:.65rem;padding:2px 7px;border-radius:999px;font-weight:700}
  .badge-hot{background:#22c55e22;color:#22c55e;border:1px solid #22c55e44}
  .badge-streaming{background:var(--accent)22;color:var(--accent);border:1px solid var(--accent)44}
  .badge-ready{background:#38bdf822;color:#38bdf8;border:1px solid #38bdf844}
  .badge-connecting{background:var(--muted)22;color:var(--muted)}
  .engine-rows{display:grid;grid-template-columns:1fr 1fr;gap:4px 12px}
  .engine-row{font-size:.78rem;color:var(--muted);display:flex;justify-content:space-between}
  .engine-row span{color:var(--text);font-weight:600}
  .speed-bar{height:3px;border-radius:2px;background:#1e1e2e;margin-top:10px;overflow:hidden}
  .speed-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));border-radius:2px;transition:width .5s}
  .metrics-row{margin-top:10px;display:flex;gap:6px;flex-wrap:wrap}
  .metric-chip{font-size:.68rem;background:#1e1e2e;border-radius:6px;padding:3px 8px;color:var(--muted)}
  .metric-chip b{color:var(--text)}
  /* Stream tester */
  .tester{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:20px;margin-bottom:24px}
  .tester h2{font-size:1rem;font-weight:700;margin-bottom:12px;color:var(--text)}
  .tester-inputs{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
  .tester input{flex:1;min-width:220px;background:#1e1e2e;border:1px solid var(--border);border-radius:8px;color:var(--text);padding:9px 14px;font-size:.88rem;outline:none;transition:border-color .2s}
  .tester input:focus{border-color:var(--accent)}
  .tester input::placeholder{color:var(--muted)}
  .btn{background:linear-gradient(135deg,var(--accent),#6d28d9);color:#fff;border:none;border-radius:8px;padding:9px 20px;font-size:.88rem;font-weight:600;cursor:pointer;transition:opacity .2s,transform .1s}
  .btn:hover{opacity:.9;transform:translateY(-1px)}
  .btn:active{transform:translateY(0)}
  .btn-sm{padding:6px 14px;font-size:.78rem}
  .btn-danger{background:linear-gradient(135deg,#dc2626,#991b1b)}
  .btn-green{background:linear-gradient(135deg,#16a34a,#166534)}
  .tester-status{font-size:.8rem;color:var(--muted);margin-bottom:10px;min-height:1.2em}
  .tester-status.ok{color:var(--green)}
  .tester-status.error{color:var(--red)}
  video{width:100%;border-radius:10px;background:#000;display:none;margin-top:10px;max-height:400px}
  video.visible{display:block}
  /* Timing chart */
  .timing-bar{display:flex;align-items:center;gap:8px;font-size:.75rem;margin:3px 0}
  .timing-seg{height:6px;border-radius:3px;min-width:2px}
  /* Logs */
  .log-box{background:#0a0a12;border:1px solid var(--border);border-radius:10px;padding:12px;font-family:monospace;font-size:.72rem;color:#94a3b8;height:180px;overflow-y:auto;margin-top:10px}
  .log-box .log-entry{margin-bottom:2px;line-height:1.4}
  .log-box .log-ok{color:#22c55e}.log-box .log-err{color:#f87171}.log-box .log-info{color:#38bdf8}
  /* Empty state */
  .empty{color:var(--muted);text-align:center;padding:30px;font-size:.88rem}
  /* Tabs */
  .tabs{display:flex;gap:4px;margin-bottom:16px}
  .tab{padding:7px 16px;border-radius:8px;font-size:.82rem;font-weight:600;cursor:pointer;border:1px solid transparent;color:var(--muted);background:none;transition:all .2s}
  .tab.active{background:var(--card);border-color:var(--border);color:var(--text)}
  .tab-panel{display:none}.tab-panel.active{display:block}
  a.back{color:var(--muted);font-size:.8rem;text-decoration:none}a.back:hover{color:var(--text)}
</style>
</head>
<body>
<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px;margin-bottom:16px">
  <div>
    <h1>📡 Hydra Dashboard</h1>
    <div class="subtitle">Torrent-to-Weblink v3.7.0 — Live monitor &amp; stream tester</div>
  </div>
  <div style="display:flex;gap:8px;align-items:center">
    <span id="mode-badge" class="mode">--</span>
    <span id="last-update" style="font-size:.72rem;color:var(--muted)"></span>
  </div>
</div>

<!-- Stats bar -->
<div class="stats-bar">
  <div class="stat-pill"><div class="label">RAM Used</div><div class="value" id="s-ram" style="color:var(--accent)">--</div></div>
  <div class="stat-pill"><div class="label">RAM Trend</div><div class="value" id="s-trend">--</div></div>
  <div class="stat-pill"><div class="label">Engines</div><div class="value" id="s-engines" style="color:#38bdf8">--</div></div>
  <div class="stat-pill"><div class="label">Hot Pool</div><div class="value" id="s-hot" style="color:var(--green)">--</div></div>
  <div class="stat-pill"><div class="label">Uptime</div><div class="value" id="s-uptime" style="color:#fbbf24">--</div></div>
</div>

<!-- RAM bar -->
<div class="ram-bar-wrap">
  <div class="ram-bar-label"><span>RAM Usage</span><span id="ram-label-text">0 / 0 MB</span></div>
  <div class="ram-bar-bg"><div id="ram-bar-fill" class="ram-bar-fill" style="width:0%;background:var(--green)"></div></div>
</div>

<!-- Tabs -->
<div class="tabs">
  <button class="tab active" onclick="switchTab('engines')">🎬 Engines</button>
  <button class="tab" onclick="switchTab('tester')">🧪 Stream Tester</button>
  <button class="tab" onclick="switchTab('logs')">📋 Logs</button>
</div>

<!-- Engines tab -->
<div class="tab-panel active" id="tab-engines">
  <div class="section-title">Active Engines</div>
  <div class="engines-grid" id="engines-grid">
    <div class="empty">No active engines. Start watching something in Stremio!</div>
  </div>
</div>

<!-- Tester tab -->
<div class="tab-panel" id="tab-tester">
  <div class="tester">
    <h2>🧪 Stream Tester</h2>
    <div class="tester-inputs">
      <input id="hash-input" placeholder="InfoHash (40 hex chars)" maxlength="40" spellcheck="false">
      <input id="fileidx-input" placeholder="File index (optional)" style="max-width:160px" type="number" min="0">
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button class="btn" onclick="warmHash()">🔥 Pre-warm</button>
      <button class="btn btn-green" onclick="testStream()">▶ Test Stream</button>
      <button class="btn btn-danger" onclick="stopStream()">⏹ Stop</button>
      <button class="btn" style="background:#1e1e2e;border:1px solid var(--border)" onclick="copyStreamUrl()">📋 Copy URL</button>
    </div>
    <div class="tester-status" id="tester-status">Enter a 40-char infoHash and click Test Stream.</div>
    <video id="test-video" controls></video>
    <!-- Timing breakdown -->
    <div id="timing-section" style="display:none;margin-top:14px">
      <div class="section-title" style="margin-bottom:8px">⏱ Startup Timing</div>
      <div id="timing-display"></div>
    </div>
  </div>

  <!-- Quick test presets -->
  <div class="tester" style="padding:14px">
    <h2 style="margin-bottom:10px">🔗 Quick Test Presets</h2>
    <div style="display:flex;gap:8px;flex-wrap:wrap" id="presets-area">
      <button class="btn btn-sm" onclick="loadEngineHash()" id="preset-btn">Load from active engine</button>
    </div>
  </div>
</div>

<!-- Logs tab -->
<div class="tab-panel" id="tab-logs">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <div class="section-title">Request Log</div>
    <button class="btn btn-sm" onclick="clearLogs()">Clear</button>
  </div>
  <div class="log-box" id="log-box"></div>
</div>

<div style="margin-top:24px"><a href="/" class="back">← Back to landing page</a></div>

<script>
const LOG_LINES = [];
const MAX_LOGS  = 200;
let   videoEl;

function log(msg, type='info') {
  const d = new Date().toLocaleTimeString();
  LOG_LINES.unshift({ d, msg, type });
  if (LOG_LINES.length > MAX_LOGS) LOG_LINES.pop();
  renderLogs();
}

function renderLogs() {
  const box = document.getElementById('log-box');
  box.innerHTML = LOG_LINES.map(l =>
    \`<div class="log-entry log-\${l.type}">[\${l.d}] \${escHtml(l.msg)}</div>\`
  ).join('');
}

function clearLogs() { LOG_LINES.length = 0; renderLogs(); }
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function switchTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => {
    const names = ['engines','tester','logs'];
    t.classList.toggle('active', names[i] === name);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
}

// ── Status polling ───────────────────────────────────────
async function fetchStatus() {
  try {
    const r    = await fetch('/api/status');
    const data = await r.json();
    updateDashboard(data);
  } catch (e) { log('Status fetch error: ' + e.message, 'err'); }
}

function updateDashboard(data) {
  // Stats
  document.getElementById('s-ram').textContent     = data.ram + ' MB';
  const trend = parseFloat(data.ramTrend);
  document.getElementById('s-trend').textContent   = (trend >= 0 ? '+' : '') + data.ramTrend + ' MB/s';
  document.getElementById('s-trend').style.color   = trend > 2 ? 'var(--red)' : trend > 0.5 ? 'var(--yellow)' : 'var(--green)';
  document.getElementById('s-engines').textContent = data.engines.length;
  document.getElementById('s-hot').textContent     = data.hotCount;
  document.getElementById('s-uptime').textContent  = fmtUptime(data.uptime);

  // Mode badge
  const mb = document.getElementById('mode-badge');
  mb.textContent  = data.mode;
  mb.className    = 'mode mode-' + data.mode;

  // RAM bar
  const pct  = Math.min(100, (data.ram / data.ramLimit) * 100);
  const fill = document.getElementById('ram-bar-fill');
  fill.style.width      = pct + '%';
  fill.style.background = pct > 90 ? 'var(--red)' : pct > 70 ? 'var(--yellow)' : 'var(--green)';
  document.getElementById('ram-label-text').textContent = data.ram + ' / ' + data.ramLimit + ' MB (' + pct.toFixed(0) + '%)';

  // Last update
  document.getElementById('last-update').textContent = 'Updated ' + new Date().toLocaleTimeString();

  // Engines grid
  const grid = document.getElementById('engines-grid');
  if (data.engines.length === 0) {
    grid.innerHTML = '<div class="empty">No active engines. Start watching something in Stremio!</div>';
  } else {
    grid.innerHTML = data.engines.map(e => engineCard(e)).join('');
    // Update presets in tester
    updatePresets(data.engines);
  }
}

function engineCard(e) {
  const spd     = parseFloat(e.speedMBs);
  const spdPct  = Math.min(100, (spd / 5) * 100);
  const stateClr = e.state === 'streaming' ? 'var(--accent)' : e.state === 'ready' ? 'var(--green)' : 'var(--yellow)';
  const card     = e.state === 'streaming' ? 'streaming' : e.hot ? 'hot' : '';

  const badgesHtml = [
    e.hot       ? '<span class="badge badge-hot">🔥 HOT</span>'      : '',
    e.activeStreams > 0 ? '<span class="badge badge-streaming">▶ STREAMING</span>' : '',
    e.ready     ? '<span class="badge badge-ready">✅ READY</span>'  : '<span class="badge badge-connecting">⏳ ' + (e.state||'connecting').toUpperCase() + '</span>',
  ].filter(Boolean).join('');

  const metricsHtml = [
    e.metadataMs  !== null ? \`<div class="metric-chip">Metadata <b>\${e.metadataMs}ms</b></div>\` : '',
    e.firstPeerMs !== null ? \`<div class="metric-chip">First Peer <b>\${e.firstPeerMs}ms</b></div>\` : '',
    e.playableMs  !== null ? \`<div class="metric-chip">Playable <b>\${e.playableMs}ms</b></div>\` : '',
    e.prebuffered          ? '<div class="metric-chip" style="color:var(--green)">✅ Prebuffered</div>' : '',
  ].filter(Boolean).join('');

  return \`<div class="engine-card \${card}">
    <div class="engine-header">
      <span class="engine-id">\${e.id}…</span>
      \${badgesHtml}
    </div>
    <div class="engine-rows">
      <div class="engine-row">Speed <span style="color:var(--accent)">\${e.speedMBs} MB/s</span></div>
      <div class="engine-row">Peers <span>\${e.peers}</span></div>
      <div class="engine-row">Downloaded <span>\${e.downloaded} MB</span></div>
      <div class="engine-row">Streams <span style="color:\${stateClr}">\${e.activeStreams}</span></div>
      <div class="engine-row" style="grid-column:1/-1">File <span style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">\${e.selectedFile || '—'}</span></div>
      <div class="engine-row">Last Access <span>\${e.lastAccess}</span></div>
    </div>
    <div class="speed-bar"><div class="speed-fill" style="width:\${spdPct}%"></div></div>
    \${metricsHtml ? '<div class="metrics-row">' + metricsHtml + '</div>' : ''}
    <div style="margin-top:10px;display:flex;gap:6px">
      <button class="btn btn-sm" onclick="setHash('\${e.fullHash}')">🧪 Test</button>
      <button class="btn btn-sm" onclick="warmSpecific('\${e.fullHash}')">🔥 Warm</button>
    </div>
  </div>\`;
}

function updatePresets(engines) {
  const area = document.getElementById('presets-area');
  if (!engines.length) return;
  area.innerHTML = engines.map(e =>
    \`<button class="btn btn-sm" onclick="setHash('\${e.fullHash}')" title="\${e.selectedFile||e.fullHash}">\${e.id}… \${e.selectedFile ? '(' + (e.selectedFile.split('.').pop().toUpperCase()) + ')' : ''}</button>\`
  ).join('');
}

function setHash(hash) {
  document.getElementById('hash-input').value = hash;
  switchTab('tester');
  setStatus('Hash loaded: ' + hash.substring(0,8) + '…', 'ok');
}

function loadEngineHash() {
  const el = document.querySelector('#engines-grid .engine-card');
  if (!el) { setStatus('No active engines found.', 'error'); return; }
  const btn = el.querySelector('button');
  if (btn) btn.click();
}

// ── Stream tester ────────────────────────────────────────
let testStart = 0;

function getHash() { return document.getElementById('hash-input').value.trim(); }
function getFileIdx() {
  const v = document.getElementById('fileidx-input').value.trim();
  return v !== '' ? parseInt(v) : undefined;
}

function setStatus(msg, type='') {
  const el = document.getElementById('tester-status');
  el.textContent  = msg;
  el.className    = 'tester-status' + (type ? ' ' + type : '');
}

function getStreamUrl() {
  const hash = getHash();
  const fidx = getFileIdx();
  if (!hash || hash.length !== 40) return null;
  return '/stream/' + hash + (fidx !== undefined ? '?fileIdx=' + fidx : '');
}

function copyStreamUrl() {
  const url = getStreamUrl();
  if (!url) { setStatus('Enter a valid 40-char infoHash first.', 'error'); return; }
  navigator.clipboard.writeText(window.location.origin + url)
    .then(() => setStatus('URL copied to clipboard!', 'ok'))
    .catch(() => setStatus(window.location.origin + url));
}

async function warmHash() {
  const hash = getHash();
  if (!hash || hash.length !== 40) { setStatus('Enter a valid 40-char infoHash.', 'error'); return; }
  setStatus('Warming engine…');
  try {
    const r = await fetch('/warm/' + hash);
    const d = await r.json();
    setStatus('Engine status: ' + d.status + (d.ready ? ' (already ready)' : ' (warming in background)'), 'ok');
    log('Warm: ' + hash.substring(0,8) + ' → ' + d.status, 'ok');
  } catch (e) { setStatus('Warm failed: ' + e.message, 'error'); log('Warm error: ' + e.message, 'err'); }
}

async function testStream() {
  const hash = getHash();
  if (!hash || hash.length !== 40) { setStatus('Enter a valid 40-char infoHash (40 hex chars).', 'error'); return; }

  const url = getStreamUrl();
  testStart = Date.now();

  setStatus('⏳ Connecting to torrent engine…');
  log('Test stream started: ' + hash.substring(0,8), 'info');

  videoEl = document.getElementById('test-video');
  videoEl.src = '';
  videoEl.className = 'visible';

  // Timing callbacks
  videoEl.onloadstart  = () => { setStatus('⏳ Loading…'); log('loadstart', 'info'); };
  videoEl.oncanplay    = () => {
    const ms = Date.now() - testStart;
    setStatus(\`✅ Ready to play in \${ms}ms\`, 'ok');
    log('canplay in ' + ms + 'ms', 'ok');
    showTimingFromEngine(hash);
  };
  videoEl.onwaiting    = () => { setStatus('⏸ Buffering…'); log('buffering', 'info'); };
  videoEl.onplaying    = () => { setStatus('▶ Playing', 'ok'); log('playing', 'ok'); };
  videoEl.onerror      = () => {
    const ms = Date.now() - testStart;
    setStatus('❌ Stream error after ' + ms + 'ms. Check server logs.', 'error');
    log('stream error: ' + (videoEl.error?.message || 'unknown'), 'err');
  };
  videoEl.onstalled    = () => { log('stalled', 'err'); };
  videoEl.onsuspend    = () => { log('suspended', 'info'); };

  videoEl.src  = url;
  videoEl.load();
  videoEl.play().catch(() => {});  // autoplay may be blocked — that's fine
}

function stopStream() {
  if (videoEl) { videoEl.pause(); videoEl.src = ''; videoEl.className = ''; }
  setStatus('Stopped.');
  document.getElementById('timing-section').style.display = 'none';
  log('Stream stopped', 'info');
}

async function showTimingFromEngine(hash) {
  try {
    const r    = await fetch('/api/status');
    const data = await r.json();
    const e    = data.engines.find(x => x.fullHash === hash);
    if (!e) return;
    const section = document.getElementById('timing-section');
    const display = document.getElementById('timing-display');
    section.style.display = 'block';

    const total = Math.max(e.playableMs || 1, e.metadataMs || 1, e.firstPeerMs || 1);
    const bar = (ms, color, label) => ms ? \`
      <div class="timing-bar">
        <div style="width:80px;color:var(--muted);font-size:.73rem">\${label}</div>
        <div style="flex:1;background:#1e1e2e;border-radius:3px;height:6px;overflow:hidden">
          <div class="timing-seg" style="width:\${Math.round((ms/total)*100)}%;background:\${color}"></div>
        </div>
        <div style="width:60px;text-align:right;font-size:.73rem;color:var(--text)">\${ms}ms</div>
      </div>\` : '';

    display.innerHTML = bar(e.firstPeerMs,  '#38bdf8', 'First Peer')
                      + bar(e.metadataMs,   '#a78bfa', 'Metadata')
                      + bar(e.playableMs,   '#22c55e', 'Playable');
  } catch (_) {}
}

async function warmSpecific(hash) {
  try {
    await fetch('/warm/' + hash);
    log('Warmed: ' + hash.substring(0,8), 'ok');
  } catch (_) {}
}

// ── Helpers ──────────────────────────────────────────────
function fmtUptime(secs) {
  const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = secs % 60;
  if (h) return h + 'h ' + m + 'm';
  if (m) return m + 'm ' + s + 's';
  return s + 's';
}

// ── Poll loop ────────────────────────────────────────────
fetchStatus();
setInterval(fetchStatus, 3000);
</script>
</body>
</html>`);
});

// ── Landing page ─────────────────────────────────────────
app.get('/', (req, res) => {
    const host       = req.get('host') || 'stremio.eletroclay.com';
    const installUrl = `stremio://${host}/manifest.json`;
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Torrent to weblink | Stremio Addon</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Outfit',sans-serif;background:radial-gradient(circle at 50% 0%,#1a1a2e 0%,#0d0d17 100%);color:#e2e8f0;display:flex;flex-direction:column;align-items:center;justify-content:center;min-height:100vh;text-align:center}
.glow{position:absolute;width:600px;height:600px;background:radial-gradient(circle,rgba(139,92,246,.15) 0%,transparent 70%);top:-200px;border-radius:50%;z-index:0;animation:pulse 8s ease-in-out infinite alternate}
@keyframes pulse{0%{transform:scale(1);opacity:.8}100%{transform:scale(1.1);opacity:1}}
.container{position:relative;z-index:1;max-width:700px;padding:50px 40px;background:rgba(255,255,255,.03);border-radius:24px;border:1px solid rgba(255,255,255,.08);backdrop-filter:blur(16px);box-shadow:0 30px 60px rgba(0,0,0,.4),inset 0 1px 0 rgba(255,255,255,.1);animation:floatUp 1s ease-out forwards}
@keyframes floatUp{from{transform:translateY(40px);opacity:0}to{transform:translateY(0);opacity:1}}
h1{font-size:3.5rem;font-weight:800;margin-bottom:15px;background:linear-gradient(135deg,#a78bfa 0%,#ec4899 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;letter-spacing:-1px}
p.subtitle{font-size:1.25rem;line-height:1.6;color:#94a3b8;margin-bottom:40px;font-weight:300}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:10px;background:linear-gradient(135deg,#8b5cf6 0%,#6d28d9 100%);color:white;text-decoration:none;padding:16px 48px;font-size:1.25rem;font-weight:600;border-radius:50px;transition:all .3s;box-shadow:0 10px 25px rgba(139,92,246,.5)}
.btn:hover{transform:translateY(-3px) scale(1.02);box-shadow:0 15px 35px rgba(139,92,246,.6)}
.btn-links{margin-top:25px;display:flex;gap:20px;justify-content:center}
.link{color:#8b5cf6;text-decoration:none;font-weight:600;font-size:1rem;transition:color .2s}.link:hover{color:#a78bfa}
.features{display:flex;flex-wrap:wrap;justify-content:center;gap:15px;margin-top:40px}
.feature{background:rgba(139,92,246,.1);padding:12px 24px;border-radius:12px;font-size:.95rem;font-weight:600;border:1px solid rgba(139,92,246,.2);display:flex;align-items:center;gap:8px}
</style>
</head>
<body>
<div class="glow"></div>
<div class="container">
  <h1>Torrent to weblink</h1>
  <p class="subtitle">The ultimate Stremio addon powered by the <b>Hydra Brain</b> engine. Flawless, buffer-free 4K & HDR streaming with predictive RAM control.</p>
  <a href="${installUrl}" class="btn">
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="m12 5 7 7-7 7"/></svg>
    Install in Stremio
  </a>
  <div class="btn-links">
    <a href="/dashboard" class="link">📊 Live Monitor</a>
    <a href="https://github.com/Aswinajay/stremio-addon" target="_blank" class="link">⭐ GitHub</a>
    <a href="https://www.buymeacoffee.com/withaswin" target="_blank" class="link">☕ Support</a>
  </div>
  <div class="features">
    <div class="feature">⚡ Instant Streams</div>
    <div class="feature">🧠 Hydra Brain</div>
    <div class="feature">🎬 40+ Sources</div>
    <div class="feature">🔥 8MB Prebuffer</div>
  </div>
</div>
</body>
</html>`);
});

// ── Stremio Addon SDK ────────────────────────────────────
app.use(getRouter(addonInterface));

// ── Hot pool TTL eviction ────────────────────────────────
setInterval(() => {
    const now = Date.now();
    for (const [hash, ts] of hotEngines) {
        if (now - ts > HOT_POOL_TTL && (activeEngines[hash]?.activeStreams || 0) === 0) {
            hotEngines.delete(hash);
            destroyEngine(hash);
        }
    }
}, 60_000);

// ── Start ────────────────────────────────────────────────
app.listen(PORT, () => {
    const base = process.env.RENDER_EXTERNAL_URL || `http://localhost:${PORT}`;
    console.log(`
╔══════════════════════════════════════════════════════╗
║          🎬 Torrent to weblink v3.7.0 🎬            ║
╠══════════════════════════════════════════════════════╣
║  Manifest : ${(base + '/manifest.json').padEnd(40)}║
║  Dashboard: ${(base + '/dashboard').padEnd(40)}║
║  Warm     : ${(base + '/warm/:infoHash').padEnd(40)}║
╚══════════════════════════════════════════════════════╝`);
});
