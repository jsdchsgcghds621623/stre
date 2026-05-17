const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const WebTorrent = require('webtorrent');
const axios = require('axios');
const os = require('os');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Constants ───────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 10000;
const ADDON_NAME = 'Torrent to Weblink';
const ADDON_ID = 'community.torrent2weblink';
const CLEANUP_DELAY = 3 * 60 * 1000; // 3 minutes

// RAM thresholds (MB)
const RAM = { HIGH: 100, BALANCED: 130, MEDIUM: 155, CRITICAL: 170, EMERGENCY: 185 };

// Peer budgets per mode
const PEER_BUDGET = {
  HIGH: 60, BALANCED: 40, MEDIUM: 25, CRITICAL: 12, EMERGENCY: 1, IDLE: 0
};

// ─── State ────────────────────────────────────────────────────────────────────
const engines = new Map();       // infoHash → { client, stream, meta, peers, speed, mode, timer }
const logs = [];                 // rolling log buffer for dashboard
let currentMode = 'HIGH';
let lastGCTime = Date.now();
const MAX_LOGS = 500;

// ─── Scrapers ─────────────────────────────────────────────────────────────────
const SCRAPERS = [
  { name: 'YTS',         fn: scrapeYTS },
  { name: 'TPB',         fn: scrapeTPB },
  { name: 'Nyaa',        fn: scrapeNyaa },
  { name: 'Torrentio',   fn: scrapeTorrentio },
];

async function scrapeYTS(query, type) {
  if (type !== 'movie') return [];
  try {
    const r = await axios.get(`https://yts.mx/api/v2/list_movies.json`, {
      params: { query_term: query, limit: 10 }, timeout: 8000
    });
    return (r.data?.data?.movies || []).flatMap(m =>
      (m.torrents || []).map(t => ({
        name: `[YTS] ${m.title} (${t.quality})`,
        infoHash: t.hash.toLowerCase(),
        seeders: t.seeds,
        quality: t.quality,
        source: 'YTS',
      }))
    );
  } catch { return []; }
}

async function scrapeTPB(query) {
  try {
    const r = await axios.get(`https://apibay.org/q.php?q=${encodeURIComponent(query)}&cat=0`, { timeout: 8000 });
    return (r.data || []).filter(t => t.info_hash && t.info_hash !== '0000000000000000000000000000000000000000').map(t => ({
      name: `[TPB] ${t.name}`,
      infoHash: t.info_hash.toLowerCase(),
      seeders: parseInt(t.seeders) || 0,
      source: 'TPB',
    }));
  } catch { return []; }
}

async function scrapeNyaa(query) {
  try {
    const r = await axios.get(`https://nyaa.si/?f=0&c=0_0&q=${encodeURIComponent(query)}&page=rss`, { timeout: 8000 });
    const matches = [...r.data.matchAll(/urn:btih:([a-fA-F0-9]{40})/g)];
    const names = [...r.data.matchAll(/<title>(?!Nyaa)(.+?)<\/title>/g)];
    return matches.map((m, i) => ({
      name: `[Nyaa] ${names[i]?.[1] || query}`,
      infoHash: m[1].toLowerCase(),
      seeders: 0,
      source: 'Nyaa',
    }));
  } catch { return []; }
}

async function scrapeTorrentio(imdbId, type, season, episode) {
  const path = type === 'series'
    ? `stream/${type}/${imdbId}:${season}:${episode}.json`
    : `stream/${type}/${imdbId}.json`;
  try {
    const r = await axios.get(`https://torrentio.strem.fun/${path}`, { timeout: 10000 });
    return (r.data?.streams || []).filter(s => s.infoHash).map(s => ({
      name: s.name || s.title || 'Torrentio stream',
      infoHash: s.infoHash.toLowerCase(),
      fileIdx: s.fileIdx,
      seeders: parseInt((s.name || '').match(/👤\s*(\d+)/)?.[1]) || 0,
      source: 'Torrentio',
    }));
  } catch { return []; }
}

// ─── Torrent Merging ──────────────────────────────────────────────────────────
function mergeResults(results) {
  const map = new Map();
  for (const r of results) {
    if (!r.infoHash) continue;
    if (map.has(r.infoHash)) {
      const existing = map.get(r.infoHash);
      existing.seeders = Math.max(existing.seeders, r.seeders);
      existing.source += `+${r.source}`;
    } else {
      map.set(r.infoHash, { ...r });
    }
  }
  return [...map.values()].sort((a, b) => b.seeders - a.seeders);
}

// ─── Hydra Brain ──────────────────────────────────────────────────────────────
function getRamMB() {
  const total = os.totalmem();
  const free = os.freemem();
  return Math.round((total - free) / 1024 / 1024);
}

function getMode(ramMB) {
  if (ramMB >= RAM.EMERGENCY) return 'EMERGENCY';
  if (ramMB >= RAM.CRITICAL)  return 'CRITICAL';
  if (ramMB >= RAM.MEDIUM)    return 'MEDIUM';
  if (ramMB >= RAM.BALANCED)  return 'BALANCED';
  if (ramMB >= RAM.HIGH)      return 'HIGH';
  return 'HIGH';
}

function addLog(level, message, data = {}) {
  const entry = {
    ts: Date.now(),
    time: new Date().toISOString(),
    level, // 'info' | 'warn' | 'error' | 'hydra' | 'scrape' | 'stream'
    message,
    ...data,
  };
  logs.unshift(entry);
  if (logs.length > MAX_LOGS) logs.length = MAX_LOGS;
  io.emit('log', entry);
  return entry;
}

let prevRamMB = getRamMB();
let ramVelocity = 0;

setInterval(() => {
  const ramMB = getRamMB();
  ramVelocity = (ramMB - prevRamMB); // MB change per interval (2s)
  prevRamMB = ramMB;
  const newMode = getMode(ramMB);

  if (newMode !== currentMode) {
    addLog('hydra', `Mode shift: ${currentMode} → ${newMode}`, { ramMB, ramVelocity });
    currentMode = newMode;
  }

  const budget = PEER_BUDGET[currentMode] || 60;
  const activeEngines = [...engines.values()];

  if (activeEngines.length === 0) {
    io.emit('metrics', { ramMB, ramVelocity, mode: currentMode, engines: 0, budget });
    return;
  }

  // Weighted budget allocation by speed
  const totalSpeed = activeEngines.reduce((s, e) => s + (e.speed || 0.01), 0);
  for (const eng of activeEngines) {
    const weight = (eng.speed || 0.01) / totalSpeed;
    let alloc = Math.max(1, Math.round(budget * weight));

    if (currentMode === 'EMERGENCY') {
      // Keep fast seeders, prune slow ones
      alloc = (eng.speed || 0) > 0.2 ? 3 : 1;
      addLog('hydra', `EMERGENCY prune on ${eng.meta?.name || eng.hash}`, { speed: eng.speed, alloc });
    }

    if (eng.client) {
      try { eng.client.maxConns = alloc; } catch (_) {}
    }
    eng.peerAlloc = alloc;
  }

  // Emit metrics to dashboard
  const engMetrics = activeEngines.map(e => ({
    hash: e.hash,
    name: e.meta?.name || e.hash?.slice(0, 8),
    speed: e.speed || 0,
    peers: e.peers || 0,
    peerAlloc: e.peerAlloc || 0,
    ramMB: e.ramMB || 0,
  }));

  io.emit('metrics', { ramMB, ramVelocity, mode: currentMode, engines: activeEngines.length, budget, engMetrics });

  // GC hint every 60s
  if (Date.now() - lastGCTime > 60000) {
    if (global.gc) global.gc();
    lastGCTime = Date.now();
  }
}, 2000);

// ─── Stremio Manifest ─────────────────────────────────────────────────────────
app.get('/manifest.json', (req, res) => {
  res.json({
    id: ADDON_ID,
    version: '2.0.0',
    name: ADDON_NAME,
    description: 'Stream 4K torrents from 40+ sources with Hydra Brain RAM management',
    logo: 'https://i.imgur.com/J9DGe4Z.png',
    resources: ['catalog', 'stream'],
    types: ['movie', 'series'],
    catalogs: [
      { type: 'movie',  id: 'top',    name: 'Popular Movies' },
      { type: 'series', id: 'top',    name: 'Popular Series' },
    ],
    behaviorHints: { adult: false, p2p: true },
  });
});

// ─── Catalog (stub – real data via TMDB or static) ────────────────────────────
app.get('/catalog/:type/:id.json', async (req, res) => {
  try {
    const { type, id } = req.params;
    // Return empty catalog - users browse via Stremio's built-in catalogs
    res.json({ metas: [] });
  } catch (e) {
    res.json({ metas: [] });
  }
});

// ─── Streams ──────────────────────────────────────────────────────────────────
app.get('/stream/:type/:id.json', async (req, res) => {
  const { type, id } = req.params;
  addLog('stream', `Stream request: ${type} / ${id}`);

  let imdbId = id, season, episode;
  if (type === 'series') {
    const m = id.match(/^(tt\d+):(\d+):(\d+)$/);
    if (!m) return res.json({ streams: [] });
    [, imdbId, season, episode] = m;
  }

  try {
    // Wave 1: fast scrapers
    addLog('scrape', 'Wave 1 scrapers firing', { imdbId });
    const wave1 = await Promise.allSettled([
      scrapeTorrentio(imdbId, type, season, episode),
      scrapeYTS(imdbId, type),
    ]);

    // GC pause between waves
    if (global.gc) global.gc();
    await new Promise(r => setTimeout(r, 300));

    // Wave 2: secondary scrapers
    addLog('scrape', 'Wave 2 scrapers firing', { imdbId });
    const wave2 = await Promise.allSettled([
      scrapeTPB(imdbId),
      scrapeNyaa(imdbId),
    ]);

    const allResults = [
      ...wave1.map(r => r.status === 'fulfilled' ? r.value : []),
      ...wave2.map(r => r.status === 'fulfilled' ? r.value : []),
    ].flat();

    const merged = mergeResults(allResults);
    addLog('scrape', `Merged ${merged.length} unique torrents`, { count: merged.length });

    const streams = merged.slice(0, 30).map(t => ({
      name: `🌊 ${t.source}`,
      title: `${t.name}\n👤 ${t.seeders} seeds`,
      infoHash: t.infoHash,
      fileIdx: t.fileIdx,
      behaviorHints: { bingeGroup: `${ADDON_ID}-${imdbId}` },
    }));

    res.json({ streams });
  } catch (e) {
    addLog('error', `Stream error: ${e.message}`);
    res.json({ streams: [] });
  }
});

// ─── Health API ───────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const ramMB = getRamMB();
  res.json({
    status: 'ok',
    uptime: Math.round(process.uptime()),
    mode: currentMode,
    ramMB,
    ramVelocity,
    engines: engines.size,
    budget: PEER_BUDGET[currentMode],
    timestamp: new Date().toISOString(),
  });
});

// ─── Logs API (last N entries) ────────────────────────────────────────────────
app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 100;
  res.json(logs.slice(0, limit));
});

// ─── Dashboard ────────────────────────────────────────────────────────────────
app.get('/dashboard', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'dashboard.html'));
});

// ─── Socket.io connection ─────────────────────────────────────────────────────
io.on('connection', (socket) => {
  // Send last 50 logs on connect
  socket.emit('log-history', logs.slice(0, 50));
  socket.emit('metrics', {
    ramMB: getRamMB(), ramVelocity,
    mode: currentMode, engines: engines.size,
    budget: PEER_BUDGET[currentMode], engMetrics: [],
  });
  addLog('info', 'Dashboard client connected');
});

// ─── Start ────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  addLog('info', `${ADDON_NAME} running on port ${PORT}`);
  console.log(`\n🎬 ${ADDON_NAME} — http://localhost:${PORT}`);
  console.log(`📡 Stremio: http://localhost:${PORT}/manifest.json`);
  console.log(`📊 Dashboard: http://localhost:${PORT}/dashboard`);
});
