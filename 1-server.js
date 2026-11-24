// world-ships-live: AIS collector + live map server
// Upstream: aisstream.io (free WebSocket AIS feed). Downstream: browsers.
const http = require('http');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const AIS_KEY = process.env.AISSTREAM_API_KEY || '';
const DATA_FILE = path.join(__dirname, 'vessels.json');
const STALE_MS = 48 * 3600 * 1000;      // drop vessels not seen for 48h
const LIVE_MS = 10 * 60 * 1000;         // "live now" = position in last 10 min
const SNAPSHOT_CAP = 30000;             // max vessels per viewport snapshot
const PUSH_MS = 1500;

/** @type {Map<string, object>} mmsi -> vessel */
const vessels = new Map();
let msgCount = 0, msgPerSec = 0, upstreamState = 'init', lastUpstreamMsg = 0;

// ---------- persistence (best effort; ephemeral on free hosting) ----------
function saveVessels() {
  try {
    const arr = [];
    for (const v of vessels.values()) arr.push(v);
    fs.writeFileSync(DATA_FILE + '.tmp', JSON.stringify(arr));
    fs.renameSync(DATA_FILE + '.tmp', DATA_FILE);
  } catch (e) { /* ignore */ }
}
function loadVessels() {
  try {
    const arr = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
    const now = Date.now();
    for (const v of arr) if (now - (v.lastSeen || 0) < STALE_MS) vessels.set(String(v.mmsi), v);
    console.log('loaded', vessels.size, 'vessels from disk');
  } catch (e) { /* first boot */ }
}
loadVessels();
setInterval(saveVessels, 5 * 60 * 1000);
process.on('SIGTERM', () => { saveVessels(); process.exit(0); });
process.on('SIGINT', () => { saveVessels(); process.exit(0); });

// ---------- upstream aisstream ----------
let backoff = 2000;
function connectUpstream() {
  if (!AIS_KEY) { upstreamState = 'no-key'; console.log('AISSTREAM_API_KEY missing'); return; }
  upstreamState = 'connecting';
  const ws = new WebSocket('wss://stream.aisstream.io/v0/stream', { perMessageDeflate: true });
  ws.on('open', () => {
    upstreamState = 'live';
    backoff = 2000;
    ws.send(JSON.stringify({
      APIKey: AIS_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FilterMessageTypes: ['PositionReport', 'StandardClassBPositionReport',
        'ExtendedClassBPositionReport', 'ShipStaticData', 'StaticDataReport']
    }));
    console.log('upstream connected');
  });
  ws.on('message', (data) => {
    msgCount++; lastUpstreamMsg = Date.now();
    let ev; try { ev = JSON.parse(data.toString()); } catch { return; }
    if (ev.MessageType === 'Error') { console.log('upstream error:', JSON.stringify(ev.Message)); return; }
    handleAis(ev);
  });
  const retry = () => {
    upstreamState = 'reconnecting';
    const wait = backoff + Math.floor(Math.random() * 1000);
    backoff = Math.min(backoff * 2, 60000);
    setTimeout(connectUpstream, wait);
  };
  ws.on('close', retry);
  ws.on('error', () => { try { ws.close(); } catch {} });
}
connectUpstream();

function normName(n) { return (n || '').trim().replace(/\s+/g, ' '); }

function handleAis(ev) {
  const m = ev.MetaData || {};
  const mmsi = String(m.MMSI || (ev.Message && Object.values(ev.Message)[0] && Object.values(ev.Message)[0].UserID) || '');
  if (!mmsi || mmsi === 'undefined') return;
  let v = vessels.get(mmsi);
  if (!v) { v = { mmsi }; vessels.set(mmsi, v); }
  const now = Date.now();
  v.lastSeen = now;
  const msg = ev.Message || {};
  if (ev.MessageType === 'PositionReport' && msg.PositionReport) {
    const p = msg.PositionReport;
    if (typeof p.Latitude === 'number' && typeof p.Longitude === 'number' && p.Latitude <= 90 && p.Latitude >= -90) {
      v.lat = p.Latitude; v.lon = p.Longitude;
      v.sog = p.Sog; v.cog = p.Cog; v.hdg = p.TrueHeading === 511 ? null : p.TrueHeading;
      v.nav = p.NavigationalStatus;
      v.lastPos = now;
    }
    if (m.ShipName) v.name = normName(m.ShipName);
  } else if ((ev.MessageType === 'StandardClassBPositionReport' || ev.MessageType === 'ExtendedClassBPositionReport')) {
    const p = msg[ev.MessageType];
    if (p && typeof p.Latitude === 'number' && typeof p.Longitude === 'number' && Math.abs(p.Latitude) <= 90) {
      v.lat = p.Latitude; v.lon = p.Longitude;
      v.sog = p.Sog; v.cog = p.Cog; v.hdg = p.TrueHeading === 511 ? null : p.TrueHeading;
      v.cls = 'B';
      v.lastPos = now;
    }
    if (p && p.Name) v.name = normName(p.Name);
    if (p && p.Type) v.type = p.Type;
    if (m.ShipName) v.name = normName(m.ShipName);
  } else if (ev.MessageType === 'ShipStaticData' && msg.ShipStaticData) {
    const s = msg.ShipStaticData;
    if (s.Name) v.name = normName(s.Name);
    v.type = s.Type; v.imo = s.ImoNumber || undefined; v.call = s.CallSign || undefined;
    if (s.Destination) v.dest = normName(s.Destination);
    if (s.Eta) v.eta = s.Eta;
    if (s.MaximumStaticDraught) v.draught = s.MaximumStaticDraught;
    if (s.Dimension) {
      const d = s.Dimension;
      v.len = (d.A || 0) + (d.B || 0) || undefined;
      v.beam = (d.C || 0) + (d.D || 0) || undefined;
    }
  } else if (ev.MessageType === 'StaticDataReport' && msg.StaticDataReport) {
    const r = msg.StaticDataReport;
    if (r.ReportPartA && r.ReportPartA.Name) v.name = normName(r.ReportPartA.Name);
    if (r.ReportPartB && r.ReportPartB.ShipType) v.type = r.ReportPartB.ShipType;
  }
  dirty.set(mmsi, v);
}

// ---------- downstream fanout ----------
const dirty = new Map(); // mmsi -> vessel changed since last push

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  if (u.pathname === '/api/stats') {
    const now = Date.now();
    let live = 0, withPos = 0;
    for (const v of vessels.values()) {
      if (v.lat !== undefined) withPos++;
      if (v.lastPos && now - v.lastPos < LIVE_MS) live++;
    }
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    res.end(JSON.stringify({
      vessels: vessels.size, withPos, live, msgPerSec,
      upstream: upstreamState, uptimeSec: Math.floor(process.uptime())
    }));
    return;
  }
  if (u.pathname === '/api/search') {
    const q = (u.searchParams.get('q') || '').trim().toLowerCase();
    res.writeHead(200, { 'content-type': 'application/json', 'access-control-allow-origin': '*' });
    if (q.length < 2) { res.end('[]'); return; }
    const out = [];
    const isNum = /^\d+$/.test(q);
    for (const v of vessels.values()) {
      if (v.lat === undefined) continue;
      if (isNum ? v.mmsi.startsWith(q) : (v.name && v.name.toLowerCase().includes(q))) {
        out.push({ mmsi: v.mmsi, name: v.name || '', lat: v.lat, lon: v.lon, type: v.type || 0, lastPos: v.lastPos || 0, dest: v.dest || '' });
        if (out.length >= 25) break;
      }
    }
    res.end(JSON.stringify(out));
    return;
  }
  // static
  let p = u.pathname === '/' ? '/index.html' : u.pathname;
  const file = path.join(__dirname, path.normalize(p).replace(/^([.][.][/\\])+/, ''));
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); res.end('not found'); return; }
    const ext = path.extname(file);
    const ct = ext === '.html' ? 'text/html' : ext === '.js' ? 'text/javascript' : ext === '.css' ? 'text/css' : 'application/octet-stream';
    res.writeHead(200, { 'content-type': ct });
    res.end(buf);
  });
});

const wss = new WebSocket.Server({ server, path: '/ws' });
const clients = new Set(); // {ws, bbox:[s,w,n,e] | null}

wss.on('connection', (ws) => {
  const c = { ws, bbox: null, wantAll: true };
  clients.add(c);
  ws.on('message', (data) => {
    let m; try { m = JSON.parse(data.toString()); } catch { return; }
    if (m.type === 'view' && Array.isArray(m.bbox)) {
      c.bbox = m.bbox.map(Number);
      sendSnapshot(c);
    }
  });
  ws.on('close', () => clients.delete(c));
  ws.on('error', () => clients.delete(c));
});

function inBBox(v, b) {
  if (!b) return true;
  const [s, w, n, e] = b;
  if (v.lat < s || v.lat > n) return false;
  if (w <= e) return v.lon >= w && v.lon <= e;
  return v.lon >= w || v.lon <= e; // antimeridian
}

function pack(v) {
  return [v.mmsi, v.lat, v.lon, v.sog ?? null, v.cog ?? null, v.type || 0,
    v.name || '', v.dest || '', v.hdg ?? null, v.lastPos || v.lastSeen || 0];
}

function sendSnapshot(c) {
  const now = Date.now();
  const picked = [];
  for (const v of vessels.values()) {
    if (v.lat === undefined) continue;
    if (!inBBox(v, c.bbox)) continue;
    picked.push(v);
  }
  let capped = false;
  if (picked.length > SNAPSHOT_CAP) {
    picked.sort((a, b) => (b.lastPos || 0) - (a.lastPos || 0));
    picked.length = SNAPSHOT_CAP;
    capped = true;
  }
  const out = picked.map(pack);
  if (c.ws.readyState === 1) c.ws.send(JSON.stringify({ type: 'snapshot', vessels: out, capped, now }));
}

setInterval(() => {
  msgPerSec = Math.round(msgCount / 5); msgCount = 0;
}, 5000);

setInterval(() => {
  // prune stale
  const now = Date.now();
  for (const [k, v] of vessels) if (now - (v.lastSeen || 0) > STALE_MS) vessels.delete(k);
  // push deltas
  if (!dirty.size) return;
  const changes = [...dirty.values()];
  dirty.clear();
  for (const c of clients) {
    if (c.ws.readyState !== 1 || !c.bbox) continue;
    const ups = [];
    for (const v of changes) if (v.lat !== undefined && inBBox(v, c.bbox)) ups.push(pack(v));
    if (ups.length) c.ws.send(JSON.stringify({ type: 'delta', ups, now }));
  }
}, PUSH_MS);

setInterval(() => {
  const now = Date.now();
  let live = 0;
  for (const v of vessels.values()) if (v.lastPos && now - v.lastPos < LIVE_MS) live++;
  const s = JSON.stringify({ type: 'stats', vessels: vessels.size, live, msgPerSec, upstream: upstreamState });
  for (const c of clients) if (c.ws.readyState === 1) c.ws.send(s);
}, 5000);

server.listen(PORT, () => console.log('listening on', PORT));
