// // Copyright 2026-present the SMS Gateway authors. All Rights Reserved.
// //
// // Licensed under the Apache License, Version 2.0 (the "License");
// // you may not use this file except in compliance with the License.
// // You may obtain a copy of the License at
// //
// //     http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.

'use strict';

/*
 * SMS Gateway relay server.
 *
 * - Phones connect OUT over WebSocket:  /ws?slug=<slug>&token=<token>
 * - Internet clients call:              /s/<slug>/<path>   (any method)
 * - Dashboard (admin) API:              /admin/*  (Authorization: Bearer <ADMIN_TOKEN>)
 *
 * Devices + message history are persisted to DATA_FILE (default ./data.json).
 */

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8080;
const REQUEST_TIMEOUT_MS = 25000;
const CMD_TIMEOUT_MS = 10000;
const MAX_MESSAGES = 20000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');

/* ------------------------------------------------------------------ storage */

let db = { adminToken: '', devices: {}, messages: [] };

// Persistence: Postgres when DATABASE_URL is set (Render), JSON file otherwise.
let pool = null;
if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.PGSSL === 'off' ? false : { rejectUnauthorized: false },
  });
}

async function loadDb() {
  let loaded = null;
  if (pool) {
    await pool.query('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v JSONB NOT NULL)');
    const r = await pool.query("SELECT v FROM kv WHERE k = 'db'");
    loaded = r.rows[0]?.v || null;
  } else {
    try { loaded = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch { /* first run */ }
  }
  db = { ...db, ...(loaded || {}) };
  if (process.env.ADMIN_TOKEN) db.adminToken = process.env.ADMIN_TOKEN;
  if (!db.adminToken) {
    db.adminToken = 'adm_' + crypto.randomBytes(24).toString('hex');
    console.log('Generated admin token (stored in the database):', db.adminToken);
  }
  flush();
}

function flush() {
  if (pool) {
    pool.query(
      "INSERT INTO kv (k, v) VALUES ('db', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
      [JSON.stringify(db)],
    ).catch((e) => console.error('save failed', e.message));
  } else {
    fs.writeFile(DATA_FILE, JSON.stringify(db), (e) => e && console.error('save failed', e));
  }
}

let saveTimer = null;
function save() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; flush(); }, pool ? 2000 : 500);
}

/** slug -> { socket, pending: Map<id,{resolve,timer}>, cmds: Map<id,{resolve,timer}> } */
const live = new Map();

const isOnline = (slug) => {
  const l = live.get(slug);
  return !!l && l.socket.readyState === WebSocket.OPEN;
};

function publicDevice(d) {
  return {
    slug: d.slug,
    name: d.name,
    sim: d.sim || null,
    apiKey: d.apiKey || null,
    online: isOnline(d.slug),
    createdAt: d.createdAt,
    lastSeen: d.lastSeen || null,
    connections: d.connections || 0,
  };
}

/* --------------------------------------------------------------------- util */

function json(res, status, obj) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

const FORWARDED_HEADERS = ['content-type', 'x-api-key', 'authorization', 'user-agent'];
function filterHeaders(headers) {
  const out = {};
  for (const h of FORWARDED_HEADERS) if (headers[h] !== undefined) out[h] = headers[h];
  return out;
}

const errBody = (status, error) => ({
  status,
  headers: { 'content-type': 'application/json; charset=utf-8' },
  body: Buffer.from(JSON.stringify({ success: false, error })),
});

/** Forward one HTTP request to a connected phone; resolves with {status,headers,body}. */
function forward(slug, method, p, headers, bodyBuf) {
  return new Promise((resolve) => {
    const dev = live.get(slug);
    if (!dev || dev.socket.readyState !== WebSocket.OPEN) {
      return resolve(errBody(503, `Phone (slug: ${slug}) is not connected. Open the app to activate it.`));
    }
    const id = crypto.randomUUID();
    const timer = setTimeout(() => {
      dev.pending.delete(id);
      resolve(errBody(504, `Timeout: the phone did not answer within ${REQUEST_TIMEOUT_MS / 1000}s.`));
    }, REQUEST_TIMEOUT_MS);
    dev.pending.set(id, { resolve, timer });
    const frame = Buffer.from(JSON.stringify({
      type: 'req', id, method, path: p, headers, body: bodyBuf.toString('base64'),
    }));
    dev.socket.send(frame, (err) => {
      if (err && dev.pending.delete(id)) {
        clearTimeout(timer);
        resolve(errBody(502, 'Failed to forward to phone.'));
      }
    });
  });
}

/** Send a control command to the phone and await its reply. */
function command(slug, cmd) {
  return new Promise((resolve) => {
    const dev = live.get(slug);
    if (!dev || dev.socket.readyState !== WebSocket.OPEN) return resolve({ ok: false, error: 'offline' });
    const id = crypto.randomUUID();
    const timer = setTimeout(() => {
      dev.cmds.delete(id);
      resolve({ ok: false, error: 'timeout (update the app to support remote commands)' });
    }, CMD_TIMEOUT_MS);
    dev.cmds.set(id, { resolve, timer });
    dev.socket.send(JSON.stringify({ type: 'cmd', id, cmd }));
  });
}

function logMessage(entry) {
  db.messages.push(entry);
  if (db.messages.length > MAX_MESSAGES) db.messages.splice(0, db.messages.length - MAX_MESSAGES);
  const d = db.devices[entry.slug];
  if (d) d.lastSeen = entry.time;
  save();
}

/** Parse a phone response to /api/v1/sms and record it. */
function recordSend({ slug, source, reqBody, result, started }) {
  let to = '', message = '';
  try {
    const j = JSON.parse(reqBody.toString() || '{}');
    to = String(j.to ?? '');
    message = String(j.message ?? '');
  } catch { /* ignore */ }
  let parsed = {};
  try { parsed = JSON.parse(result.body.toString() || '{}'); } catch { /* ignore */ }
  logMessage({
    id: crypto.randomUUID(),
    time: Date.now(),
    slug,
    source,
    to,
    message,
    status: result.status,
    success: result.status >= 200 && result.status < 300 && parsed.success !== false,
    parts: parsed.parts ?? null,
    error: parsed.success === false ? parsed.error || null : null,
    ms: Date.now() - started,
  });
}

/* ------------------------------------------------------------------- stats */

function overview() {
  const now = Date.now();
  const DAY = 86400000, HOUR = 3600000;
  const startOfToday = new Date(); startOfToday.setHours(0, 0, 0, 0);
  const msgs = db.messages;
  const ok = msgs.filter((m) => m.success).length;

  const hourly = Array.from({ length: 24 }, (_, i) => ({
    t: Math.floor(now / HOUR) * HOUR - (23 - i) * HOUR, ok: 0, fail: 0,
  }));
  const daily = Array.from({ length: 14 }, (_, i) => ({
    t: startOfToday.getTime() - (13 - i) * DAY, ok: 0, fail: 0,
  }));
  const perDevice = {};
  const recipients = {};
  let today = 0, last24 = 0, latencySum = 0, latencyN = 0;

  for (const m of msgs) {
    const key = m.success ? 'ok' : 'fail';
    if (m.time >= startOfToday.getTime()) today++;
    if (m.time >= now - DAY) last24++;
    const h = hourly.find((b) => m.time >= b.t && m.time < b.t + HOUR);
    if (h) h[key]++;
    const d = daily.find((b) => m.time >= b.t && m.time < b.t + DAY);
    if (d) d[key]++;
    const pd = (perDevice[m.slug] ||= { ok: 0, fail: 0 });
    pd[key]++;
    if (m.to) recipients[m.to] = (recipients[m.to] || 0) + 1;
    if (m.success && m.ms) { latencySum += m.ms; latencyN++; }
  }

  const devices = Object.values(db.devices);
  return {
    devices: devices.length,
    online: devices.filter((d) => isOnline(d.slug)).length,
    total: msgs.length,
    success: ok,
    failed: msgs.length - ok,
    successRate: msgs.length ? Math.round((ok / msgs.length) * 1000) / 10 : null,
    today,
    last24h: last24,
    avgLatencyMs: latencyN ? Math.round(latencySum / latencyN) : null,
    hourly,
    daily,
    perDevice: devices.map((d) => ({
      slug: d.slug, name: d.name, online: isOnline(d.slug),
      ok: perDevice[d.slug]?.ok || 0, fail: perDevice[d.slug]?.fail || 0,
    })),
    topRecipients: Object.entries(recipients)
      .sort((a, b) => b[1] - a[1]).slice(0, 5).map(([to, count]) => ({ to, count })),
  };
}

/* -------------------------------------------------------------- admin routes */

function authOk(req) {
  const h = req.headers.authorization || '';
  const t = h.startsWith('Bearer ') ? h.slice(7) : '';
  const a = Buffer.from(t), b = Buffer.from(db.adminToken);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

async function handleAdmin(req, res, url, parts) {
  if (!authOk(req)) return json(res, 401, { success: false, error: 'Unauthorized' });
  const m = req.method;
  const seg = parts.slice(1); // after "admin"

  if (m === 'GET' && seg[0] === 'overview') return json(res, 200, overview());

  if (seg[0] === 'devices') {
    if (m === 'GET' && seg.length === 1) {
      return json(res, 200, { devices: Object.values(db.devices).map(publicDevice) });
    }
    if (m === 'POST' && seg.length === 1) {
      const body = JSON.parse((await readBody(req)).toString() || '{}');
      const slug = crypto.randomBytes(4).toString('hex');
      const token = crypto.randomBytes(16).toString('hex');
      db.devices[slug] = {
        slug, token, name: String(body.name || `Device ${Object.keys(db.devices).length + 1}`).slice(0, 60),
        apiKey: null, sim: null, createdAt: Date.now(), lastSeen: null, connections: 0,
      };
      save();
      return json(res, 201, { device: publicDevice(db.devices[slug]), token });
    }
    const d = db.devices[seg[1]];
    if (!d) return json(res, 404, { success: false, error: 'Unknown device' });

    if (m === 'GET' && seg.length === 2) return json(res, 200, { device: publicDevice(d), token: d.token });
    if (m === 'PATCH' && seg.length === 2) {
      const body = JSON.parse((await readBody(req)).toString() || '{}');
      if (body.name) d.name = String(body.name).slice(0, 60);
      save();
      return json(res, 200, { device: publicDevice(d) });
    }
    if (m === 'DELETE' && seg.length === 2) {
      live.get(d.slug)?.socket.close(4002, 'device removed');
      delete db.devices[d.slug];
      db.messages = db.messages.filter((x) => x.slug !== d.slug);
      save();
      return json(res, 200, { success: true });
    }
    if (m === 'POST' && seg[2] === 'regenerate-key') {
      const r = await command(d.slug, 'regenerate_key');
      if (!r.ok) {
        return json(res, r.error === 'offline' ? 409 : 504, {
          success: false,
          error: r.error === 'offline' ? 'Device is offline — open the app first.' : r.error,
        });
      }
      d.apiKey = r.apiKey;
      save();
      return json(res, 200, { success: true, apiKey: d.apiKey });
    }
    if (m === 'POST' && seg[2] === 'send') {
      const body = await readBody(req);
      if (!d.apiKey) return json(res, 409, { success: false, error: 'API key unknown yet — reconnect the phone (updated app) or regenerate the key.' });
      const started = Date.now();
      const result = await forward(d.slug, 'POST', '/api/v1/sms',
        { 'content-type': 'application/json', 'x-api-key': d.apiKey }, body);
      recordSend({ slug: d.slug, source: 'dashboard', reqBody: body, result, started });
      res.writeHead(result.status, { 'Content-Type': 'application/json; charset=utf-8' });
      return res.end(result.body);
    }
  }

  if (m === 'GET' && seg[0] === 'messages') {
    const q = (url.searchParams.get('q') || '').toLowerCase();
    const dev = url.searchParams.get('device');
    const status = url.searchParams.get('status'); // ok | fail
    const limit = Math.min(Number(url.searchParams.get('limit')) || 50, 500);
    const offset = Number(url.searchParams.get('offset')) || 0;
    let list = db.messages;
    if (dev) list = list.filter((x) => x.slug === dev);
    if (status === 'ok') list = list.filter((x) => x.success);
    if (status === 'fail') list = list.filter((x) => !x.success);
    if (q) list = list.filter((x) => x.to.toLowerCase().includes(q) || x.message.toLowerCase().includes(q));
    const total = list.length;
    const items = list.slice().reverse().slice(offset, offset + limit).map((x) => ({
      ...x, device: db.devices[x.slug]?.name || x.slug,
    }));
    return json(res, 200, { total, items });
  }

  json(res, 404, { success: false, error: 'Unknown admin endpoint' });
}

/* ------------------------------------------------------------- http server */

async function handleHttpRequest(req, res) {
  const url = new URL(req.url, 'http://localhost');
  const parts = url.pathname.split('/').filter(Boolean);

  if (url.pathname === '/health') {
    return json(res, 200, {
      status: 'ok',
      devices: Object.keys(db.devices).filter(isOnline).length,
      registered: Object.keys(db.devices).length,
    });
  }

  if (parts[0] === 'admin') return handleAdmin(req, res, url, parts);

  if (parts[0] !== 's' || parts.length < 2) {
    return json(res, 404, {
      success: false,
      error: 'Unknown endpoint. Use /s/<your-slug>/api/v1/sms with your API key.',
    });
  }

  const slug = parts[1];
  const known = !!db.devices[slug];
  const bodyBuf = await readBody(req);
  const subPath = '/' + parts.slice(2).join('/');
  const started = Date.now();
  const result = await forward(slug, req.method, subPath, filterHeaders(req.headers), bodyBuf);

  if (known && req.method === 'POST' && subPath === '/api/v1/sms') {
    recordSend({ slug, source: 'api', reqBody: bodyBuf, result, started });
  }
  const headers = { ...result.headers };
  if (!headers['content-type']) headers['content-type'] = 'application/json; charset=utf-8';
  res.writeHead(Number(result.status) || 500, headers);
  res.end(result.body);
}

const server = http.createServer((req, res) => {
  handleHttpRequest(req, res).catch((e) => json(res, 500, { success: false, error: String(e) }));
});

/* --------------------------------------------------------------- websockets */

const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (socket, req) => {
  const url = new URL(req.url, 'http://localhost');
  const slug = (url.searchParams.get('slug') || '').trim();
  const token = (url.searchParams.get('token') || '').trim();

  if (!slug || slug.length < 4) return socket.close(4000, 'invalid slug');

  let dev = db.devices[slug];
  if (dev && dev.token && dev.token !== token) return socket.close(4001, 'invalid token');
  if (!dev) {
    if (!token) return socket.close(4001, 'token required');
    dev = db.devices[slug] = {
      slug, token, name: `Device ${slug}`, apiKey: null, sim: null,
      createdAt: Date.now(), lastSeen: null, connections: 0,
    };
  }
  dev.connections = (dev.connections || 0) + 1;
  dev.lastSeen = Date.now();
  save();

  const old = live.get(slug);
  if (old && old.socket !== socket) old.socket.close(4003, 'replaced');
  const state = { socket, pending: new Map(), cmds: new Map() };
  live.set(slug, state);
  console.log(`[+] device connected: ${slug}`);

  socket.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    if (msg.type === 'res' && typeof msg.id === 'string') {
      const p = state.pending.get(msg.id);
      if (!p) return;
      state.pending.delete(msg.id);
      clearTimeout(p.timer);
      p.resolve({
        status: Number(msg.status) || 500,
        headers: msg.headers || {},
        body: msg.body ? Buffer.from(msg.body, 'base64') : Buffer.alloc(0),
      });
    } else if (msg.type === 'hello') {
      if (typeof msg.apiKey === 'string' && msg.apiKey) dev.apiKey = msg.apiKey;
      if (typeof msg.sim === 'string') dev.sim = msg.sim;
      dev.lastSeen = Date.now();
      save();
    } else if (msg.type === 'cmd_res' && typeof msg.id === 'string') {
      const c = state.cmds.get(msg.id);
      if (!c) return;
      state.cmds.delete(msg.id);
      clearTimeout(c.timer);
      c.resolve({ ok: !!msg.ok, apiKey: msg.apiKey, error: msg.error });
    }
  });

  const cleanup = () => {
    if (live.get(slug) === state) {
      live.delete(slug);
      dev.lastSeen = Date.now();
      save();
      console.log(`[-] device disconnected: ${slug}`);
    }
    for (const [, p] of state.pending) {
      clearTimeout(p.timer);
      p.resolve(errBody(502, 'Phone disconnected while handling the request.'));
    }
    state.pending.clear();
    for (const [, c] of state.cmds) { clearTimeout(c.timer); c.resolve({ ok: false, error: 'disconnected' }); }
    state.cmds.clear();
  };
  socket.on('close', cleanup);
  socket.on('error', cleanup);
});

// Keep connections alive through NATs / load balancers.
const keepAlive = setInterval(() => wss.clients.forEach((c) => c.ping()), 30000);
wss.on('close', () => clearInterval(keepAlive));

loadDb().then(() => server.listen(PORT, () => {
  console.log(`SMS Gateway relay listening on 0.0.0.0:${PORT}`);
  console.log(`  phones connect to:  ws://<this-host>/ws?slug=<slug>&token=<token>`);
  console.log(`  clients call:       http://<this-host>/s/<slug>/api/v1/sms`);
  console.log(`  admin API:          /admin/*  (Bearer admin token)`);
})).catch((e) => { console.error('startup failed', e); process.exit(1); });

// Persist on shutdown (Render sends SIGTERM on deploys/restarts).
process.on('SIGTERM', async () => {
  try {
    if (pool) await pool.query(
      "INSERT INTO kv (k, v) VALUES ('db', $1) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
      [JSON.stringify(db)]);
    else fs.writeFileSync(DATA_FILE, JSON.stringify(db));
  } catch (e) { console.error(e.message); }
  process.exit(0);
});
