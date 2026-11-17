// Die letzte Zuflucht – Live-Server
// Verbindet sich mit deinem TikTok-LIVE und schickt Follows, Chat, Geschenke und Likes an die Karte.
// Start: node server.js   (Umgebungsvariablen: TIKTOK_USER, optional SIGN_API_KEY, PORT)

const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent } = require('tiktok-live-connector');

const PORT = process.env.PORT || 3000;
const USER = String(process.env.TIKTOK_USER || '').trim().replace(/^@/, '');
const SIGN_API_KEY = process.env.SIGN_API_KEY || undefined;
const RETRY_MS = 30000;

/* ---------- Webseite ausliefern ---------- */
function page() {
  const html = fs.readFileSync(path.join(__dirname, 'index.html'), 'utf8');
  return html.replace('<script>\n(() => {', '<script>window.CAMP_LIVE_WS = true;</script>\n<script>\n(() => {');
}

const status = { user: USER || null, state: USER ? 'wartet' : 'kein TIKTOK_USER gesetzt', since: Date.now(), lastError: null, events: 0 };
function setState(s, err) { status.state = s; status.since = Date.now(); if (err !== undefined) status.lastError = err; console.log('[status]', s, err || ''); broadcast({ type: 'status', status }); }

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/' || url.pathname === '/index.html') {
    try { res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); res.end(page()); }
    catch (e) { res.writeHead(500); res.end('index.html fehlt'); }
    return;
  }
  if (url.pathname === '/api/status') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ ...status, clients: wss.clients.size }));
    return;
  }
  if (url.pathname === '/api/reconnect') {
    reconnectNow();
    res.writeHead(200, { 'content-type': 'application/json' }); res.end('{"ok":true}');
    return;
  }
  if (url.pathname === '/api/test' && process.env.DEV_TEST === '1') { // nur zum Testen
    emit(Object.fromEntries(url.searchParams)); res.writeHead(200); res.end('ok'); return;
  }
  res.writeHead(404); res.end('Nicht gefunden');
});

/* ---------- WebSocket zur Karte ---------- */
const wss = new WebSocketServer({ server, path: '/ws' });
const buffer = []; // Ereignisse, die ankommen, während keine Karte offen ist
function broadcast(msg) {
  const data = JSON.stringify(msg);
  let sent = false;
  for (const c of wss.clients) if (c.readyState === 1) { c.send(data); sent = true; }
  return sent;
}
function emit(ev) {
  status.events++;
  if (!broadcast(ev)) { buffer.push(ev); if (buffer.length > 2000) buffer.shift(); }
}
wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'status', status }));
  if (buffer.length) { ws.send(JSON.stringify(buffer.splice(0))); }
});

/* ---------- TikTok LIVE ---------- */
let conn = null, retryTimer = null, connecting = false;
const who = d => ({ user: d.user && d.user.uniqueId, name: (d.user && (d.user.nickname || d.user.uniqueId)) || '' });

async function connect() {
  if (!USER || connecting) return;
  connecting = true; clearTimeout(retryTimer);
  try {
    if (conn) { try { conn.disconnect(); } catch (e) {} conn = null; }
    setState('verbinde mit @' + USER + ' …');
    conn = new TikTokLiveConnection(USER, { signApiKey: SIGN_API_KEY, processInitialData: false });

    conn.on(WebcastEvent.FOLLOW, d => { const w = who(d); if (w.user) emit({ type: 'follow', ...w }); });
    conn.on(WebcastEvent.CHAT, d => { const w = who(d); if (w.user) emit({ type: 'chat', ...w, text: d.comment || '' }); });
    conn.on(WebcastEvent.LIKE, d => { const w = who(d); emit({ type: 'like', ...w, count: d.likeCount || 1 }); });
    conn.on(WebcastEvent.GIFT, d => {
      const g = d.giftDetails || {};
      // Serien-Geschenke (z. B. Rosen-Combo) erst zählen, wenn die Serie vorbei ist
      if (g.giftType === 1 && !d.repeatEnd) return;
      const diamonds = (g.diamondCount || 1) * Math.max(1, d.repeatCount || 1);
      const w = who(d); if (w.user) emit({ type: 'gift', ...w, diamonds, gift: g.giftName || '' });
    });
    conn.on(WebcastEvent.STREAM_END, () => { setState('Live beendet – warte auf nächste Live'); scheduleRetry(); });
    conn.on(WebcastEvent.DISCONNECTED, () => { if (status.state.startsWith('verbunden')) { setState('getrennt – neuer Versuch …'); scheduleRetry(5000); } });
    conn.on('error', e => { if (e && e.message) console.log('[tiktok error]', e.message); });

    await conn.connect();
    setState('verbunden mit @' + USER + ' ✅', null);
  } catch (e) {
    const msg = (e && e.message) || String(e);
    const offline = /offline|not live|isn't online|UserOffline|Room ID/i.test(msg) || (e && e.constructor && /Offline/.test(e.constructor.name));
    setState(offline ? 'nicht live (oder Name falsch) – neuer Versuch in 30 s' : 'Verbindung fehlgeschlagen – neuer Versuch in 30 s', msg.slice(0, 200));
    scheduleRetry();
  } finally { connecting = false; }
}
function scheduleRetry(ms = RETRY_MS) { clearTimeout(retryTimer); retryTimer = setTimeout(connect, ms); }
function reconnectNow() { scheduleRetry(100); }

server.listen(PORT, () => {
  console.log(`Die letzte Zuflucht läuft auf Port ${PORT}`);
  if (USER) connect(); else console.log('Bitte TIKTOK_USER setzen (dein TikTok-Name ohne @).');
});
