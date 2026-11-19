// Signaling server minimal: hanya meneruskan offer/answer/ICE. Audio & video lewat P2P.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PEERS = Number(process.env.MAX_PEERS) || 5;
const html = fs.readFileSync(path.join(__dirname, 'index.html'));

function iceServers() {
  const list = [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun.cloudflare.com:3478' },
  ];
  // Opsional: TURN untuk jaringan yang tidak bisa P2P langsung
  // TURN_URL boleh beberapa, pisah koma. Contoh: turn:xxx:443?transport=tcp
  if (process.env.TURN_URL) {
    list.push({
      urls: process.env.TURN_URL.split(',').map((s) => s.trim()),
      username: process.env.TURN_USER || '',
      credential: process.env.TURN_PASS || '',
    });
  }
  return list;
}

const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.end('ok');
    return;
  }
  if (req.url === '/config') {
    res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
    res.end(JSON.stringify({ iceServers: iceServers(), maxPeers: MAX_PEERS }));
    return;
  }
  res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
  res.end(html);
});

const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const rooms = new Map(); // room -> Map(id -> {ws, name})

const send = (ws, obj) => {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
};

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => (ws.isAlive = true));
  let me = null;

  ws.on('message', (raw) => {
    let m;
    try {
      m = JSON.parse(raw);
    } catch {
      return;
    }

    if (m.type === 'join' && !me) {
      const room = String(m.room || '').toLowerCase().replace(/[^a-z0-9-]/g, '').slice(0, 32);
      if (!room) return;
      let r = rooms.get(room);
      if (!r) {
        r = new Map();
        rooms.set(room, r);
      }
      if (r.size >= MAX_PEERS) {
        send(ws, { type: 'full' });
        return;
      }
      const id = crypto.randomBytes(4).toString('hex');
      const name = String(m.name || 'Pemain').slice(0, 24);
      me = { id, room, name };
      send(ws, {
        type: 'joined',
        id,
        peers: [...r.entries()].map(([pid, p]) => ({ id: pid, name: p.name })),
      });
      r.set(id, { ws, name });
    } else if (m.type === 'signal' && me) {
      const target = rooms.get(me.room)?.get(m.to);
      if (target) send(target.ws, { type: 'signal', from: me.id, name: me.name, data: m.data });
    }
  });

  ws.on('close', () => {
    if (!me) return;
    const r = rooms.get(me.room);
    if (!r) return;
    r.delete(me.id);
    for (const p of r.values()) send(p.ws, { type: 'peer-left', id: me.id });
    if (!r.size) rooms.delete(me.room);
  });
});

// Heartbeat supaya koneksi tidak diputus proxy/hosting saat idle
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

server.listen(PORT, () => console.log(`Mabar Room jalan di http://localhost:${PORT}`));
