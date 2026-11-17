const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 10;
const GAME_MS = 60 * 1000;
const COUNTDOWN_MS = 3000;
const MAX_TAPS_PER_SEC = 14; // 防作弊：每秒最多计入的点击数

const QRCode = require('qrcode');

const server = http.createServer(async (req, res) => {
  if (req.url.startsWith('/qr')) {
    const u = new URL(req.url, 'http://x').searchParams.get('url') || '';
    if (!/^https?:\/\//.test(u) || u.length > 300) { res.writeHead(400); return res.end('bad url'); }
    try {
      const svg = await QRCode.toString(u, { type: 'svg', margin: 1, width: 220 });
      res.writeHead(200, { 'Content-Type': 'image/svg+xml' });
      return res.end(svg);
    } catch { res.writeHead(500); return res.end('error'); }
  }
  const file = path.join(__dirname, 'public', 'index.html');
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(500); return res.end('error'); }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const rooms = new Map();
let uid = 1;

function getRoom(id) {
  if (!rooms.has(id)) {
    rooms.set(id, { id, players: new Map(), state: 'lobby', hostId: null, startAt: 0, endAt: 0, timer: null });
  }
  return rooms.get(id);
}

function ranking(room) {
  return [...room.players.values()]
    .sort((a, b) => b.dist - a.dist)
    .map((p, i) => ({ id: p.id, name: p.name, dist: p.dist, rank: i + 1 }));
}

function snapshot(room) {
  const now = Date.now();
  return {
    type: 'state',
    state: room.state,
    hostId: room.hostId,
    countdown: room.state === 'countdown' ? Math.max(0, room.startAt - now) : 0,
    remain: room.state === 'running' ? Math.max(0, room.endAt - now) : 0,
    players: [...room.players.values()].map(p => ({ id: p.id, name: p.name, dist: p.dist, online: !!p.ws })),
    result: room.state === 'finished' ? ranking(room) : null,
  };
}

function broadcast(room) {
  const msg = JSON.stringify(snapshot(room));
  for (const p of room.players.values()) {
    if (p.ws && p.ws.readyState === 1) p.ws.send(msg);
  }
}

function startGame(room) {
  room.state = 'countdown';
  room.startAt = Date.now() + COUNTDOWN_MS;
  for (const p of room.players.values()) { p.dist = 0; p.bucket = MAX_TAPS_PER_SEC; p.last = Date.now(); }
  clearInterval(room.timer);
  room.timer = setInterval(() => {
    const now = Date.now();
    if (room.state === 'countdown' && now >= room.startAt) {
      room.state = 'running';
      room.endAt = now + GAME_MS;
    } else if (room.state === 'running' && now >= room.endAt) {
      room.state = 'finished';
      clearInterval(room.timer);
      room.timer = null;
    }
    broadcast(room);
  }, 100);
}

wss.on('connection', (ws) => {
  let room = null;
  let me = null;

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.type === 'join') {
      const r = getRoom(String(m.room || 'default').slice(0, 16));
      const name = String(m.name || '玩家').slice(0, 8);
      // 断线重连：同一个 pid 回到原位
      let p = m.pid && r.players.get(m.pid);
      if (p) {
        p.ws = ws;
      } else {
        if (r.state !== 'lobby') return ws.send(JSON.stringify({ type: 'error', msg: '游戏已开始，请等下一局' }));
        if (r.players.size >= MAX_PLAYERS) return ws.send(JSON.stringify({ type: 'error', msg: '房间已满（10人）' }));
        p = { id: 'p' + uid++, name, dist: 0, ws, bucket: MAX_TAPS_PER_SEC, last: Date.now() };
        r.players.set(p.id, p);
        if (!r.hostId) r.hostId = p.id;
      }
      room = r; me = p;
      ws.send(JSON.stringify({ type: 'joined', pid: p.id }));
      broadcast(r);
    } else if (!room || !me) {
      return;
    } else if (m.type === 'start') {
      if (me.id === room.hostId && room.state === 'lobby' && room.players.size >= 1) startGame(room);
    } else if (m.type === 'tap') {
      if (room.state !== 'running') return;
      const now = Date.now();
      me.bucket = Math.min(MAX_TAPS_PER_SEC, me.bucket + ((now - me.last) / 1000) * MAX_TAPS_PER_SEC);
      me.last = now;
      if (me.bucket >= 1) { me.bucket -= 1; me.dist += 1; }
    } else if (m.type === 'again') {
      if (me.id === room.hostId && room.state === 'finished') {
        room.state = 'lobby';
        for (const p of room.players.values()) p.dist = 0;
        broadcast(room);
      }
    }
  });

  ws.on('close', () => {
    if (!room || !me) return;
    me.ws = null;
    if (room.state === 'lobby') {
      room.players.delete(me.id);
      if (room.hostId === me.id) room.hostId = room.players.keys().next().value || null;
    }
    const anyOnline = [...room.players.values()].some(p => p.ws);
    if (!anyOnline) { clearInterval(room.timer); rooms.delete(room.id); return; }
    broadcast(room);
  });
});

server.listen(PORT, () => console.log('赛马游戏已启动: http://localhost:' + PORT));
