const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 5000;
const PUBLIC_DIR = path.join(__dirname, 'public');

// MIME types for static files
const MIME_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon'
};

// Simple static file server
const server = http.createServer((req, res) => {
  let reqUrl = req.url.split('?')[0];

  // Quietly handle browser favicon requests without 404 error
  if (reqUrl === '/favicon.ico') {
    res.writeHead(204);
    return res.end();
  }

  if (reqUrl === '/') reqUrl = '/index.html';
  if (reqUrl === '/terms' || reqUrl === '/tc') reqUrl = '/tc.html';

  const cleanPath = reqUrl.replace(/^\/+/, '');
  const filePath = path.join(PUBLIC_DIR, cleanPath);

  // Security: prevent directory traversal
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    return res.end('Access denied');
  }

  fs.stat(filePath, (err, stats) => {
    if (err || !stats.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      return res.end('404 Not Found');
    }

    const ext = path.extname(filePath).toLowerCase();
    const contentType = MIME_TYPES[ext] || 'application/octet-stream';

    // Prevent cloud proxies and browsers from caching app logic
    res.writeHead(200, {
      'Content-Type': contentType,
      'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate, max-age=0',
      'Pragma': 'no-cache',
      'Expires': '0'
    });
    fs.createReadStream(filePath).pipe(res);
  });
});

// WebSocket Signaling Server
const wss = new WebSocketServer({ server });
const rooms = new Map(); // roomId -> Set of ws clients

// 25-second heartbeat to prevent cloud proxies (Render/Cloudflare) from dropping idle connections
function heartbeat() {
  this.isAlive = true;
}

const heartbeatInterval = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, 25000);

wss.on('close', () => {
  clearInterval(heartbeatInterval);
});

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);

  let currentRoom = null;

  ws.on('message', (rawMessage) => {
    try {
      const data = JSON.parse(rawMessage);

      // Handle ping keepalive from client
      if (data.type === 'ping') {
        ws.isAlive = true;
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      if (data.type === 'join') {
        const { roomId } = data;
        currentRoom = roomId;

        if (!rooms.has(roomId)) {
          rooms.set(roomId, new Set());
        }

        const room = rooms.get(roomId);

        // Prune any stale or closed sockets from the room
        for (const client of room) {
          if (client.readyState !== 1) { // 1 === WebSocket.OPEN
            room.delete(client);
          }
        }

        if (room.size >= 2 && !room.has(ws)) {
          ws.send(JSON.stringify({ type: 'room-full' }));
          return;
        }

        room.add(ws);

        if (room.size === 1) {
          // First peer waiting
          ws.send(JSON.stringify({ type: 'room-created', roomId }));
        } else if (room.size === 2) {
          // Second peer arrived: tell sender to initiate WebRTC offer
          const [firstPeer] = room;
          firstPeer.send(JSON.stringify({ type: 'peer-joined', initiator: true }));
          ws.send(JSON.stringify({ type: 'peer-joined', initiator: false }));
        }
        return;
      }

      // Forward signaling messages (offer, answer, candidate, file-meta, ready) to the other peer in the room
      if (currentRoom && rooms.has(currentRoom)) {
        const room = rooms.get(currentRoom);
        for (const client of room) {
          if (client !== ws && client.readyState === ws.OPEN) {
            client.send(rawMessage.toString());
          }
        }
      }
    } catch (err) {
      console.error('Signaling message error:', err.message);
    }
  });

  ws.on('close', () => {
    if (currentRoom && rooms.has(currentRoom)) {
      const room = rooms.get(currentRoom);
      room.delete(ws);

      for (const client of room) {
        if (client.readyState === ws.OPEN) {
          client.send(JSON.stringify({ type: 'peer-disconnected' }));
        }
      }

      if (room.size === 0) {
        rooms.delete(currentRoom);
      }
    }
  });
});

// Detect Local Network IP for convenient Mobile testing
function getLocalIp() {
  const interfaces = os.networkInterfaces();
  for (const ifaceName of Object.keys(interfaces)) {
    for (const iface of interfaces[ifaceName]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return 'localhost';
}

server.listen(PORT, '0.0.0.0', () => {
  const localIp = getLocalIp();
  console.log(`\n=================================================`);
  console.log(`P2P File Transfer Server is running:`);
  console.log(`- Local machine:   http://localhost:${PORT}`);
  console.log(`- Mobile / Wi-Fi:  http://${localIp}:${PORT}`);
  console.log(`=================================================\n`);
});
