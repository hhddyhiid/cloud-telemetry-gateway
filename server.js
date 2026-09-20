const http = require('http');
const net = require('net');
const { WebSocketServer, createWebSocketStream } = require('ws');

const UUID = process.env.UUID || 'd54473c1-8cde-4fa4-a0ca-80402ef8dcc1';
const PORT = process.env.PORT || 8080;
const WSPATH = process.env.WSPATH || '/sys-metric-sync';
const cleanUuid = UUID.replace(/-/g, '').toLowerCase();

const HTML_DASHBOARD = ;

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/dashboard') {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(HTML_DASHBOARD);
    return;
  }
  if (req.url === '/healthz' || req.url === '/status') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      node: 'apexedge-container-gw',
      uptime_seconds: Math.floor(process.uptime()),
      memory_rss_mb: Math.round(process.memoryUsage().rss / 1024 / 1024),
      timestamp: new Date().toISOString()
    }));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, );
  if (url.pathname === WSPATH) {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit('connection', ws, req);
    });
  } else {
    socket.destroy();
  }
});

wss.on('connection', (ws) => {
  const duplex = createWebSocketStream(ws);

  ws.once('message', (msg) => {
    try {
      if (msg.length < 18) { ws.close(); return; }
      const version = msg[0];
      const clientUuid = msg.slice(1, 17).toString('hex');
      if (clientUuid !== cleanUuid) { ws.close(); return; }

      let idx = 17 + msg[17] + 1;
      const cmd = msg[idx++];
      if (cmd !== 1) { ws.close(); return; } // TCP Connect only

      const port = msg.readUInt16BE(idx);
      idx += 2;
      const atyp = msg[idx++];
      let host = '';

      if (atyp === 1) { // IPv4
        host = msg.slice(idx, idx + 4).join('.');
        idx += 4;
      } else if (atyp === 2) { // Domain
        const len = msg[idx++];
        host = msg.slice(idx, idx + len).toString();
        idx += len;
      } else if (atyp === 3) { // IPv6
        const parts = [];
        for (let i = 0; i < 16; i += 2) {
          parts.push(msg.readUInt16BE(idx + i).toString(16));
        }
        host = parts.join(':');
        idx += 16;
      } else {
        ws.close();
        return;
      }

      // Reply VLESS header [version, addon_len=0]
      ws.send(Buffer.from([version, 0]));

      const remote = net.connect({ host, port }, () => {
        if (idx < msg.length) {
          remote.write(msg.slice(idx));
        }
        duplex.on('error', () => {}).pipe(remote).on('error', () => {}).pipe(duplex);
      });

      remote.on('error', () => {
        try { ws.close(); } catch(e) {}
      });
    } catch (e) {
      try { ws.close(); } catch(err) {}
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log();
});
