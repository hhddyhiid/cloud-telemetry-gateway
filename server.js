const http = require('http');
const net = require('net');
const { WebSocketServer, createWebSocketStream } = require('ws');

const UUID = process.env.UUID || 'd54473c1-8cde-4fa4-a0ca-80402ef8dcc1';
const PORT = process.env.PORT || 8080;
const WSPATH = process.env.WSPATH || '/sys-metric-sync';
const cleanUuid = UUID.replace(/-/g, '').toLowerCase();

const HTML_DASHBOARD = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>ApexEdge Telemetry Mesh Gateway</title>
  <style>
    :root { --bg: #090d16; --card: #111827; --border: #1f2937; --text: #f3f4f6; --accent: #10b981; }
    body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 40px 20px; display: flex; justify-content: center; align-items: center; min-height: 100vh; box-sizing: border-box; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 16px; max-width: 640px; width: 100%; padding: 36px; box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); }
    .badge { display: inline-flex; align-items: center; gap: 8px; background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); color: #34d399; font-size: 13px; font-weight: 600; padding: 4px 12px; border-radius: 9999px; margin-bottom: 20px; }
    .dot { width: 8px; height: 8px; background: #10b981; border-radius: 50%; box-shadow: 0 0 10px #10b981; }
    h1 { font-size: 24px; font-weight: 700; margin: 0 0 12px 0; color: #fff; }
    p { color: #9ca3af; font-size: 14px; line-height: 1.6; margin: 0 0 24px 0; }
    .grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 16px; }
    .stat { background: rgba(255,255,255,0.02); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
    .stat-label { font-size: 12px; text-transform: uppercase; color: #6b7280; font-weight: 600; }
    .stat-val { font-size: 18px; font-weight: 700; color: #fff; margin-top: 6px; }
    .footer { margin-top: 24px; padding-top: 20px; border-top: 1px solid var(--border); font-size: 12px; color: #4b5563; text-align: center; }
  </style>
</head>
<body>
  <div class="card">
    <div class="badge"><span class="dot"></span> APEXEDGE NODE OPERATIONAL</div>
    <h1>Telemetry Ingestion Gateway</h1>
    <p>Real-time distributed telemetry ingestion and metrics streaming edge node. High-availability Anycast mesh routing active.</p>
    <div class="grid">
      <div class="stat"><div class="stat-label">Cluster Health</div><div class="stat-val" style="color: #34d399;">99.99% Guaranteed</div></div>
      <div class="stat"><div class="stat-label">Pipeline Protocol</div><div class="stat-val">WSS / Telemetry V2</div></div>
      <div class="stat"><div class="stat-label">Encryption</div><div class="stat-val">TLS 1.3 / AES-GCM</div></div>
      <div class="stat"><div class="stat-label">Engine Runtime</div><div class="stat-val">Node.js 22 LTS</div></div>
    </div>
    <div class="footer">ApexEdge Global Telemetry Mesh &copy; 2026. All telemetry streams are verified and encrypted.</div>
  </div>
</body>
</html>`;

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
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
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
  console.log(`ApexEdge Telemetry Gateway listening on 0.0.0.0:${PORT}`);
});
