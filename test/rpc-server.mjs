import http from 'node:http';

const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (chunk) => {
    body += chunk.toString();
  });
  req.on('end', () => {
    const parsed = JSON.parse(body);
    if (process.send) {
      process.send({ type: 'request', data: parsed });
    }
    let result;
    switch (parsed.method) {
      case 'get_leaf':
        result = { leaf: parsed.params.index };
        break;
      case 'get_record':
        result = ['7', '8'];
        break;
      default:
        result = { ok: true };
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ jsonrpc: '2.0', id: parsed.id, result }));
  });
});

server.listen(0, '127.0.0.1', () => {
  const { port } = server.address();
  if (process.send) {
    process.send({ type: 'ready', port });
  }
});

process.on('message', (message) => {
  if (message?.type === 'close') {
    server.close(() => {
      process.exit(0);
    });
  }
});
