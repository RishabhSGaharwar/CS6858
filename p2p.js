const net = require('net');
const readline = require('readline');

const port = parseInt(process.argv[2]);
const peers = process.argv.slice(3).map(peer => ({ host: 'localhost', port: parseInt(peer) }));

class NetworkInfo{
    constructor(socket, host, port){
        this.socket = socket;
        this.host = host;
        this.port = port;
    }
}

const server = net.createServer(socket => {
  console.log('New connection from ' + socket.remoteAddress + ':' + socket.remotePort);
  // Store the newly connected socket
  connectToPeers();
  connections[socket.remoteAddress + ':' + socket.remotePort] = socket;

  socket.on('data', data => {
    console.log('Received data from ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + data.toString());
  });

  socket.on('close', () => {
    console.log('Connection with ' + socket.remoteAddress + ':' + socket.remotePort + ' closed');
    delete connections[socket.remoteAddress + ':' + socket.remotePort];
  });

  socket.on('error', err => {
    console.error('Error with ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + err.message);
  });
});

server.on('error', err => {
  console.error('Server error: ' + err.message);
});

server.listen(port, () => {
  console.log('Server started on port ' + port);
  connectToPeers();
});

const connections = {};

function connectToPeers() {
  peers.forEach(peer => {
    if(connections[peer.host+':'+peer.port]===undefined){
    const client = net.createConnection({ port: peer.port, host: peer.host }, () => {
      console.log('Connected to peer ' + peer.host + ':' + peer.port);
      connections[peer.host + ':' + peer.port] = client;
      
    //   client.on('data', data => {
    //     console.log('Received data from ' + peer.host + ':' + peer.port + ': ' + data.toString());
    //   });
    });
    client.on('error', err => {
      console.error('Error connecting to ' + peer.host + ':' + peer.port + ': ' + err.message);
    });
  }
});
}

// Create readline interface
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Prompt user for input
rl.setPrompt('Enter message: ');

// Listen for user input
rl.on('line', input => {
  // Send input to all connected peers
  Object.values(connections).forEach(client => {
    client.write(input);
  });

  rl.prompt();
});

rl.prompt();
