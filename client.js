const net = require('net');
const readline = require('readline');
const envFilePath = './hosts.env';
const peers = [];
const host_type = process.argv[2];
const host_id = parseInt(process.argv[3]);
const fs = require('fs');
host_addr = -1;
host_port = -1;
var sending_time = {}
var receiving_time = {}

function sleepSync(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {}
}

function readEnvFile() {
  // const peers = [];
  
  try {
    // Read the contents of the .env file
    const data = fs.readFileSync(envFilePath, 'utf8');
    
    // Split the contents into lines
    const lines = data.split('\n');
    
    // Parse each line and extract peer information
    lines.forEach(line => {
      const [type, id, host, port] = line.trim().split(',').map(item => item.trim());
      if (type && id && host && port) {
        if(type==host_type && parseInt(id)==host_id){
          host_addr = host;
          host_port = parseInt(port);
        }
        if(type=='Server'){
          peers.push({ type, id: parseInt(id), host, port: parseInt(port) });
        }
      }
    });
  } catch (err) {
    console.error('Error reading .env file:', err.message);
  }
  
}

readEnvFile();
countDictionary = {}
buffer = '';

function MessageHandler(msg_tuple){
  console.log(msg_tuple);
  var index = parseInt(msg_tuple[3]);
  index++;

    if (!countDictionary[index]) {
      countDictionary[index] = 0;
    }

    countDictionary[index]++;

    if (countDictionary[index] === 3) {
      if(!receiving_time[index])
        {
          receiving_time[index] = 0;
        }
      receiving_time[index] = Date.now();
      console.log(receiving_time[index], receiving_time[index]-sending_time[index]);
    }
}

const server = net.createServer(socket => {
  console.log('New connection from ' + socket.remoteAddress + ':' + socket.remotePort);
  // Store the newly connected socket
  connectToPeers();

  socket.on('data', data => {
    buffer += data.toString();
    let delimIndex;
    while ((delimIndex = buffer.indexOf('\0')) !== -1) {
        const message = buffer.substring(0, delimIndex);
        buffer = buffer.substring(delimIndex + 1);
        console.log('Received data from ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + message);
        const msg_tuple = JSON.parse(message);
        MessageHandler(msg_tuple);
    }
  });
  socket.on('close', () => {
    console.log('Connection with ' + socket.remoteAddress + ':' + socket.remotePort + ' closed');
    connectToPeers();
  });

  socket.on('error', err => {
    console.error('Error with ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + err.message);
  });
});

server.on('error', err => {
  console.error('Server error: ' + err.message);
});

server.listen(host_port, () => {
  console.log('Server started on port ' + host_port);
  connectToPeers();
});

const connections = {};

function connectToPeers() {
  peers.forEach(peer => {
    if(connections[peer.host+':'+peer.port]===undefined){
    const client = net.createConnection({ port: peer.port, host: peer.host }, () => {
      console.log('Connected to peer ' + peer.host + ':' + peer.port);
      connections[peer.host + ':' + peer.port] = [client,peer];
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

var seq = 1;

// Prompt user for input
rl.setPrompt('Enter message: ');

function generateLargeString() {
  const size = 25 * 1024; // Size in bytes
  const char = 'a'; // Character to repeat
  return char.repeat(size);
}

// Listen for user input
rl.on('line', input => {
  if(input == "WRITE")
    {
      console.log("NOW SEND");
      console.log(sending_time);
      console.log("NOW RECEIVE");
      console.log(receiving_time);
      const delays = Object.keys(sending_time).map(key => {
        const sendingTime = sending_time[key];
        const receivingTime = receiving_time[key];
        const delay = receivingTime - sendingTime;
        return { key, sendingTime, receivingTime, delay };
      });
      
      const averageDelay = delays.reduce((sum, { delay }, index, array) => {
        if (index === array.length - 1) {
          return sum;
        }
        return sum + delay;
      }, 0) / (delays.length - 1);      
      
      const filePath = 'delays.txt';
      
      fs.writeFileSync(filePath, 'Key, Sending Time, Receiving Time, Delay\n');
      delays.forEach(({ key, sendingTime, receivingTime, delay }) => {
        const line = `${key}, ${sendingTime}, ${receivingTime}, ${delay}\n`;
        fs.appendFileSync(filePath, line);
      });
      
      // Write average delay to file
      fs.appendFileSync(filePath, `\nAverage Delay: ${averageDelay}`);
      
      console.log(`Data written to ${filePath}`);
    }
    else{
      for(i = 0; i<parseInt(input); i++){
        send_req = "REQUEST " + (i);
        Object.values(connections).forEach(connection => {
          const msg_tuple = ['REQUEST', host_id, (seq-1), send_req];
          const json_data = JSON.stringify(msg_tuple);

          const client = connection[0];
          (client).write(json_data + '\0');
        });
        sending_time[parseInt(seq)] = Date.now();
        seq++;
    }
    sleepSync(50)
  }
  rl.prompt();
});

rl.prompt();