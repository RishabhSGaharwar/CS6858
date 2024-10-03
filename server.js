const net = require('net');
const readline = require('readline');
const envFilePath = './hosts.env';
const peers = [];
const host_type = process.argv[2];
const host_id = parseInt(process.argv[3]);
const fs = require('fs');
const { parse } = require('path');
host_addr = -1;
host_port = -1;
my_view = 0;
m_last = 0;
v_last = -1;
unordered = [];
pending = {};
processing = new Map();
state = 'normal';
isPrepared = false;
isPrePrepared = false;
prepared_count = 0;
commit_count = 0;
f = 1;
n = 4;
merge_count = 0;
prepared_request_digest = null;
commit_sent = false;
reply_sent = false;
isPrimary = false;
blacklist = [];
merge_list = [];

// console.log = function(){}
// console.error = function(){}

checkPointBuffer = [];
checkPoint = [{my_view : my_view, m_last : m_last, v_last : v_last,
               unordered: unordered, pending : pending, processing:processing,
               state : state, isPrepared : isPrepared, isPrePrepared : isPrePrepared,
               prepared_count : prepared_count, commit_count : commit_count, 
               merge_count : merge_count, commit_sent : commit_sent, reply_sent : reply_sent, 
               blacklist : blacklist, merge_list : merge_list, prepared_request_digest : prepared_request_digest}
               , checkPointBuffer];



let buffer = '';
function sleepSync(ms) {
  const start = Date.now();
  while (Date.now() - start < ms) {}
}

var delayMS = 10000;
var timer = null;
// Processing is a map from {[<REQUEST, c, seq, op>, view_of_request] -> list of PREPARE messages corresponding to this request and view}

function startTimer (){
  if(!timer)
    {
      timer = setTimeout(expireTimer, delayMS);
    }
}

function stopTimer (){
  if(timer)
    {
      clearTimeout(timer);
    }
  timer = null;
}

function restartTimer(){
  stopTimer();
  startTimer();
}

function expireTimer(){
  console.error("In Expire");
  console.error(unordered)
  my_P = [];
  view_num = my_view; // This is the view number we will send in MERGE message
  for(const [key, value] in processing)
  {
    if(value.length>=2*f+1 && key[1]>=(v_last-n))
    {
      my_P = value;
    }
    if(state=='normal' && prepared_count>=f+1) // Is check for f+1 PRE-PREPARED really necessary doe?
    {
      view_num = my_view+1;
    }
  }
  // state = 'merge';
  blacklist.push(my_view%n);
  isPrepared=false;
  isPrePrepared=false;
  my_view++;
  prepared_count = 0;
  commit_count = 0;
  commit_sent = false;
  reply_sent = false;
  v_last = my_view;
  if(unordered.length>0){
    while(blacklist.includes(my_view%n))
    {
      my_view++;
    }
    restartTimer();
  }
  console.error("MY VIEW NOW IS ", my_view, " COMMIT");
  console.error(blacklist);
  send_tuple = ['MERGE', host_id, my_view, my_P];
  json_data = JSON.stringify(send_tuple);
  Object.values(connections).forEach(connection => {
    const socket = connection[0];
    const peer = connection[1];
    if(peer.type=='Server'){
      socket.write(json_data + '\0');
    }
  });
  if((my_view%n)==host_id){
    isPrimary=true;

    if(unordered.length!=0 && state=='normal'){
      sendPrePrepare();
    }
    if(state=='merge')
    {
      sendPrePrepareMerge();
    }
  }
  restartTimer();
}

function exec(request){
  return 1;
}

function MergeHandle(msg_tuple){;}

function readEnvFile() {
  try {
    const data = fs.readFileSync(envFilePath, 'utf8');
    
    const lines = data.split('\n');
    
    lines.forEach(line => {
      const [type, id, host, port] = line.trim().split(',').map(item => item.trim());
      if (type && id && host && port) {
        if(type==host_type && parseInt(id)==host_id){
          host_addr = host;
          host_port = parseInt(port);
          peers.push({ type, id: parseInt(id), host, port: parseInt(port) });
        }
        else{
          peers.push({ type, id: parseInt(id), host, port: parseInt(port) });
        }
      }
    });
  } catch (err) {
    console.error('Error reading .env file:', err.message);
  }
}

function sendPrePrepare(){
  if((state=='normal') && isPrePrepared==false){
    console.error("In sendPrepPrepare");
    dm = unordered[0];
    // unordered.shift();
    send_tuple = ['PRE-PREPARE', host_id, my_view, dm];
    json_data = JSON.stringify(send_tuple);
    Object.values(connections).forEach(connection => {
      const socket = connection[0];
      const peer = connection[1];
      if(peer.type=='Server'){
        socket.write(json_data + '\0');
      }
    });
    isPrePrepared = true;
    if(processing.has([dm, my_view]))
    {
      ;
    }
    else
    {
      processing.set([dm, my_view], []);
    }
  }
}

function sendPrePrepareMerge(){
  const filename = `server_${host_id}_log.txt`;
  fs.readFile(filename, 'utf8', (err, data) => {
    console.error("IN sendPrePrepareMerge ", filename);
    if (err) {
      console.error(`Error reading file ${filename}: ${err}`);
      return;
    }
    
    const merge_list = data.trim().split('\n');
    console.error('MERGE LIST', merge_list);
    
    const send_tuple = ['PRE-PREPARE-MERGE', host_id, my_view, merge_list];
    const json_data = JSON.stringify(send_tuple);
    
    let flag_sent = true;
    
    Object.values(connections).forEach(connection => {
      const socket = connection[0];
      const peer = connection[1];
      if (peer.type === 'Server') {
        if (!socket.write(json_data + '\0')) {
          flag_sent = false;
        }
      }
    });
    
    if (flag_sent) {
      console.error(`Server ${host_id} SENT PRE-PREPARE-MERGE to all`);
    } else {
      console.error(`Server ${host_id} couldn't SEND PRE-PREPARE-MERGE to all`);
    }
  });
}

function executeRequestAndLog(request, reply) {
  const logData = `${request}, ${Date.now().toString()} - Request: ${(request)}, Primary is ${(my_view)%n}\n`;
  const logFileName = `server_${host_id}_log.txt`;

  if (!fs.existsSync(logFileName)) {
      fs.writeFileSync(logFileName, '');
  }

  fs.appendFile(logFileName, logData, (err) => {
      if (err) {
          console.error(`Error writing to log file ${logFileName}:`, err);
      } else {
          console.log(`Request logged to ${logFileName}`);
          console.log(unordered);
      }
  });
}

function reqStateTransfer(data){
  send_tuple = ['STATE-TRANSFER', host_id];
  json_data = JSON.stringify(send_tuple);
  Object.values(connections).forEach(connection => {
    const socket = connection[0];
    const peer = connection[1];
    if(peer.type=='Serverr'){
      socket.write(json_data + '\0');
    }
  });
  const filename = `server_${host_id}_log.txt`;
  fs.readFile(filename, 'utf8', (err, data) => {
    console.error("IN sendPrePrepareMerge ", filename);
    if (err) {
      console.error(`Error reading file ${filename}: ${err}`);
      return;
    }
    console.log("DATA is ", data);
    const fileContent = data;
    fs.writeFile(filename, fileContent, 'utf8', (err) => {
      if (err) {
        console.error(`Error writing to file ${filename}: ${err}`);
        return;
      }
      console.error(`Successfully wrote to file ${filename}`);
    });
  });
}

function StateTransferHandler(msg_tuple){
  send_tuple = ['STATE-REPLY', checkPoint];
  json_data = JSON.stringify(send_tuple);
  Object.values(connections).forEach(connection => {
    const socket = connection[0];
    const peer = connection[1];
    if(peer.type=='Server' && peer.id == msg_tuple[1]){
      socket.write(json_data + '\0');
    }
  });
}

function StateReplyHandler(msg_tuple){
  updateState(msg_tuple[1][0]);
  executeCommands(msg_tuple[1][1]);
}

function RequestHandler(msg_tuple){
  unordered.push(JSON.stringify(msg_tuple));
  if(unordered.length==1 && isPrimary==true){
    sendPrePrepare();
  }
  startTimer();
}

function PrePrepareHandler(msg_tuple){
  if(msg_tuple[2]==my_view && isPrepared==false && state == 'normal'){
    // Send PREPARE to all servers
    send_tuple = ['PREPARE', host_id, my_view, msg_tuple[3]];
    json_data = JSON.stringify(send_tuple);
    flag_sent = true;
    Object.values(connections).forEach(connection => {
      const socket = connection[0];
      const peer = connection[1];
      if(peer.type=='Server'){
        flag_sent = flag_sent && socket.write(json_data + '\0');
      }
    });
    sleepSync(50);
    if(flag_sent)
    {
      console.error(`Server ${host_id} SENT PREPARE to all`);
    }
    else
    {
      console.error(`Server ${host_id} couldn't SEND PREPARE to all`); 
    }

    if(processing.has([(msg_tuple[3]), msg_tuple[2]]))
    {
      ;
    }
    else
    {
      processing.set([(msg_tuple[3]), msg_tuple[2]], [ ]);
    }
    isPrepared = true;
    // prepared_count++;
  }
}

function PrepareHandler(msg_tuple){
  if(msg_tuple[2]==my_view){ //Removed the second condition, check once
    prepared_count++;
    console.log("PROCESSING IS ", processing);
    processing.forEach((value, key) => {
      console.log("IN PROCESSING, KEY is ", key, " AND ", key[1]);
      if (key[1] == my_view && value) {
        value.push(msg_tuple);
      }
    });
    console.log("Processing is ", processing)
    console.error("Prepare count is ", prepared_count);
    if(prepared_count >= (2*f+1) && commit_sent==false){
      if(state=='normal'){
        send_tuple = ['COMMIT', host_id, my_view];
        json_data = JSON.stringify(send_tuple);
        flag_sent = true;
        Object.values(connections).forEach(connection => {
          const socket = connection[0];
          const peer = connection[1];
          if(peer.type=='Server'){
            flag_sent = flag_sent && socket.write(json_data + '\0');
          }
        });
        sleepSync(50);
        if(flag_sent)
        {
          console.error(`Server ${host_id} SENT COMMIT to all`);
        }
        else
        {
          console.error(`Server ${host_id} couldn't SEND COMMIT to all`); 
        }
        commit_sent = true;
        // commit_count++;
      }
    }
  }
}

function CommitHandler(msg_tuple){
  if(msg_tuple[2]==my_view){
    commit_count++;
    if(commit_count >= 2*f+1 && reply_sent==false){
      if(state=='normal'){
        stopTimer();
      }
      request = null;
      for(const [key, value] of processing)
      {
        // console.error(key[1], my_view, key[1]==my_view, parseInt(key[1])==parseInt(my_view));
        {
          if(parseInt(key[1])==parseInt(my_view))
            {
              request = (key[0]);
            }
        }
      }
      if(request!=null){
        console.log(`Executing request ${request}`);
        reply = exec(request);
        request_temp = request.slice(1, -1).split(',');
        send_tuple = ['REPLY', host_id, reply, parseInt(request_temp[2])];
        json_data = JSON.stringify(send_tuple);
        Object.values(connections).forEach(connection => {
          const socket = connection[0];
          const peer = connection[1];
          console.error(request_temp);
          if(peer.type=='Client' && peer.id==request_temp[1]){
            socket.write(json_data + '\0');
          }
        });
        sleepSync(50);
        executeRequestAndLog(request, json_data);
        remove_index = -1;
        for(i=0; i<unordered.length; i++){
          if((unordered[i])==(request)){
            remove_index = i;
            break;
          }
        }
        if(remove_index!=-1){
          console.error(`Executed Request found at ${remove_index} and removed`);
          unordered.splice(remove_index,1);
        }
        else if((unordered.length)!=0){
          console.error("REQUEST DOES NOT EXIST");
        }
      }
      else{
        // merge stuff
      }
      v_last = my_view;
      
      isPrepared=false;
      isPrePrepared=false;
      prepared_count = 0;
      commit_count = 0;
      commit_sent = false;
      reply_sent = false;
      v_last = my_view;
      my_view += 1;
      while(blacklist.includes(my_view%n))
      {
        my_view++;
      }
      console.error("MY VIEW NOW IS ", my_view, " MERGE");
      console.log(my_view%n, host_id, (my_view%n)==host_id, unordered.length)
      if((my_view%n)==host_id){
        isPrimary=true;

        if(state=='merge')
        {
          sendPrePrepareMerge();
        }
        if(unordered.length!=0){
          sendPrePrepare();
        }
      }
      if(unordered.length>0)
        {
          restartTimer();
        }
      //something to do with pending
      
    }
  }
}

function MergeHandler(msg_tuple){
  if(msg_tuple[2] >= v_last){
    merge_count++;
    console.log("In Merge Handler, ", merge_count);
    if(merge_count >= f+1 && state!='merge')
    {
      for (const [key, value] of processing) {
        if (value.length >= 2 * f + 1 && key[1] >= v_last - n) {
            myP = value;
            send_tuple = ['MERGE', host_id, my_view, myP];
            json_data = JSON.stringify(send_tuple);
            let flag_sent = true; // Initialize flag_sent here
            Object.values(connections).forEach(connection => {
                const socket = connection[0];
                const peer = connection[1];
                if (peer.type === 'Server') {
                  socket.write(json_data + '\0');
                }
            });
            if (flag_sent) {
                console.error(`Server ${host_id} SENT MERGE to all`);
            } else {
                console.error(`Server ${host_id} couldn't SEND MERGE to all`);
            }
            sleepSync(50);
            break; // Break out of the loop after sending MERGE
        }
      }        
      state = 'merge';
      isPrePrepared = false;
      isPrepared = false;
      my_view++;
    }
    if((my_view%n)==host_id){
      isPrimary=true;

      if(state=='merge')
      {
        sendPrePrepareMerge();
      }
      if(unordered.length!=0){
        sendPrePrepare();
      }
    }
  }
}

function PrePrepareMergeHandler(msg_tuple){
  if(isPrePrepared==false && my_view >= v_last && msg_tuple[1]!=host_id){
    v_min = my_view+100;
    console.error("PREPREPAREMERGE MY_VIEW, ", my_view, msg_tuple);
    for(let i=0; i<(msg_tuple[3].length); i++)
    {
      item = msg_tuple[3][i].split(',')[0];
      item = item.slice(1, -1).split(',')[2];
      if(parseInt(item)<parseInt(v_min))
      {
        v_min = parseInt(item);
      }
    }
    if(v_last+1 >= v_min)
    {
      ;      
    }
    else
    {
      reqStateTransfer(msg_tuple[3]);
    }
  }
  my_view = parseInt(msg_tuple[2])+1;
  state = 'normal';
  if(m_last < v_last)
  {
    if(blacklist.length==f)
    {
      blacklist.shift();
    }
    blacklist.push((my_view-1)%n);
  }
  else
  {
    blacklist[blacklist.length-1] = (my_view-1)%n;
  }
  restartTimer
  m_last = my_view;
  merge_count = 0;
  if((my_view%n)==host_id){
    isPrimary=true;

    if(state=='merge')
    {
      sendPrePrepareMerge();
    }
    if(unordered.length!=0){
      sendPrePrepare();
    }
  }
}

function MessageHandler(msg_tuple, socket){
  const msg_type = msg_tuple[0];
    if(msg_type=='STATE-TRANSFER'){
      StateTransferHandler(msg_tuple);
    }

    if(msg_type=='STATE-REPLY'){
      StateReplyHandler(msg_tuple);
    }

    if(msg_type=='REQUEST'){
      RequestHandler(msg_tuple);
    }

    if(msg_type=='PRE-PREPARE'){
      PrePrepareHandler(msg_tuple);
    }

    if(msg_type=='PREPARE'){
      PrepareHandler(msg_tuple);
    }

    if(msg_type=='COMMIT'){
      CommitHandler(msg_tuple);
    }

    if(msg_type=='MERGE'){
      MergeHandle(msg_tuple);
    }
    
    if(msg_type=='PRE-PREPARE-MERGE'){
      PrePrepareMergeHandler(msg_tuple);
    }
    if(msg_type=='IDENTIFY')
    {
      peerKeyHere = msg_tuple[1]+':'+msg_tuple[2];
      // console.log("IDENTIFY MESSAGE IS ", msg_tuple);
      // console.log("PEER KEY IS ", peerKeyHere);
      SocketID = socket.remoteAddress+':'+socket.remotePort;
      socket_peer_map[SocketID] = peerKeyHere;
    }
}

function updateState(serverState){
  my_view = serverState.my_view;
  m_last = serverState.m_last;
  v_last = serverState.v_last;
  unordered = serverState.unordered;
  pending = serverState.pending;
  processing = serverState.processing;
  state = serverState.state;
  isPrepared = serverState.isPrepared;
  isPrePrepared = serverState.isPrePrepared;
  prepared_count = serverState.prepared_count;
  commit_count = serverState.commit_count;
  merge_count = serverState.merge_count;
  prepared_request_digest = serverState.prepared_request_digest;
  commit_sent = serverState.commit_sent;
  reply_sent = serverState.reply_sent;
  blacklist = serverState.blacklist;
  merge_list = serverState.merge_list;
}

function executeCommands(commandsList){
  commandsList.forEach(command => {
    MessageHandler(command);
  })
}

const server = net.createServer(socket => {
  console.log('New connection from ' + socket.remoteAddress + ':' + socket.remotePort);
  connectToPeers();

  socket.on('data', data => {
    buffer += data.toString();
    let delimIndex;
    while ((delimIndex = buffer.indexOf('\0')) !== -1) {
        const message = buffer.substring(0, delimIndex);
        buffer = buffer.substring(delimIndex + 1);
        console.error('Received data from ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + message);
        const msg_tuple = JSON.parse(message);
        checkPointBuffer.push(msg_tuple);
        MessageHandler(msg_tuple, socket);
      }
  });

  socket.on('close', () => {
    console.log('Connection with ' + socket.remoteAddress + ':' + socket.remotePort + ' closed');
    peerID = socket_peer_map[socket.remoteAddress + ':' + socket.remotePort];
    delete connections[peerID]
    connectToPeers();
  });

  socket.on('error', err => {
    console.error('Error with ' + socket.remoteAddress + ':' + socket.remotePort + ': ' + err.message);
    delete connections[socket.remoteAddress + ':' + socket.remotePort];
  });
});

server.on('error', err => {
  console.error('Server error: ' + err.message);
});

readEnvFile();
console.error(peers);
if(my_view==host_id){
  isPrimary=true;
}

server.listen(host_port, () => {
  console.log('Server started on port ' + host_port);
  connectToPeers();
});



connections = {};
socket_peer_map = {};

function connectToPeers() {
  peers.forEach(peer => {
    const peerKey = `${peer.host}:${peer.port}`;
    if(!connections[peerKey]){
    const client = net.createConnection({ port: peer.port, host: peer.host }, () => {
      console.log('Connected to peer ' + peer.host + ':' + peer.port + " CLIENT  " + [client.remoteAddress, client.remotePort]);
      connections[peer.host + ':' + peer.port] = [client,peer];
      send_tuple = ['IDENTIFY', host_addr, host_port];
      json_data = JSON.stringify(send_tuple);
      if(peer.type=='Server'){
        client.write(json_data+'\0');
      }
    });
    client.on('error', err => {
      console.error('Error connecting to ' + peer.host + ':' + peer.port + ': ' + err.message);
    });
  }
});
}