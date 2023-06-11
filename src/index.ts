/*
 * Copyright (c) 2023. Arkin Solomon.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied limitations under the License.
 */

import http from 'http';
import Express from 'express';
import socketio from 'socket.io';

const app = Express();
const server = http.createServer(app);
const io = new socketio.Server(server);

io.on('connection', client => {
  console.log('New websocket connection');
});

const port = process.env.PORT || 3000;
server.listen(port, () => {
  console.log(`Server is up on port ${port}!`);
});