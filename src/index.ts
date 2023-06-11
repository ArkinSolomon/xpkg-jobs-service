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

/**
 * The different type of jobs.
 * 
 * @name JobType
 * @enum {string}
 */
enum JobType {
  Packaging = 'packaging',
  Resource = 'resource'
}

/**
 * Data sent by the worker about it's job.
 * 
 * @typedef {Object} JobData
 * @property {JobType} jobType The type of the job.
 * @property {PackagingInfo|ResourceInfo} info The information about the job.
 */
type JobData = {
  jobType: JobType;
  info: PackagingInfo | ResourceInfo;
}

/**
 * Information about a packaging job.
 * 
 * @typedef {Object} PackagingInfo
 * @property {string} packageId The id of the package being processed.
 * @property {string} version The version of the package being processed.
 */
export type PackagingInfo = {
  packageId: string;
  version: string;
}

/**
 * Information about a resource job.
 * 
 * @typedef {Object} ResourceInfo
 * @property {string} resourceId The id of the resource being processed.
 */
export type ResourceInfo = {
  resourceId: string;
};

import dotenv from 'dotenv';
dotenv.config();

import http from 'http';
import Express from 'express';
import socketio from 'socket.io';
import logger from './logger.js';
import hasha from 'hasha';
import * as jobDatabase from './jobDatabase.js';

logger.info('X-Pkg jobs service starting');

const app = Express();
const server = http.createServer(app);
const io = new socketio.Server(server);

io.on('connection', client => {
  const clientLogger = logger.child({ ip: client.conn.remoteAddress });
  clientLogger.info('New connection');

  let authorized = false;
  let jobsDone = false;

  let jobData: JobData;

  // Dual password authorization for extra security
  client.emit('handshake', process.env.SERVER_TRUST_KEY);

  client.on('handshake', key => {
    if (!key || typeof key !== 'string') {
      client.disconnect();
      clientLogger.info('No password provided or invalid type');
      return;
    }

    const hashed = hasha(key, { algorithm: 'sha256' });
    if (key === hashed) {
      authorized = true;
      logger.emit('Client authorized');
      client.emit('authorized');
    }
  });

  client.on('job_data', async (data: JobData) => {
    if (!authorized) {
      client.disconnect();
      clientLogger.info('Client attempted to send job data when incomplete');
      return;
    }

    jobData = data;
    clientLogger.setBindings(data);
    clientLogger.info('Client data recieved');

    switch (data.jobType) {
    case JobType.Packaging: {
      const jobInfo = data.info as PackagingInfo;
      await jobDatabase.addPackagingJob(jobInfo.packageId, jobInfo.version);
      break;
    }
    // case JobType.Resource:
    //   const jobInfo = data.info as ResourceInfo;
    // break;
    default:
      clientLogger.warn('Invalid job type: ' + data.jobType);
      client.disconnect();
      return;
    }

    clientLogger.info('Job type registered on database');
  });

  client.on('done', async () => {
    jobsDone = true;
    clientLogger.info('Job completed');

    switch (jobData.jobType) {
    case JobType.Packaging: {
      const jobInfo = jobData.info as PackagingInfo;
      await jobDatabase.completePackagingJob(jobInfo.packageId, jobInfo.version);
      break;
    }
    // case JobType.Resource:
    //   const jobInfo = data.info as ResourceInfo;
    // break;
    default:
      clientLogger.error('Invalid job type (while completing)');
      client.disconnect();
      return;
    }
  });

  client.on('disconnect', async () => {
    if (!authorized || jobsDone)
      return;

    clientLogger.info('Unexpected disconnect, attempting to set job as failure');
    let didFail;
    switch (jobData.jobType) {
    case JobType.Packaging: {
      const jobInfo = jobData.info as PackagingInfo;
      didFail = await jobDatabase.failPackagingJob(jobInfo.packageId, jobInfo.version);
      break;
    }
    // case JobType.Resource:
    //   const jobInfo = data.info as ResourceInfo;
    // break;
    default:
      clientLogger.error('Invalid job type (while failing)');
      client.disconnect();
      return;
    }

    if (didFail)
      clientLogger.info('Failed job');
    else
      clientLogger.info('Job already had different status, was not failed');
  });
});

const port = process.env.PORT || 5027;
server.listen(port, () => {
  console.log(`Server is up on port ${port}!`);
});