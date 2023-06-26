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
export enum JobType {
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
type PackagingInfo =  { 
  packageId: string;
  version: string;
}

/**
 * Information about a resource job.
 * 
 * @typedef {Object} ResourceInfo
 * @property {string} resourceId The id of the resource being processed.
 */
type ResourceInfo = {
  resourceId: string;
};

import dotenv from 'dotenv';
dotenv.config();

import logger from './logger.js';
logger.info('X-Pkg jobs service starting');

import fs from 'fs/promises';
import https from 'https';
import Express from 'express';
import {Server, Socket} from 'socket.io';
import hasha from 'hasha';
import JobDatabase from './jobDatabase.js';
import JobClaimer from './jobClaimer.js';
import VersionModel from './versionModel.js';

const packagingDatabase = new JobDatabase<PackagingInfo>(JobType.Packaging, async j => {
  await VersionModel
    .findOneAndUpdate({
      ...j,
      status: 'processing'
    }, {
      status: 'failed_server'
    })
    .exec();
});

const app = Express();
const [key, cert, ca] = await Promise.all([
  fs.readFile(process.env.HTTPS_KEY_PATH as string, 'utf8'),
  fs.readFile(process.env.HTTPS_CERT_PATH as string, 'utf8'),
  fs.readFile(process.env.HTTPS_CHAIN_PATH as string, 'utf8'),
]);
const server = https.createServer({
  key, cert, ca
}, app);
const io = new Server(server);

// If all jobs are not reclaimed within 10 minutes of boot set them to being failed
const unclaimedPackagingJobs = await packagingDatabase.getAllJobsWithTime();
const packagingJobClaimer = new JobClaimer<PackagingInfo>(unclaimedPackagingJobs, (job1, job2) => job1.packageId === job2.packageId && job1.version === job2.version, packagingDatabase);

if (!unclaimedPackagingJobs.length)
  logger.info('No unclaimed packaging jobs');
else if (unclaimedPackagingJobs.length === 1)
  logger.info('1 unclaimed packaging job');
else
  logger.info(unclaimedPackagingJobs.length + ' unclaimed packaging jobs');

// A list of all of the clients connected to the service, with their job information and socket
const clients: { jobData: JobData; client: Socket; }[] = [];

const ONE_HOUR_MS = 60 * 60 * 1000;
const THREE_HOUR_MS = 3 * ONE_HOUR_MS;

// We want to give a chance for all jobs to be claimed
setTimeout(() => {
  logger.info('Allowing package jobs to be aborted');
  setInterval(async function(){
    const packagingJobs = await packagingDatabase.getAllJobsWithTime();
    const abortJobs = packagingJobs.filter(({ startTime: t }) => t.getTime() < Date.now() - THREE_HOUR_MS);

    for (const abortJob of abortJobs) {
      const abortLogger = logger.child(abortJob);

      // Since we're awaiting, we have to filter again every loop
      const packagingClients = clients.filter(c => c.jobData.jobType == JobType.Packaging);
      const client = packagingClients.find(c => (c.jobData.info as PackagingInfo).packageId === abortJob.packageId && (c.jobData.info as PackagingInfo).version === abortJob.version);
      
      // Try to fail it, no worries if it just happened to complete in that short time
      if (!client || !client.client.connected) {
        if (client)
          abortLogger.info('Could not abort job, client found but not connected, failing instead');
        else
          abortLogger.info('Could not abort job, no client found, failing instead');
        await packagingDatabase.failJob(abortJob);
        continue;
      }

      abortLogger.warn('Aborting job');
      client.client.emit('abort');
      
      let aborted = false;
      client.client.once('aborting', () => {
        aborted = true;
        abortLogger.info('Worker is aborting job');
      });

      setTimeout(() => {
        if (aborted)
          return;
        abortLogger.info('Disconnecting worker that is not aborting job');
        client.client.disconnect();
      }, 250);
    } 
  }, ONE_HOUR_MS / 2);
}, 90000);

io.on('connection', client => {
  const clientLogger = logger.child({ ip: client.conn.remoteAddress });
  clientLogger.info('New connection');

  let authorized = false;
  let jobDone = false;

  let jobType: JobType | undefined;
  let jobInfo: ResourceInfo | PackagingInfo;

  client.on('handshake', password => {
    if (!password || typeof password !== 'string') {
      client.disconnect();
      clientLogger.warn('No password provided or invalid type');
      return;
    }

    const hashed = hasha(password, { algorithm: 'sha256' });
    if (hashed === process.env.JOBS_SERVICE_HASH) {
      authorized = true;
      logger.emit('Client authorized');
      client.emit('authorized');
    } else {
      client.disconnect();
      clientLogger.warn('Invalid password provided');
    }
  });

  client.on('job_data', async (data: JobData) => {
    if (!authorized) {
      client.disconnect();
      clientLogger.warn('Client attempted to send job data when unauthorized');
      return;
    }

    if (!data || !data.jobType) {
      client.disconnect();
      if (data)
        logger.warn('Client did not send job type with job data');
      else
        logger.warn('Client did not send job data with job_data event');
      return;
    }

    jobType = data.jobType;
    clientLogger.setBindings(data);
    clientLogger.info('Client data recieved');

    switch (data.jobType) {
    case JobType.Packaging: {
        
      // We do not know if these are the only keys provided, so only select them
      let sentJobInfo = data.info as PackagingInfo;
      sentJobInfo = {
        packageId: sentJobInfo.packageId,
        version: sentJobInfo.version
      };
      jobInfo = sentJobInfo;
      
      if (!sentJobInfo.packageId || typeof sentJobInfo.packageId !== 'string' || !sentJobInfo.version || typeof sentJobInfo.version !== 'string')
      {
        client.disconnect();
        clientLogger.error('Client attempted to send invalid packaging job data');
        return;
      }

      await packagingDatabase.addJob(sentJobInfo);
      packagingJobClaimer.tryClaimJob(sentJobInfo);
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

    clients.push({
      jobData: {
        jobType,
        info: jobInfo
      }, client});
    clientLogger.info('Job registered on database');
    client.emit('job_data_recieived');
  });

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  client.on('done', async reason => {
    if (!authorized || !jobInfo || !jobType) {
      client.disconnect();
      clientLogger.info('Client attempted to state job completed without authorization, or with no job data');
      return;
    }

    jobDone = true;
    clientLogger.info(`Worker stating job completed (${reason})`);

    switch (jobType) {
    case JobType.Packaging: {
      await packagingDatabase.removeJob(jobInfo as PackagingInfo);
      break;
    }
    // case JobType.Resource:
    //   const jobInfo = data.info as ResourceInfo;
    // break;
    default:
        
      // This shouldn't reach
      clientLogger.error('Invalid job type (while completing)');
      client.disconnect();
      return;
    }

    logger.emit('Removed job from database');
    client.emit('goodbye');
  });

  client.on('disconnect', async reason => {
    if (!authorized) {
      clientLogger.info(`Unauthorized socket disconnected (${reason})`);
      return;
    }

    if (!jobInfo || !jobType) {
      clientLogger.info(`Disconnected without sending jobs data (${reason})`);
      return;
    }

    const thisIndex = clients.findIndex(({ client: c }) => c === client);
    clients.splice(thisIndex, 1);

    if (jobDone) {
      clientLogger.info(`Completed worker has successfully disconnected from jobs service (${reason})`);
      return;
    } else if (reason === 'server namespace disconnect') {
      clientLogger.info('Worker would not respond to abort request (server namespace disconnect)');
      return;
    }

    clientLogger.info(`Unexpected disconnect, attempting to set job as failure (${reason})`);
    switch (jobType) {
    case JobType.Packaging: {
      packagingDatabase.failJob(jobInfo as PackagingInfo);
      break;
    }
    // case JobType.Resource:
    //   const jobInfo = data.info as ResourceInfo;
    // break;
    default:
        
      // Shouldn't reach
      clientLogger.error('Invalid job type (while failing)');
      client.disconnect();
      return;
    }

    clientLogger.info('Tried to fail job');
  });

  // Dual password authorization for extra security
  client.emit('handshake', process.env.SERVER_TRUST_KEY);
});

const port = process.env.PORT || 443;
server.listen(port, () => {
  logger.info(`X-Pkg jobs service is up on port ${port}`);
});