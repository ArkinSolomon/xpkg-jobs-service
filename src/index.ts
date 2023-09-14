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
  packageVersion: string;
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
  const failLogger = logger.child({
    packageId: j.packageId,
    packageVersion: j.packageVersion,
  });
  failLogger.trace('Failing packaging job');
  await VersionModel
    .findOneAndUpdate({
      packageId: j.packageId,
      packageVersion: j.packageVersion,
      status: 'processing'
    }, {
      $set: {
        status: 'failed_server'
      }
    })
    .exec();
  failLogger.trace('Failed packaging job');
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

const packagingComparer = (job1: PackagingInfo, job2: PackagingInfo) => job1.packageId === job2.packageId && job1.packageVersion === job2.packageVersion;

const unclaimedPackagingJobs = (await packagingDatabase.getAllJobsWithTime()).map(j => (
  <PackagingInfo>{
    packageId: j.packageId,
    packageVersion: j.packageVersion
  }
));
const packagingJobClaimer = new JobClaimer<PackagingInfo>(unclaimedPackagingJobs, packagingComparer, packagingDatabase);

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
setTimeout(async () => {
  logger.info('Allowing jobs to be aborted');
  setInterval(createJobAborter(JobType.Packaging, packagingDatabase, packagingComparer), ONE_HOUR_MS / 2);

  // Register any jobs that were never registered with the jobs service
  setInterval(async () => {
    const processingVersions = await VersionModel.find({
      status: 'processing'
    })
      .exec();

    for (const processingVersion of processingVersions) {
      const jobInfo = {
        packageId: processingVersion.packageId,
        packageVersion: processingVersion.packageVersion
      };
      logger.info(jobInfo, 'Registering unregistered job');
      await packagingDatabase.addJob(jobInfo);
    }
  }, ONE_HOUR_MS);
}, 90000);

io.on('connection', client => {
  const clientLogger = logger.child({ ip: client.conn.remoteAddress });
  clientLogger.trace('New connection');

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
      clientLogger.warn('Client sent job data when unauthorized');
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
    clientLogger.trace('Client data recieved');

    switch (data.jobType) {
    case JobType.Packaging: {
        
      // We do not know if these are the only keys provided, so only select them
      let sentJobInfo = data.info as PackagingInfo;
      sentJobInfo = {
        packageId: sentJobInfo.packageId,
        packageVersion: sentJobInfo.packageVersion
      };
      jobInfo = sentJobInfo;
      
      if (!sentJobInfo.packageId || typeof sentJobInfo.packageId !== 'string' || !sentJobInfo.packageVersion || typeof sentJobInfo.packageVersion !== 'string')
      {
        client.disconnect();
        clientLogger.error('Client sent invalid packaging job data');
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
      clientLogger.info('Client sent job completion without authorization, or with no job data');
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
      clientLogger.warn(`Unauthorized socket disconnected (${reason})`);
      return;
    }

    if (!jobInfo || !jobType) {
      clientLogger.warn(`Client disconnected without sending jobs data (${reason})`);
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

    clientLogger.info(`Unexpected worker disconnect, attempting to set job as failure (${reason})`);
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

    clientLogger.trace('Tried to fail job');
  });

  client.emit('handshake', process.env.SERVER_TRUST_KEY);

  setTimeout(() => {
    if (authorized && jobInfo)
      return;
    clientLogger.warn('Client not authorized or did not send job information within 30 seconds, disconnecting');
    client.disconnect();
  }, 30000);
});

const port = process.env.PORT || 443;
server.listen(port, () => {
  logger.info(`X-Pkg jobs service is up on port ${port}`);
});

/**
 * Create a new asynchronous function that will try to abort all jobs that take too long for a specific job database
 * 
 * @template T 
 * @param {JobType} jobType The type of job to abort.
 * @param {JobDatabase<T>} abortDatabase The database which contains the jobs to abort.
 * @param {(T, T) => boolean} comparer The function which compares two jobs and determines if they are equivalent. Returns true if they are, or false otherwise.
 * @returns {() => Promise<void>} An asynchronous that will abort all jobs (or fail them) when run.
 */
function createJobAborter<T extends object>(jobType: JobType, abortDatabase: JobDatabase<T>, comparer: (job1: T, job2: T) => boolean): () => Promise<void> {
  return async function () {
    const jobs = await abortDatabase.getAllJobsWithTime() as (T & { startTime?: Date })[];
    
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const abortJobs = jobs.filter(({ startTime: t }) => t!.getTime() < Date.now() - THREE_HOUR_MS);

    for (const abortJob of abortJobs) {
      const abortLogger = logger.child(abortJob);

      // Since we're awaiting, we have to filter again every loop
      const jobClients = clients.filter(c => c.jobData.jobType == jobType);
      const client = jobClients.find(c => comparer(c.jobData.info as T, abortJob));
      
      delete abortJob.startTime;

      // Try to fail it, no worries if it just happened to complete in that short time
      if (!client || !client.client.connected) {
        if (client)
          abortLogger.info('Could not abort job, client found but not connected, failing instead');
        else
          abortLogger.info('Could not abort job, no client found, failing instead');
        await abortDatabase.failJob(abortJob);
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
      }, 5000);
    }
  };
}