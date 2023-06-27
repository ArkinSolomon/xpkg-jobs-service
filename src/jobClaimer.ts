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
 * Compare two jobs, and determine if they are equal.
 * 
 * @callback JobComparer
 * @template T
 * @param {T} job1 The first job to compare.
 * @param {T} job2 The second job to compare.
 * @returns {boolean} True if the jobs are equal, or false otherwise.
 */
type JobComparer<T> = (job1: T, job2: T) => boolean;

import JobDatabase from './jobDatabase.js';
import logger from './logger.js';

/**
 * An instance of this class manages claiming jobs for a single type of job.
 * 
 * @template T
 */
export default class JobClaimer<T extends object> {
  
  private _jobList: T[];
  private _jobComparer: JobComparer<T>;

  private _claimedJobs: T[] = [];
  private _locked = false;  

  /**
   * Create a new claimer with a list of jobs that can be claimed. Automatically starts the timer to fail unclaimed jobs.
   * 
   * @constructor
   * @param {T[]} jobList The list of jobs that are available to be claimed.
   * @param {JobComparer<T>} comparer The function to compare two jobs and determine if they are equal.
   * @param {JobDatabase<T>} jobDatabase The database that is keeping track of these jobs.
   */
  constructor(jobList: T[], jobComparer: JobComparer<T>, jobDatabase: JobDatabase<T>) {
    this._jobComparer = jobComparer;
    this._jobList = jobList;

    if (this._jobList.length) {
      setTimeout(async () => {
        this._locked = true;

        // We shouldn't have *that* many jobs so it should be fine to do this
        for (const job of this._jobList) {
          const index = this._claimedJobs.findIndex(j => this._jobComparer(j, job));
          if (index > -1) {
            this._claimedJobs.splice(index, 1);
            logger.info(job, 'Claimed job');
          } else {
            logger.info(job, 'Failing unclaimed job');
            await jobDatabase.failJob(job);
          }
        }
      }, 60000);
    }
  }

  /**
   * Try to claim a job.
   * 
   * @param {T} jobInfo The information of the job to claim.
   */
  tryClaimJob(jobInfo: T): void {
    if (this._locked)
      return;
    
    this._claimedJobs.push(jobInfo);
  }
}