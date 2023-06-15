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

import { JobType, PackagingInfo, ResourceInfo } from './index.js';
import * as jobDatabase from './jobDatabase.js';
import logger from './logger.js';

/**
 * An instance of this class manages claiming jobs for a single type of job.
 */
export default class JobClaimer {
  
  private _jobType: JobType;
  private _jobList: (PackagingInfo | ResourceInfo)[];

  private _claimedJobs: (PackagingInfo | ResourceInfo)[] = [];

  private _locked = false;

  /**
   * Create a new claimer with a list of jobs that can be claimed. Automatically starts the timer to fail unclaimed jobs.
   * 
   * @constructor
   * @param {JobType} jobType The type of jobs this claimer can claim.
   * @param {(PackagingInfo|ResourceInfo)[]} jobList The list of jobs that are available to be claimed.
   */
  constructor(jobType: JobType, jobList: (PackagingInfo | ResourceInfo)[]) {
    this._jobType = jobType;
    this._jobList = jobList;

    if (this._jobList.length) {
      setTimeout(async () => {
        this._locked = true;

        // We shouldn't have *that* many jobs so it should be fine to do this
        for (const job of this._jobList) {
          const index = this._claimedJobs.findIndex(j => this._doJobsMatch(j, job));
          if (index > -1) {
            this._claimedJobs.splice(index, 1);

            switch (this._jobType) {
            case JobType.Packaging: 
              logger.info(job, 'Packaging job claimed');
              break;
            case JobType.Resource:
              logger.info(job, 'Resource job claimed');
              break;
            default:
              throw new Error('Invalid job type (can not log claimed job)');
            }
          }
          else {

            // Fail the job based on its type
            switch (this._jobType) {
            case JobType.Packaging: {
              const j = job as PackagingInfo;
              logger.info(j, 'Failing unclaimed packaging job');
              await jobDatabase.failPackagingJob(j.packageId, j.version);
              break;
            }
            case JobType.Resource:
              //TODO
              break;
            default:
              throw new Error('Invalid job type (can not fail unclaimed jobs)');
            }
          }
        }
      }, 60000);
    }
  }

  /**
   * 
   * 
   * @param {PackagingInfo|ResourceInfo} jobInfo The information of the job to claim.
   */
  tryClaimJob(jobInfo: PackagingInfo | ResourceInfo): void {
    if (this._locked)
      return;
    
    this._claimedJobs.push(jobInfo);
  }

  /**
   * Compare two jobs and determine if they are equal.
   * 
   * @param {PackagingInfo|ResourceInfo} job1 The first job to compare.
   * @param {PackagingInfo|ResourceInfo} job2 The second job to compare.
   * @returns {boolean} True if the jobs are the same.
   */
  private _doJobsMatch(job1: PackagingInfo | ResourceInfo, job2: PackagingInfo | ResourceInfo): boolean {
    switch (this._jobType) {
    case JobType.Packaging:
    {
      const j1 = job1 as PackagingInfo;
      const j2 = job2 as PackagingInfo;
      return j1.packageId === j2.packageId && j1.version === j2.version;
    }
    case JobType.Resource: {
      const j1 = job1 as ResourceInfo;
      const j2 = job2 as ResourceInfo;
      return j1.resourceId === j2.resourceId;
    }
    default:
      throw new Error('Invalid job type (can not match)');
    }
  }
}