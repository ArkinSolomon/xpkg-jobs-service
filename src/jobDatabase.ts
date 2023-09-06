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

interface Job<T extends object> {
  startTime: Date;
  jobData: T;
}

import mongoose, { Model, Schema } from 'mongoose';
import logger from './logger.js';
import { JobType } from './index.js';
import { Logger } from 'pino';

try {
  await mongoose.connect(`mongodb+srv://${process.env.MONGODB_IP}/?authSource=%24external&authMechanism=MONGODB-X509` as string, {
    sslValidate: true,
    tlsCertificateKeyFile: process.env.MONGODB_KEY_PATH,
    authMechanism: 'MONGODB-X509',
    authSource: '$external'
  });
  logger.info('Connected to MongoDB Atlas');
} catch (e) {
  logger.fatal(e);
  process.exit(1);
}

const jobDB = mongoose.connection.useDb('jobs');

/**
 * An instance of this class represents a collection for a specific job type.
 * 
 * @template T
 */
export default class JobDatabase<T extends object> {

  private _jobType: JobType;
  private _dbLogger: Logger;

  private _internalSchema: Schema<Job<T>>;
  private _JobModel: Model<Job<T>>;

  private _failJob: (jobData: T) => Promise<void>;

  /**
   * Create a new database (a MongoDB collection) for a specific type of job.
   * 
   * @constructor
   * @param {JobType} jobType The type of job this database is for.
   * @param {(T) => Promise<void>} failJob The function that executes when the job fails.
   */
  constructor(jobType: JobType, failJob: (jobData: T) => Promise<void>) {
    this._jobType = jobType;
    this._failJob = failJob;
    this._dbLogger = logger.child({
      jobDbType: this._jobType
    });

    this._internalSchema = new Schema<Job<T>>({
      startTime: {
        type: Date,
        required: true
      },
      jobData: {
        type: Schema.Types.Mixed,
        required: true
      }
    }, {
      collection: this._jobType
    });

    this._JobModel = jobDB.model<Job<T>>(this._jobType, this._internalSchema);
  }

  /**
   * Add a job to the database if it does not exist. Nothing is changed if the job exists.
   * 
   * @param {T} jobData The job to add.
   * @returns {Promise<void>} A promise which is resolved after the job has been saved.
   */
  async addJob(jobData: T): Promise<void> {
    this._dbLogger.trace(jobData, 'Adding job');
    await this._JobModel.updateOne({ jobData }, {
      $setOnInsert: {
        startTime: new Date(),
        jobData
      }
    }, { upsert: true }).exec();
    this._dbLogger.trace(jobData, 'Added job');
  }

  /**
   * Remove a job.
   * 
   * @param {T} jobData The job to remove.
   * @returns {Promise<void>} A promise which is resolved after the job has been removed.
   */
  async removeJob(jobData: T): Promise<void> {
    this._dbLogger.trace(jobData, 'Removing job');
    await this._JobModel.findOneAndRemove({
      jobData,
    }).exec();
    this._dbLogger.trace(jobData, 'Removed job');
  }

  /**
   * Remove a job from the database, and set its status to failed.
   * 
   * @param {T} jobData The job to fail.
   * @returns {Promise<void>} A promise which is resolved after the job has been failed. 
   */
  async failJob(jobData: T): Promise<void> {
    this._dbLogger.trace(jobData, 'Failing job');

    await Promise.all([
      this.removeJob(jobData),
      this._failJob(jobData)
    ]);

    this._dbLogger.trace(jobData, 'Failed job');
  }

  /**
   * Get all jobs of this type (without the start time).
   * 
   * @returns {Promise<T[]>} A promise which resolves to the start time of this job.
   */
  async getAllJobs(): Promise<T[]> {
    this._dbLogger.trace('Getting all jobs');
    const jobs = await this._JobModel
      .find()
      .select('jobData')
      .lean()
      .exec();
  
    this._dbLogger.trace('Got all jobs');
    return jobs.map(j => j.jobData as T);
  }

  /**
   * Get all jobs of this type (with the start time).
   * 
   * @returns {Promise<(T & {startTime: Date;})[]>} All jobs of this type, with the start time.
   */
  async getAllJobsWithTime(): Promise<(T & { startTime: Date; })[]> {
    this._dbLogger.trace('Getting all jobs with time');
    const jobs = await this._JobModel
      .find()
      .select('-_id -__v')
      .lean()
      .exec();
    
    const ret = (jobs as Array<Job<T>>).map(j => ({
      startTime: j.startTime,
      ...j.jobData
    }));
    this._dbLogger.trace('Got all jobs with time');
    return ret;
  }
}