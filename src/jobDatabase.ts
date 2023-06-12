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

import mysql from 'mysql2';
import { PackagingInfo } from './index.js';

const pool = mysql.createPool({
  connectionLimit: 15,
  host: '127.0.0.1',
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: 'xpkg_packages',
  multipleStatements: false
});

/**
 * Execute a query string.
 * 
 * @param {string} queryString The query string to execute.
 * @returns {Promise<unknown[]>} A promise which resolves to the result of the query data, or rejects if the operation fails.
 */
function query(queryString: string): Promise<unknown[]> {
  return new Promise((resolve, reject) => {
    pool.getConnection((err, connection) => {
      if (err)
        return reject(err);

      connection.query(queryString, (err, data) => {
        connection.release();
        if (err)
          return reject(err);

        resolve(data as unknown[]);
      });
      connection.on('error', reject);
    });
  });
}

/**
 * Register a job that packages a specific package at a version.
 * 
 * @async
 * @param {string} packageId The id of the package being packaged.
 * @param {string} version The version being packaged.
 * @returns {Promise<void>} A promise which resolves if the operation completes successfully.
 */
export async function addPackagingJob(packageId: string, version: string): Promise<void> {
  await query(mysql.format('INSERT INTO package_processing_jobs (packageId, version) VALUES (?, ?);', [packageId, version]));
}

/**
 * Complete a packaging job.
 * 
 * @async
 * @param {string} packageId The id of the package who's job is complete.
 * @param {string} version The version of the package who's job is complete.
 * @returns {Promise<void>} A promise which resolves if the operation completes successfully.
 */
export async function completePackagingJob(packageId: string, version: string) {
  return query(mysql.format('DELETE FROM package_processing_jobs WHERE packageId=? AND version=?;', [packageId, version]));
}

/**
 * Fail a packaging job.
 * 
 * @async
 * @param {string} packageId The id of the package who's job is being failed.
 * @param {string} version The version of the package who's job is being failed.
 * @returns {Promise<boolean>} A promise which resolves to true if the job was set as failed, or false if a different status was already inserted into the database.
 */
export async function failPackagingJob(packageId: string, version: string): Promise<boolean> {
  const [, status] = await Promise.all([
    completePackagingJob(packageId, version),
    query(mysql.format('SELECT status FROM versions WHERE packageId=? AND version=? LIMIT 1;', [packageId, version]))
  ]);

  // We always expect the job to be there, so this shouldn't fail
  const jobStatus = (status as { status: string; }[])[0].status;

  // Only update the job if it's processing, otherwise assume it's finished
  if (jobStatus === 'processing') {
    await query(mysql.format('UPDATE versions SET status=? WHERE packageId=? AND version=?;', ['failed_server', packageId, version]));
    return true;
  }
  return false;
}

/**
 * Get all packaging jobs.
 * 
 * @async
 * @returns {Promise<(PackagingInfo & {startTime: Date})[]>} A promise which resolves to all of hte information of all packaging jobs.
 */
export async function getAllPackagingJobs(): Promise<(PackagingInfo & {startTime: Date})[]> {
  return (await query('SELECT packageId, version, startTime FROM package_processing_jobs;')) as (PackagingInfo & {startTime: Date})[];
}