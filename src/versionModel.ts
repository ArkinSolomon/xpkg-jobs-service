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

import mongoose, { Schema } from 'mongoose';

// Partial schema since this is all we use in the version database
const versionSchema = new Schema({
  packageId: {
    type: String,
    required: true
  },
  packageVersion: {
    type: String,
    required: true
  },
  status: {
    type: String,
    required: true,
  },
}, {
  collection: 'versions'
});

const versionDB = mongoose.connection.useDb('packages');
const VersionModel = versionDB.model('version', versionSchema);
export default VersionModel;