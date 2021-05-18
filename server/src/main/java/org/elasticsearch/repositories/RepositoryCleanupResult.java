/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public final class RepositoryCleanupResult implements Writeable {

    private long bytes;

    private long blobs;

    private RepositoryCleanupResult() {
        this(DeleteResult.ZERO);
    }

    public RepositoryCleanupResult(DeleteResult result) {
        this.blobs = result.blobsDeleted();
        this.bytes = result.bytesDeleted();
    }

    public RepositoryCleanupResult(StreamInput in) throws IOException {
        bytes = in.readLong();
        blobs = in.readLong();
    }

    public long bytes() {
        return bytes;
    }

    public long blobs() {
        return blobs;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(bytes);
        out.writeLong(blobs);
    }

    @Override
    public String toString() {
        return "RepositoryCleanupResult{" +
               "bytes=" + bytes +
               ", blobs=" + blobs +
               '}';
    }
}
