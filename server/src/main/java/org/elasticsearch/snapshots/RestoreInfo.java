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

package org.elasticsearch.snapshots;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;

/**
 * Information about successfully completed restore operation.
 * <p>
 * Returned as part of {@link org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse}
 */
public class RestoreInfo implements Writeable {

    private final String name;
    private final List<String> indices;
    private final int totalShards;
    private final int successfulShards;

    public RestoreInfo(String name, List<String> indices, int totalShards, int successfulShards) {
        this.name = name;
        this.indices = indices;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
    }

    public RestoreInfo(StreamInput in) throws IOException {
        name = in.readString();
        indices = Collections.unmodifiableList(in.readStringList());
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
    }

    /**
     * Snapshot name
     *
     * @return snapshot name
     */
    public String name() {
        return name;
    }

    /**
     * List of restored indices
     *
     * @return list of restored indices
     */
    public List<String> indices() {
        return indices;
    }

    /**
     * Number of shards being restored
     *
     * @return number of being restored
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * Number of failed shards
     *
     * @return number of failed shards
     */
    public int failedShards() {
        return totalShards - successfulShards;
    }

    /**
     * Number of successful shards
     *
     * @return number of successful shards
     */
    public int successfulShards() {
        return successfulShards;
    }

    /**
     * REST status of the operation
     *
     * @return REST status
     */
    public RestStatus status() {
        return RestStatus.OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringCollection(indices);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestoreInfo that = (RestoreInfo) o;
        return totalShards == that.totalShards &&
            successfulShards == that.successfulShards &&
            Objects.equals(name, that.name) &&
            Objects.equals(indices, that.indices);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indices, totalShards, successfulShards);
    }
}
