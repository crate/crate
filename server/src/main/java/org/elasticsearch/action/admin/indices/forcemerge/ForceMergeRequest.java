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

package org.elasticsearch.action.admin.indices.forcemerge;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jspecify.annotations.Nullable;

import io.crate.metadata.PartitionName;

/**
 * A request to force merging the segments of one or more indices. In order to
 * run a merge on all the indices, pass an empty array.
 * {@link #maxNumSegments(int)} allows to control the number of segments
 * to force merge down to. Defaults to simply checking if a merge needs
 * to execute, and if so, executes it
 *
 * @see ForceMergeResponse
 */
public class ForceMergeRequest extends BroadcastRequest {

    public static final class Defaults {
        public static final int MAX_NUM_SEGMENTS = -1;
        public static final boolean ONLY_EXPUNGE_DELETES = false;
        public static final boolean FLUSH = true;
    }

    private int maxNumSegments = Defaults.MAX_NUM_SEGMENTS;
    private boolean onlyExpungeDeletes = Defaults.ONLY_EXPUNGE_DELETES;
    private boolean flush = Defaults.FLUSH;

    private static final Version FORCE_MERGE_UUID_VERSION = Version.V_4_3_0;

    /**
     * Force merge UUID to store in the live commit data of a shard under
     * {@link org.elasticsearch.index.engine.Engine#FORCE_MERGE_UUID_KEY} after force merging it.
     */
    @Nullable
    private final String forceMergeUUID;


    /**
     * Constructs a merge request over one or more indices.
     * An empty list will request a force merge over all indices
     */
    public ForceMergeRequest(List<PartitionName> partitions) {
        super(partitions);
        forceMergeUUID = UUIDs.randomBase64UUID();
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public int maxNumSegments() {
        return maxNumSegments;
    }

    /**
     * Will merge the index down to &lt;= maxNumSegments. By default, will cause the merge
     * process to merge down to half the configured number of segments.
     */
    public ForceMergeRequest maxNumSegments(int maxNumSegments) {
        this.maxNumSegments = maxNumSegments;
        return this;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merging.
     * Defaults to full merging ({@code false}).
     */
    public boolean onlyExpungeDeletes() {
        return onlyExpungeDeletes;
    }

    /**
     * Should the merge only expunge deletes from the index, without full merge.
     * Defaults to full merging ({@code false}).
     */
    public ForceMergeRequest onlyExpungeDeletes(boolean onlyExpungeDeletes) {
        this.onlyExpungeDeletes = onlyExpungeDeletes;
        return this;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public boolean flush() {
        return flush;
    }

    /**
     * Should flush be performed after the merge. Defaults to {@code true}.
     */
    public ForceMergeRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    /**
     * Force merge UUID to use when force merging or {@code null} if not using one in a mixed version cluster containing nodes older than
     * {@link #FORCE_MERGE_UUID_VERSION}.
     */
    @Nullable
    public String forceMergeUUID() {
        return forceMergeUUID;
    }

    public ForceMergeRequest(StreamInput in) throws IOException {
        super(in);
        maxNumSegments = in.readInt();
        onlyExpungeDeletes = in.readBoolean();
        flush = in.readBoolean();
        if (in.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            forceMergeUUID = in.readOptionalString();
        } else {
            forceMergeUUID = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(maxNumSegments);
        out.writeBoolean(onlyExpungeDeletes);
        out.writeBoolean(flush);
        if (out.getVersion().onOrAfter(FORCE_MERGE_UUID_VERSION)) {
            out.writeOptionalString(forceMergeUUID);
        }
    }

    @Override
    public String toString() {
        return "ForceMergeRequest{" +
                "maxNumSegments=" + maxNumSegments +
                ", onlyExpungeDeletes=" + onlyExpungeDeletes +
                ", flush=" + flush +
                '}';
    }
}
