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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, all statistics are enabled.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequest extends BroadcastRequest<IndicesStatsRequest> {

    private CommonStatsFlags flags = new CommonStatsFlags();

    /**
     * Sets all flags to return all stats.
     */
    public IndicesStatsRequest all() {
        flags.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequest clear() {
        flags.clear();
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public IndicesStatsRequest types(String... types) {
        flags.types(types);
        return this;
    }


    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public IndicesStatsRequest groups(String... groups) {
        flags.groups(groups);
        return this;
    }

    public String[] groups() {
        return this.flags.groups();
    }

    public IndicesStatsRequest docs(boolean docs) {
        flags.set(Flag.Docs, docs);
        return this;
    }

    public boolean docs() {
        return flags.isSet(Flag.Docs);
    }

    public IndicesStatsRequest store(boolean store) {
        flags.set(Flag.Store, store);
        return this;
    }

    public boolean store() {
        return flags.isSet(Flag.Store);
    }

    public IndicesStatsRequest fieldDataFields(String... fieldDataFields) {
        flags.fieldDataFields(fieldDataFields);
        return this;
    }

    public String[] fieldDataFields() {
        return flags.fieldDataFields();
    }

    public IndicesStatsRequest completion(boolean completion) {
        flags.set(Flag.Completion, completion);
        return this;
    }

    public boolean completion() {
        return flags.isSet(Flag.Completion);
    }

    public IndicesStatsRequest completionFields(String... completionDataFields) {
        flags.completionDataFields(completionDataFields);
        return this;
    }

    public String[] completionFields() {
        return flags.completionDataFields();
    }

    public boolean includeSegmentFileSizes() {
        return flags.includeSegmentFileSizes();
    }

    public IndicesStatsRequest includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        flags.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flags.writeTo(out);
    }

    public IndicesStatsRequest(StreamInput in) throws IOException {
        super(in);
        flags = new CommonStatsFlags(in);
    }

    public IndicesStatsRequest() {
    }
}
