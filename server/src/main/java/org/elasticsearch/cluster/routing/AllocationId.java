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

package org.elasticsearch.cluster.routing;

import java.io.IOException;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.jspecify.annotations.Nullable;

/**
 * Uniquely identifies an allocation. An allocation is a shard moving from unassigned to initializing,
 * or relocation.
 * <p>
 * Relocation is a special case, where the origin shard is relocating with a relocationId and same id, and
 * the target shard (only materialized in RoutingNodes) is initializing with the id set to the origin shard
 * relocationId. Once relocation is done, the new allocation id is set to the relocationId. This is similar
 * behavior to how ShardRouting#currentNodeId is used.
 */
public record AllocationId(
        /// The allocation id uniquely identifying an allocation, note, if it
        /// is a relocation the [#relocationId()] need to be taken into account
        /// as well.
        String id,

        /// The transient relocation id holding the unique id that is used for relocation.
        @Nullable String relocationId) implements Writeable {

    private static final String ID_KEY = "id";
    private static final String RELOCATION_ID_KEY = "relocation_id";

    private static final ObjectParser<AllocationId.Builder, Void> ALLOCATION_ID_PARSER = new ObjectParser<>(
            "allocationId");

    static {
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setId, new ParseField(ID_KEY));
        ALLOCATION_ID_PARSER.declareString(AllocationId.Builder::setRelocationId, new ParseField(RELOCATION_ID_KEY));
    }

    private static class Builder {
        private String id;
        private String relocationId;

        public void setId(String id) {
            this.id = id;
        }

        public void setRelocationId(String relocationId) {
            this.relocationId = relocationId;
        }

        public AllocationId build() {
            return new AllocationId(id, relocationId);
        }
    }

    public AllocationId(StreamInput in) throws IOException {
        this(in.readString(), in.readOptionalString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.id);
        out.writeOptionalString(this.relocationId);
    }

    /**
     * Creates a new allocation id for initializing allocation.
     */
    public static AllocationId newInitializing() {
        return new AllocationId(UUIDs.randomBase64UUID(), null);
    }

    /**
     * Creates a new allocation id for initializing allocation based on an existing id.
     */
    public static AllocationId newInitializing(String existingAllocationId) {
        return new AllocationId(existingAllocationId, null);
    }

    /**
     * Creates a new allocation id for the target initializing shard that is the result
     * of a relocation.
     */
    public static AllocationId newTargetRelocation(AllocationId allocationId) {
        assert allocationId.relocationId() != null;
        return new AllocationId(allocationId.relocationId(), allocationId.id());
    }

    /**
     * Creates a new allocation id for a shard that moves to be relocated, populating
     * the transient holder for relocationId.
     */
    public static AllocationId newRelocation(AllocationId allocationId) {
        assert allocationId.relocationId() == null;
        return new AllocationId(allocationId.id(), UUIDs.randomBase64UUID());
    }

    /**
     * Creates a new allocation id representing a cancelled relocation.
     * <p>
     * Note that this is expected to be called on the allocation id
     * of the *source* shard
     */
    public static AllocationId cancelRelocation(AllocationId allocationId) {
        assert allocationId.relocationId() != null;
        return new AllocationId(allocationId.id(), null);
    }

    /**
     * Creates a new allocation id finalizing a relocation.
     * <p>
     * Note that this is expected to be called on the allocation id
     * of the *target* shard and thus it only needs to clear the relocating id.
     */
    public static AllocationId finishRelocation(AllocationId allocationId) {
        assert allocationId.relocationId() != null;
        return new AllocationId(allocationId.id(), null);
    }

    @Override
    public String toString() {
        return "[id=" + id + (relocationId == null ? "" : ", rId=" + relocationId) + "]";
    }

    public static AllocationId fromXContent(XContentParser parser) throws IOException {
        return ALLOCATION_ID_PARSER.parse(parser, new AllocationId.Builder(), null).build();
    }
}
