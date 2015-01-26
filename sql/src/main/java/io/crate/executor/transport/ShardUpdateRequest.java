/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport;

import io.crate.Constants;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ShardUpdateRequest extends InstanceShardOperationRequest<ShardUpdateRequest> implements DocumentRequest<ShardUpdateRequest> {

    private String id;
    @Nullable
    private String routing;
    private long version = Versions.MATCH_ANY;
    private Map<String, Symbol> assignments;

    @Nullable
    private Reference[] missingAssignmentsColumns;
    @Nullable
    private Object[] missingAssignments;

    public ShardUpdateRequest() {
    }

    public ShardUpdateRequest(String index, String id) {
        super(index);
        this.id = id;
    }

    public ShardUpdateRequest(ShardId shardId, Uid uid, @Nullable Long version) {
        this(shardId.getIndex(), uid.id());
        this.shardId = shardId.id();
        if (version != null) {
            version(version);
        }
    }

    @Override
    public String type() {
        return Constants.DEFAULT_MAPPING_TYPE;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public ShardUpdateRequest routing(@Nullable String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    @Override
    @Nullable
    public String routing() {
        return routing;
    }

    public ShardUpdateRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return version;
    }

    public ShardUpdateRequest shardId(int shardId) {
        this.shardId = shardId;
        return this;
    }

    public int shardId() {
        return shardId;
    }

    public int retryOnConflict() {
        return version == Versions.MATCH_ANY ? Constants.UPDATE_RETRY_ON_CONFLICT : 0;
    }

    public ShardUpdateRequest assignments(Map<String, Symbol> assignments) {
        this.assignments = assignments;
        return this;
    }

    public Map<String, Symbol> assignments() {
        return assignments;
    }

    @Nullable
    public Object[] missingAssignments() {
        return missingAssignments;
    }

    public void missingAssignments(@Nullable Object[] missingAssignments) {
        this.missingAssignments = missingAssignments;
    }

    public Reference[] missingAssignmentsColumns() {
        return missingAssignmentsColumns;
    }

    public void missingAssignmentsColumns(Reference[] missingAssignmentsColumns) {
        this.missingAssignmentsColumns = missingAssignmentsColumns;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readString();
        routing = in.readOptionalString();
        version = Versions.readVersion(in);
        int mapSize = in.readVInt();
        assignments = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            assignments.put(in.readString(), Symbol.fromStream(in));
        }
        int missingAssignmentsColumnsSize = in.readVInt();
        missingAssignmentsColumns = new Reference[missingAssignmentsColumnsSize];
        for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
            missingAssignmentsColumns[i] = (Reference)Reference.fromStream(in);
        }

        int missingAssignmentsSize = in.readVInt();
        this.missingAssignments = new Object[missingAssignmentsSize];
        for (int i = 0; i < missingAssignmentsSize; i++) {
            Reference ref = missingAssignmentsColumns[i];
            missingAssignments[i] = ref.valueType().streamer().readValueFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(id);
        out.writeOptionalString(routing);
        Versions.writeVersion(version, out);
        out.writeVInt(assignments.size());
        for (Map.Entry<String, Symbol> entry : assignments.entrySet()) {
            out.writeString(entry.getKey());
            Symbol.toStream(entry.getValue(), out);
        }
        // Stream References
        if (missingAssignmentsColumns != null) {
            out.writeVInt(missingAssignmentsColumns.length);
            for(Reference reference : missingAssignmentsColumns) {
                Reference.toStream(reference, out);
            }
        } else {
            out.writeVInt(0);
        }
        if (missingAssignments != null) {
            out.writeVInt(missingAssignments.length);
            for (int i = 0; i < missingAssignments.length; i++) {
                Reference reference = missingAssignmentsColumns[i];
                reference.valueType().streamer().writeValueTo(out, missingAssignments[i]);
            }
        } else {
            out.writeVInt(0);
        }
    }

}
