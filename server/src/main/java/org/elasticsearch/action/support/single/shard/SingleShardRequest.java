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

package org.elasticsearch.action.support.single.shard;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

public abstract class SingleShardRequest extends TransportRequest {

    protected final Index index;

    ShardId internalShardId;

    public SingleShardRequest(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            internalShardId = new ShardId(in);
        }
        String indexName = IndexMetadata.INDEX_NAME_NA_VALUE;
        String indexUUID = IndexMetadata.INDEX_UUID_NA_VALUE;
        if (in.getVersion().before(Version.V_6_1_0)) {
            indexName = in.readOptionalString();
        } else {
            indexUUID = in.readString();
        }
        index = new Index(indexName, indexUUID);
    }

    protected SingleShardRequest(Index index) {
        this.index = index;
    }

    public Index index() {
        return index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(internalShardId);
        if (out.getVersion().before(Version.V_6_1_0)) {
            out.writeOptionalString(index.getName());
        } else {
            out.writeString(index.getUUID());
        }
    }
}
