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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal validate request executed directly against a specific index shard.
 */
public class ShardValidateQueryRequest extends BroadcastShardRequest {

    private QueryBuilder query;
    private String[] types = Strings.EMPTY_ARRAY;
    private boolean explain;
    private boolean rewrite;
    private long nowInMillis;
    private AliasFilter filteringAliases;

    public ShardValidateQueryRequest() {
    }

    public ShardValidateQueryRequest(ShardId shardId, AliasFilter filteringAliases, ValidateQueryRequest request) {
        super(shardId, request);
        this.query = request.query();
        this.types = request.types();
        this.explain = request.explain();
        this.rewrite = request.rewrite();
        this.filteringAliases = Objects.requireNonNull(filteringAliases, "filteringAliases must not be null");
        this.nowInMillis = request.nowInMillis;
    }

    public QueryBuilder query() {
        return query;
    }

    public String[] types() {
        return this.types;
    }

    public boolean explain() {
        return this.explain;
    }

    public boolean rewrite() {
        return this.rewrite;
    }

    public AliasFilter filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        query = in.readNamedWriteable(QueryBuilder.class);

        int typesSize = in.readVInt();
        if (typesSize > 0) {
            types = new String[typesSize];
            for (int i = 0; i < typesSize; i++) {
                types[i] = in.readString();
            }
        }
        filteringAliases = new AliasFilter(in);
        explain = in.readBoolean();
        rewrite = in.readBoolean();
        nowInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        out.writeVInt(types.length);
        for (String type : types) {
            out.writeString(type);
        }
        filteringAliases.writeTo(out);
        out.writeBoolean(explain);
        out.writeBoolean(rewrite);
        out.writeVLong(nowInMillis);
    }
}
