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

package org.elasticsearch.action;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.jspecify.annotations.Nullable;

import io.crate.exceptions.TableScopeException;
import io.crate.metadata.RelationName;
import io.crate.rest.action.HttpErrorStatus;

public class UnavailableShardsException extends ElasticsearchException implements TableScopeException {

    private static final Pattern MSG_INDEX_NAME_PATTERN = Pattern.compile("\\[([^\\]]+)\\].*");

    @Nullable
    private final RelationName relationName;

    public UnavailableShardsException(ShardId shardId, @Nullable RelationName relationName, String message, Object... args) {
        super(buildMessage(relationName, shardId, message), args);
        this.relationName = relationName;
    }

    public UnavailableShardsException(RelationName relation, ShardId shardId) {
        super(buildMessage(relation, shardId));
        this.relationName = relation;
    }

    private static String buildMessage(RelationName relation, ShardId shardId) {
        return String.format(Locale.ENGLISH, "[%s] shard %s is not available", relation, shardId.id());
    }

    private static String buildMessage(@Nullable RelationName relationName, ShardId shardId, String message) {
        if (relationName == null) {
            return "[" + shardId.getIndexUUID() + "][" + shardId.id() + "] " + message;
        }
        return "[" + relationName + "][" + shardId.id() + "] " + message;
    }

    public UnavailableShardsException(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_6_3_6)) {
            this.relationName = in.readOptionalWriteable(RelationName::new);
        } else {
            RelationName name = null;
            try {
                Matcher matcher = MSG_INDEX_NAME_PATTERN.matcher(getMessage());
                name = matcher.matches() ? RelationName.fromIndexName(matcher.group(1)) : null;
            } catch (Exception ex) {
                name = null;
            }
            this.relationName = name;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeOptionalWriteable(relationName);
        }
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.ONE_OR_MORE_SHARDS_NOT_AVAILABLE;
    }

    @Override
    public Iterable<RelationName> getTableIdents() {
        return relationName == null ? List.of() : List.of(relationName);
    }
}
