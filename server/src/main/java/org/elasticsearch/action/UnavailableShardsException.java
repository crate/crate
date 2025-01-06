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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.jetbrains.annotations.Nullable;
import io.crate.exceptions.TableScopeException;
import io.crate.metadata.RelationName;
import io.crate.rest.action.HttpErrorStatus;

public class UnavailableShardsException extends ElasticsearchException implements TableScopeException {

    private static final Pattern MSG_INDEX_NAME_PATTERN = Pattern.compile("\\[([^\\]]+)\\].*");

    @Nullable
    private final RelationName relationName;

    public UnavailableShardsException(@Nullable ShardId shardId, String message, Object... args) {
        super(buildMessage(shardId, message), args);
        this.relationName = shardId == null ? null : RelationName.fromIndexName(shardId.getIndexName());
    }

    public UnavailableShardsException(ShardId shardId) {
        super(buildMessage(shardId));
        this.relationName = shardId == null ? null : RelationName.fromIndexName(shardId.getIndexName());
    }

    private static String buildMessage(ShardId shardId) {
        return String.format(Locale.ENGLISH, "[%s] shard %s is not available", shardId.getIndexName(), shardId.id());
    }

    private static String buildMessage(@Nullable ShardId shardId, String message) {
        if (shardId == null) {
            return message;
        }
        return "[" + shardId.getIndexName() + "][" + shardId.id() + "] " + message;
    }

    public UnavailableShardsException(StreamInput in) throws IOException {
        super(in);
        RelationName name = null;
        try {
            Matcher matcher = MSG_INDEX_NAME_PATTERN.matcher(getMessage());
            name = matcher.matches() ? RelationName.fromIndexName(matcher.group(1)) : null;
        } catch (Exception ex) {
            name = null;
        }
        this.relationName = name;
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
