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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Represents a failure to search on a specific shard.
 */
public class ShardSearchFailure extends ShardOperationFailedException {

    private static final String REASON_FIELD = "reason";
    private static final String NODE_FIELD = "node";
    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";

    public static final ShardSearchFailure[] EMPTY_ARRAY = new ShardSearchFailure[0];

    private SearchShardTarget shardTarget;

    ShardSearchFailure() {
    }

    public ShardSearchFailure(Exception e) {
        this(e, null);
    }

    public ShardSearchFailure(Exception e, @Nullable SearchShardTarget shardTarget) {
        super(shardTarget == null ? null : shardTarget.getFullyQualifiedIndexName(),
            shardTarget == null ? -1 : shardTarget.getShardId().getId(),
            ExceptionsHelper.detailedMessage(e),
            ExceptionsHelper.status(ExceptionsHelper.unwrapCause(e)),
            ExceptionsHelper.unwrapCause(e));

        final Throwable actual = ExceptionsHelper.unwrapCause(e);
        if (actual instanceof SearchException) {
            this.shardTarget = ((SearchException) actual).shard();
        } else if (shardTarget != null) {
            this.shardTarget = shardTarget;
        }
    }

    /**
     * The search shard target the failure occurred on.
     */
    @Nullable
    public SearchShardTarget shard() {
        return this.shardTarget;
    }

    @Override
    public String toString() {
        return "shard [" + (shardTarget == null ? "_na" : shardTarget) + "], reason [" + reason + "], cause [" +
                (cause == null ? "_na" : ExceptionsHelper.stackTrace(cause)) + "]";
    }

    public static ShardSearchFailure readShardSearchFailure(StreamInput in) throws IOException {
        ShardSearchFailure shardSearchFailure = new ShardSearchFailure();
        shardSearchFailure.readFrom(in);
        return shardSearchFailure;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            shardTarget = new SearchShardTarget(in);
            index = shardTarget.getFullyQualifiedIndexName();
            shardId = shardTarget.getShardId().getId();
        }
        reason = in.readString();
        status = RestStatus.readFrom(in);
        cause = in.readException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (shardTarget == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            shardTarget.writeTo(out);
        }
        out.writeString(reason);
        RestStatus.writeTo(out, status);
        out.writeException(cause);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(SHARD_FIELD, shardId());
        builder.field(INDEX_FIELD, index());
        if (shardTarget != null) {
            builder.field(NODE_FIELD, shardTarget.getNodeId());
        }
        builder.field(REASON_FIELD);
        builder.startObject();
        ElasticsearchException.generateThrowableXContent(builder, params, cause);
        builder.endObject();
        return builder;
    }

    public static ShardSearchFailure fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        String currentFieldName = null;
        int shardId = -1;
        String indexName = null;
        String clusterAlias = null;
        String nodeId = null;
        ElasticsearchException exception = null;
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SHARD_FIELD.equals(currentFieldName)) {
                    shardId  = parser.intValue();
                } else if (INDEX_FIELD.equals(currentFieldName)) {
                    indexName  = parser.text();
                    int indexOf = indexName.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                    if (indexOf > 0) {
                        clusterAlias = indexName.substring(0, indexOf);
                        indexName = indexName.substring(indexOf + 1);
                    }
                } else if (NODE_FIELD.equals(currentFieldName)) {
                    nodeId  = parser.text();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (REASON_FIELD.equals(currentFieldName)) {
                    exception = ElasticsearchException.fromXContent(parser);
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        SearchShardTarget searchShardTarget = null;
        if (nodeId != null) {
            searchShardTarget = new SearchShardTarget(nodeId,
                    new ShardId(new Index(indexName, IndexMetaData.INDEX_UUID_NA_VALUE), shardId), clusterAlias, OriginalIndices.NONE);
        }
        return new ShardSearchFailure(exception, searchShardTarget);
    }
}
