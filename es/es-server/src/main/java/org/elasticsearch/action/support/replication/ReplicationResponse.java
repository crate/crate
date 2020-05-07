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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import javax.annotation.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Base class for write action responses.
 */
public class ReplicationResponse extends TransportResponse {

    public static final ReplicationResponse.ShardInfo.Failure[] EMPTY = new ReplicationResponse.ShardInfo.Failure[0];

    private ShardInfo shardInfo;

    public ReplicationResponse() {
    }

    public ReplicationResponse(StreamInput in) throws IOException {
        shardInfo = new ReplicationResponse.ShardInfo(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardInfo.writeTo(out);
    }

    public ShardInfo getShardInfo() {
        return shardInfo;
    }

    public void setShardInfo(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    public static class ShardInfo implements Writeable, ToXContentObject {

        private static final String TOTAL = "total";
        private static final String SUCCESSFUL = "successful";
        private static final String FAILED = "failed";
        private static final String FAILURES = "failures";

        private final int total;
        private final int successful;
        private final Failure[] failures;

        public ShardInfo(int total, int successful, Failure... failures) {
            assert total >= 0 && successful >= 0;
            this.total = total;
            this.successful = successful;
            this.failures = failures;
        }

        public ShardInfo(StreamInput in) throws IOException {
            total = in.readVInt();
            successful = in.readVInt();
            int size = in.readVInt();
            failures = new Failure[size];
            for (int i = 0; i < size; i++) {
                failures[i] = new Failure(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(failures.length);
            for (Failure failure : failures) {
                failure.writeTo(out);
            }
        }

        /**
         * @return the total number of shards the write should go to (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getTotal() {
            return total;
        }

        /**
         * @return the total number of shards the write succeeded on (replicas and primaries). This includes relocating shards, so this
         *         number can be higher than the number of shards.
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * @return The total number of replication failures.
         */
        public int getFailed() {
            return failures.length;
        }

        /**
         * @return The replication failures that have been captured in the case writes have failed on replica shards.
         */
        public Failure[] getFailures() {
            return failures;
        }

        public RestStatus status() {
            RestStatus status = RestStatus.OK;
            for (Failure failure : failures) {
                if (failure.primary() && failure.status().getStatus() > status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TOTAL, total);
            builder.field(SUCCESSFUL, successful);
            builder.field(FAILED, getFailed());
            if (failures.length > 0) {
                builder.startArray(FAILURES);
                for (Failure failure : failures) {
                    failure.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "ShardInfo{" +
                "total=" + total +
                ", successful=" + successful +
                ", failures=" + Arrays.toString(failures) +
                '}';
        }

        public static class Failure extends ShardOperationFailedException implements ToXContentObject {

            private static final String INDEX = "_index";
            private static final String SHARD = "_shard";
            private static final String NODE = "_node";
            private static final String REASON = "reason";
            private static final String STATUS = "status";
            private static final String PRIMARY = "primary";

            private final ShardId shardId;
            private final String nodeId;
            private final boolean primary;

            public Failure(ShardId shardId,
                           @Nullable String nodeId,
                           Exception cause,
                           RestStatus status,
                           boolean primary) {
                super(shardId.getIndexName(), shardId.getId(), ExceptionsHelper.detailedMessage(cause), status, cause);
                this.shardId = shardId;
                this.nodeId = nodeId;
                this.primary = primary;
            }

            public ShardId fullShardId() {
                return shardId;
            }

            /**
             * @return On what node the failure occurred.
             */
            @Nullable
            public String nodeId() {
                return nodeId;
            }

            /**
             * @return Whether this failure occurred on a primary shard.
             * (this only reports true for delete by query)
             */
            public boolean primary() {
                return primary;
            }

            public Failure(StreamInput in) throws IOException {
                shardId = new ShardId(in);
                super.shardId = shardId.getId();
                index = shardId.getIndexName();
                nodeId = in.readOptionalString();
                cause = in.readException();
                status = RestStatus.readFrom(in);
                primary = in.readBoolean();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                shardId.writeTo(out);
                out.writeOptionalString(nodeId);
                out.writeException(cause);
                RestStatus.writeTo(out, status);
                out.writeBoolean(primary);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(INDEX, shardId.getIndexName());
                builder.field(SHARD, shardId.id());
                builder.field(NODE, nodeId);
                builder.field(REASON);
                builder.startObject();
                ElasticsearchException.generateThrowableXContent(builder, params, cause);
                builder.endObject();
                builder.field(STATUS, status);
                builder.field(PRIMARY, primary);
                builder.endObject();
                return builder;
            }

            public static Failure fromXContent(XContentParser parser) throws IOException {
                XContentParser.Token token = parser.currentToken();
                ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

                String shardIndex = null, nodeId = null;
                int shardId = -1;
                boolean primary = false;
                RestStatus status = null;
                ElasticsearchException reason = null;

                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (INDEX.equals(currentFieldName)) {
                            shardIndex = parser.text();
                        } else if (SHARD.equals(currentFieldName)) {
                            shardId = parser.intValue();
                        } else if (NODE.equals(currentFieldName)) {
                            nodeId = parser.text();
                        } else if (STATUS.equals(currentFieldName)) {
                            status = RestStatus.valueOf(parser.text());
                        } else if (PRIMARY.equals(currentFieldName)) {
                            primary = parser.booleanValue();
                        }
                    } else if (token == XContentParser.Token.START_OBJECT) {
                        if (REASON.equals(currentFieldName)) {
                            reason = ElasticsearchException.fromXContent(parser);
                        } else {
                            parser.skipChildren(); // skip potential inner objects for forward compatibility
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        parser.skipChildren(); // skip potential inner arrays for forward compatibility
                    }
                }
                return new Failure(new ShardId(shardIndex, IndexMetaData.INDEX_UUID_NA_VALUE, shardId), nodeId, reason, status, primary);
            }
        }
    }
}
