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

package org.elasticsearch.action.support;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.common.exceptions.Exceptions;

public class DefaultShardOperationFailedException extends ShardOperationFailedException {

    public DefaultShardOperationFailedException(ElasticsearchException e) {
        super(
            e.getIndex() == null ? null : e.getIndex().getName(),
            e.getShardId() == null ? -1 : e.getShardId().id(),
            Exceptions.stackTrace(e),
            e
        );
    }

    public DefaultShardOperationFailedException(String index, int shardId, Throwable cause) {
        super(index, shardId, Exceptions.stackTrace(cause), cause);
    }

    public DefaultShardOperationFailedException(StreamInput in) throws IOException {
        index = in.readOptionalString();
        shardId = in.readVInt();
        cause = in.readException();
        if (in.getVersion().before(Version.V_5_10_0)) {
            in.readString(); // ignore the old RestStatus
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(index);
        out.writeVInt(shardId);
        out.writeException(cause);
        if (out.getVersion().before(Version.V_5_10_0)) {
            out.writeString("INTERNAL_SERVER_ERROR");
        }
    }

    @Override
    public String toString() {
        return "[" + index + "][" + shardId + "] failed, reason [" + reason() + "]";
    }
}
