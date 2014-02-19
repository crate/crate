/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package org.cratedb.action;

import org.cratedb.action.sql.SQLRequest;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;


/**
 * Sent from a Handler to a Mapper Node to query a shard and then send the results to Reducer Nodes
 *
 * See {@link TransportDistributedSQLAction} for a full overview of the workflow.
 */
public class SQLShardRequest extends TransportRequest implements Streamable {

    public String[] reducers;
    public SQLRequest sqlRequest;
    public UUID contextId;
    public int shardId;
    public String concreteIndex;


    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        concreteIndex = in.readString();
        shardId = in.readVInt();
        contextId = new UUID(in.readLong(), in.readLong());
        reducers = in.readStringArray();
        sqlRequest = new SQLRequest();
        sqlRequest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(concreteIndex);
        out.writeVInt(shardId);
        out.writeLong(contextId.getMostSignificantBits());
        out.writeLong(contextId.getLeastSignificantBits());
        out.writeStringArray(reducers);
        sqlRequest.writeTo(out);
    }
}
