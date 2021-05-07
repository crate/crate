/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.beans;

import java.beans.ConstructorProperties;

public class ShardInfo {
    final int shardId;
    final String routingState;
    final String state;
    final String table;
    final String partitionIdent;
    final long size;

    @ConstructorProperties({"shardId", "table", "partitionIdent", "routingState", "state", "size"})
    public ShardInfo(int shardId, String table, String partitionIdent, String routingState, String state, long size) {
        this.shardId = shardId;
        this.routingState = routingState;
        this.state = state;
        this.table = table;
        this.partitionIdent = partitionIdent;
        this.size = size;
    }

    public int getShardId() {
        return shardId;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionIdent() {
        return partitionIdent;
    }

    public String getRoutingState() {
        return routingState;
    }

    public String getState() {
        return state;
    }

    public long getSize() {
        return size;
    }
}
