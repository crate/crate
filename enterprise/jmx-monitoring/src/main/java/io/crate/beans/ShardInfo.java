/*
 * This file is part of a module with proprietary Enterprise Features.
 *
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 *
 * To use this file, Crate.io must have given you permission to enable and
 * use such Enterprise Features and you must have a valid Enterprise or
 * Subscription Agreement with Crate.io.  If you enable or use the Enterprise
 * Features, you represent and warrant that you have a valid Enterprise or
 * Subscription Agreement with Crate.io.  Your use of the Enterprise Features
 * if governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
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
