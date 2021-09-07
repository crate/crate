/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.replication.logical.action;

import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAwareRequest;

import java.io.IOException;

public abstract class RestoreShardRequest<T extends SingleShardRequest<T>> extends SingleShardRequest<T>
    implements RemoteClusterAwareRequest {

    private final String restoreUUID;
    private final DiscoveryNode node;
    private final ShardId publisherShardId;
    private final String subscriberClusterName;
    private final ShardId subscriberShardId;

    public RestoreShardRequest(String restoreUUID,
                               DiscoveryNode node,
                               ShardId publisherShardId,
                               String subscriberClusterName,
                               ShardId subscriberShardId) {
        super(publisherShardId.getIndexName());
        this.restoreUUID = restoreUUID;
        this.node = node;
        this.publisherShardId = publisherShardId;
        this.subscriberClusterName = subscriberClusterName;
        this.subscriberShardId = subscriberShardId;
    }

    public RestoreShardRequest(StreamInput in) throws IOException {
        super(in);
        this.restoreUUID = in.readString();
        this.node = new DiscoveryNode(in);
        this.publisherShardId = new ShardId(in);
        this.subscriberClusterName = in.readString();
        this.subscriberShardId = new ShardId(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(restoreUUID);
        node.writeTo(out);
        publisherShardId.writeTo(out);
        out.writeString(subscriberClusterName);
        subscriberShardId.writeTo(out);
    }

    @Override
    public DiscoveryNode getPreferredTargetNode() {
        return node;
    }

    public ShardId publisherShardId() {
        return publisherShardId;
    }

    public String subscriberClusterName() {
        return subscriberClusterName;
    }

    public ShardId subscriberShardId() {
        return subscriberShardId;
    }

    public String restoreUUID() {
        return restoreUUID;
    }

    @Override
    public String toString() {
        return "RestoreShardRequest{" +
               "restoreUUID='" + restoreUUID + '\'' +
               ", node=" + node +
               ", publisherShardId=" + publisherShardId +
               ", subscriberClusterName='" + subscriberClusterName + '\'' +
               ", subscriberShardId=" + subscriberShardId +
               ", index='" + index + '\'' +
               '}';
    }
}
