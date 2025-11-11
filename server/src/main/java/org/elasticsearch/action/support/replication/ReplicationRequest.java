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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.TransportRequest;

import io.crate.common.unit.TimeValue;

/**
 * Requests that are run on a particular replica, first on the primary and then on the replicas like
 * {@link TransportShardRefreshAction}.
 */
public abstract class ReplicationRequest<Request extends ReplicationRequest<Request>> extends TransportRequest {

    public static final TimeValue DEFAULT_TIMEOUT = new TimeValue(1, TimeUnit.MINUTES);

    /**
     * Target shard the request should execute on. In case of index and delete requests,
     * shard id gets resolved by the transport action before performing request operation
     * and at request creation time for shard-level bulk, refresh and flush requests.
     */
    protected final ShardId shardId;

    protected TimeValue timeout = DEFAULT_TIMEOUT;

    /**
     * The number of shard copies that must be active before proceeding with the replication action.
     */
    protected ActiveShardCount waitForActiveShards = ActiveShardCount.DEFAULT;

    private long routedBasedOnClusterVersion = 0;

    private final Version senderVersion;

    /**
     * Creates a new request with resolved shard id
     */
    public ReplicationRequest(ShardId shardId) {
        this.shardId = shardId;
        this.senderVersion = Version.CURRENT;
    }

    /**
     * A timeout to wait if the index operation can't be performed immediately. Defaults to {@code 1m}.
     */
    @SuppressWarnings("unchecked")
    public final Request timeout(TimeValue timeout) {
        this.timeout = timeout;
        return (Request) this;
    }

    public TimeValue timeout() {
        return timeout;
    }

    public ActiveShardCount waitForActiveShards() {
        return this.waitForActiveShards;
    }

    /**
     * @return the shardId of the shard where this operation should be executed on.
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * Sets the number of shard copies that must be active before proceeding with the replication
     * operation. Defaults to {@link ActiveShardCount#DEFAULT}, which requires one shard copy
     * (the primary) to be active. Set this value to {@link ActiveShardCount#ALL} to
     * wait for all shards (primary and all replicas) to be active. Otherwise, use
     * {@link ActiveShardCount#from(int)} to set this value to any non-negative integer, up to the
     * total number of shard copies (number of replicas + 1).
     */
    @SuppressWarnings("unchecked")
    public final Request waitForActiveShards(ActiveShardCount waitForActiveShards) {
        this.waitForActiveShards = waitForActiveShards;
        return (Request) this;
    }

    /**
     * Sets the minimum version of the cluster state that is required on the next node before we redirect to another primary.
     * Used to prevent redirect loops, see also {@link TransportReplicationAction.ReroutePhase#doRun()}
     */
    @SuppressWarnings("unchecked")
    protected Request routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        this.routedBasedOnClusterVersion = routedBasedOnClusterVersion;
        return (Request) this;
    }

    long routedBasedOnClusterVersion() {
        return routedBasedOnClusterVersion;
    }

    public ReplicationRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_6_0_0)) {
            in.readBoolean();
        }
        shardId = new ShardId(in);
        waitForActiveShards = ActiveShardCount.readFrom(in);
        timeout = in.readTimeValue();
        if (in.getVersion().before(Version.V_6_0_0)) {
            // old index name, not used anymore
            in.readString();
        }
        routedBasedOnClusterVersion = in.readVLong();
        senderVersion = in.getVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_6_0_0)) {
            out.writeBoolean(true);
        }
        shardId.writeTo(out);
        waitForActiveShards.writeTo(out);
        out.writeTimeValue(timeout);
        if (out.getVersion().before(Version.V_6_0_0)) {
            out.writeString(shardId.getIndexName());
        }
        out.writeVLong(routedBasedOnClusterVersion);
        System.out.println("Replication request: shardID: " + shardId);
    }

    @Override
    public abstract String toString(); // force a proper to string to ease debugging

    /**
     * This method is called before this replication request is retried
     * the first time.
     */
    public void onRetry() {
        // nothing by default
    }

    public Version senderVersion() {
        return senderVersion;
    }
}
